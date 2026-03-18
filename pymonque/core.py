from __future__ import annotations
from pydantic import BaseModel, Field, create_model

from typing import Any, Literal, Self, Callable, get_type_hints
from types import FunctionType, MethodType

from pymongo.database import Database
from pymongo.collection import Collection

from datetime import datetime, timedelta
from uuid import UUID, uuid4

import random
import math

import inspect

from .exceptions import TaskValidationError, TaskNotFound, DistributionValidationError, DistributionNotFound


TASK_STATUS = Literal["pending", "success", "failed", "canceled", "outdated", "incompatible"]
SCHEDULER_STATUS = Literal["enabled", "disabled"]

OVERDUE_TASKS_POLICY = Literal["skip", "execute now"]
OVERDUE_SCHEDULES_POLICY = Literal["skip", "execute once", "execute reconstructed"]


def getStaticmethods(cls: type) -> dict[str, Callable]:
    return {
        name: obj.__func__
        for base in reversed(cls.__mro__)  # walk trough all parents
        for name, obj in base.__dict__.items()
        if isinstance(obj, staticmethod) and not name.startswith("_")  # add only staticmethods
    }

def getInstanceMethods(cls: type) -> dict[str, Callable]:
    return {
        name: obj
        for base in reversed(cls.__mro__)
        for name, obj in base.__dict__.items()
        if inspect.isfunction(obj) and not name.startswith("_")  # add only 
    }

def buildValidator(func: Callable) -> type[BaseModel]:
    sig = inspect.signature(func)
    hints = get_type_hints(func)

    fields = {}

    for name, param in sig.parameters.items():
        annotation = hints.get(name, object)

        if param.default is inspect._empty:
            default = ...
        else:
            default = param.default

        fields[name] = (annotation, default)

    return create_model(f"{func.__name__}_Args", **fields)


class Distributions:
    @classmethod
    def _getDistributions(cls) -> dict[str, Callable]:
        return getStaticmethods(cls)
    
    @staticmethod
    def constant(dailyFrequency: float) -> timedelta:
        interval_sec = 86400 / dailyFrequency
        return timedelta(seconds=interval_sec)
    
    @staticmethod
    def normal(dailyFrequency: float, stdFraction: float) -> timedelta:
        mean_sec = 86400 / dailyFrequency
        std_sec = mean_sec * stdFraction
        interval_sec = random.gauss(mean_sec, std_sec)
        interval_sec = max(0, interval_sec)  # avoid negative intervals
        return timedelta(seconds=interval_sec)

    @staticmethod
    def lognormal(dailyFrequency: float, sigma: float) -> timedelta:
        mean_sec = 86400 / dailyFrequency
        mu = math.log(mean_sec) - (sigma**2)/2
        interval_sec = random.lognormvariate(mu, sigma)
        return timedelta(seconds=interval_sec)

    @staticmethod
    def exponential(dailyFrequency: float) -> timedelta:
        mean_sec = 86400 / dailyFrequency
        interval_sec = random.expovariate(1 / mean_sec)
        return timedelta(seconds=interval_sec)


class Distribution(BaseModel):
    name: str
    kwargs: dict[str, Any]


class Task(BaseModel):
    functionName: str
    kwargs: dict[str, Any]
    status: TASK_STATUS = "pending"
    deadline: datetime
    factory: TaskFactory

    executionTime: timedelta | None = None
    error: str | None = None


class TaskFactory(BaseModel):
    uid: UUID = Field(default_factory=uuid4)
    name: str

    def _emit(self, functionName: str, kwargs: dict[str, Any], deadline: datetime) -> Task:
        return Task(functionName=functionName, kwargs=kwargs, deadline=deadline, factory=self)


class Scheduler(TaskFactory):
    name: str = "Scheduler"
    functionName: str
    kwargs: dict[str, Any]
    distribution: Distribution
    deadline: datetime
    status: SCHEDULER_STATUS = "enabled"

    def _emit(self, deadline: datetime) -> Task:
        return super()._emit(self.functionName, self.kwargs, deadline)


def task(obj):
    if isinstance(obj, staticmethod):
        func = obj.__func__
        setattr(func, "__is_task__", True)
        return staticmethod(func)
    
    if isinstance(obj, (FunctionType, MethodType)):
        setattr(obj, "__is_task__", True)
        return obj
    
    raise TypeError(f"@task cannot be applied to {type(obj)}")


class BaseQueue:
    @classmethod
    def _getStaticmethodTasks(cls) -> dict[str, Callable]:
        candidates = {
            name: obj
            for base in reversed(cls.__mro__)
            for name, obj in base.__dict__.items()
        }  # resolve child overrides

        return {
            name: obj.__func__
            for name, obj in candidates.items()
            if isinstance(obj, staticmethod)
            and getattr(obj.__func__, "__is_task__", False)
        }  # only return static methods marked with the task decorator
    
    @classmethod
    def _getInstanceTasks(cls) -> dict[str, Callable]:
        candidates = {
            name: obj
            for base in reversed(cls.__mro__)
            for name, obj in base.__dict__.items()
        }  # resolve child overrides

        return {
            name: object
            for name, obj in candidates.items()
            if inspect.isfunction(obj)
            and getattr(obj, "__is_task__", False)
        }

    def __init__(self, queueDB: Database, DistributionsClass: type[Distributions] = Distributions):
        self.defaultFactory: TaskFactory = TaskFactory(name="default")
        
        # resolve tasks and distributions
        self.tasks: dict[str, Callable] = {
            **type(self)._getStaticmethodTasks(),
            **{
                name: getattr(self, name)
                for name in type(self)._getInstanceTasks()
            }
        }

        self.distributions: dict[str, Callable] = DistributionsClass._getDistributions()


        # build kwarg validators
        self.taskValidators: dict[str, type[BaseModel]] = {
            name: buildValidator(func)
            for name, func in self.tasks.items()
        }

        self.distributionValidators: dict[str, type[BaseModel]] = {
            name: buildValidator(func)
            for name, func in self.distributions.items()
        }


        # prepare DB
        self.tasksCollection: Collection = queueDB["pymonque_tasks"]
        self.schedulersCollection: Collection = queueDB["pymonque_schedulers"]

        self.tasksCollection.create_index([("functionName", 1), ("status", 1), ("deadline", 1)])  # Make task query blazingly fast
        self.tasksCollection.create_index([("functionName", 1), ("status", 1), ("factory.uid", 1)])  # Optimize task deletion

        self.schedulersCollection.create_index([("uid", 1), ("status", 1)])
        self.schedulersCollection.create_index([("functionName", 1), ("status", 1)])

        self.tasksCollection.update_many(
            {"status": "pending", "functionName": {"$nin": self.tasks.keys()}},
            {"$set": {"status": "incompatible", "error": "Function for this task does not exist in the TaskClass"}}
        )  # flag pending tasks that cannot be executed

        self.schedulersCollection.update_many(
            {"status": "enabled", "functionName": {"$nin": self.tasks.keys()}}, 
            {"$set": {"status": "disabled"}}
        )  # disable Schedulers that emit tasks that cannot be executed

    def _validateTask(self, functionName: str, kwargs: dict[str, Any]):
        try:
            self.taskValidators[functionName](**kwargs)
        except ValueError as e:
            raise TaskValidationError(f"Failed to validate task {functionName}") from e
        except KeyError:
            raise TaskNotFound(f"Task {functionName} does not exist in this Queue")
        
    def _validateDistribution(self, distribution: Distribution):
        try:
            self.distributionValidators[distribution.name](**distribution.kwargs)
        except ValueError as e:
            raise DistributionValidationError(f"Failed to validate distribution {distribution.name}") from e
        except KeyError:
            raise DistributionNotFound(f"Distribution {distribution.name} does not exist in this Queue")

    def scheduleTask(self, functionName: str, kwargs: dict[str, Any], deadline: datetime, factory: TaskFactory | None = None):
        factory = factory or self.defaultFactory

        task = factory._emit(
            functionName=functionName, 
            kwargs=kwargs, 
            deadline=deadline
        )

        serialized = task.model_dump(mode="python", by_alias=True, exclude_none=True)
        self.tasksCollection.insert_one(serialized)

    def scheduleValidatedTask(self, functionName: str, kwargs: dict[str, Any], deadline: datetime, factory: TaskFactory | None = None):
        self._validateTask(functionName, kwargs)

        self.scheduleTask(
            functionName=functionName, 
            kwargs=kwargs, 
            deadline=deadline, 
            factory=factory
        )

    def addSchedluer(self, functionName: str, kwargs: dict[str, Any], distribution: Distribution):
        deadline = datetime.now() + self.distributions[distribution.name](**distribution.kwargs)

        scheduler = Scheduler(
            functionName=functionName, 
            kwargs=kwargs, 
            distribution=distribution, 
            deadline=deadline
        )

        serialized = scheduler.model_dump(mode="python", by_alias=True, exclude_none=True)
        self.schedulersCollection.insert_one(serialized)

    def addValidatedSchedluer(self, functionName: str, kwargs: dict[str, Any], distribution: Distribution):
        self._validateDistribution(distribution)
        self._validateTask(functionName, kwargs)

        self.addSchedluer(
            functionName=functionName,
            kwargs=kwargs,
            distribution=distribution
        )
