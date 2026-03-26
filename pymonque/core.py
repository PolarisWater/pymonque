from __future__ import annotations
from pydantic import BaseModel, Field, create_model, field_validator, field_serializer, ConfigDict

from typing import Any, Literal, Callable, get_type_hints
from types import FunctionType, MethodType

from pymongo.database import Database
from pymongo.collection import Collection

from datetime import datetime, timedelta
from uuid import UUID, uuid4

import random
import math
import time
import traceback

import inspect

from pymonque.exceptions import TaskValidationError, TaskNotFound, DistributionValidationError, DistributionNotFound
from pymonque.mongo import MongoModel


TASK_STATUS = Literal["pending", "success", "processing", "failed", "canceled", "outdated", "incompatible"]
SCHEDULER_STATUS = Literal["enabled", "disabled", "processing"]

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


    return create_model(
        f"{func.__name__}_Args",
        **fields,
        __config__ = ConfigDict(extra="forbid")
    )

def uuid4str() -> str:
    return str(uuid4())


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


class Distribution(MongoModel):
    name:           str
    kwargs:         dict[str, Any]


class Task(MongoModel):
    uid:            str                = Field(default_factory=uuid4str)
    functionName:   str
    kwargs:         dict[str, Any]
    status:         TASK_STATUS         = "pending"
    deadline:       datetime
    factory:        TaskFactory

    executionTime:  timedelta | None    = None
    result:         Any | None          = None
    error:          str | None          = None

    @field_serializer("executionTime")
    def serialize_executionTime(value: timedelta) -> float:
        return float(value.seconds)
    
    @field_validator("executionTime", mode="before")
    def validate_executionTime(value) -> timedelta:
        if isinstance(value, (int, float)):
            return timedelta(seconds=value)
        
        return value


class TaskFactory(MongoModel):
    uid:    str    = Field(default_factory=uuid4str)
    name:   str

    def _emit(self, functionName: str, kwargs: dict[str, Any], deadline: datetime) -> Task:
        return Task(functionName=functionName, kwargs=kwargs, deadline=deadline, factory=self)


class Scheduler(TaskFactory):
    name:           str                 = "Scheduler"
    functionName:   str
    kwargs:         list[dict[str, Any]]  # all tasks are emited at once
    distribution:   Distribution
    deadline:       datetime
    status:         SCHEDULER_STATUS    = "enabled"

    def _emit(self, deadline: datetime) -> list[Task]:
        return [
            super()._emit(self.functionName, kwarg, deadline)
            for kwarg in self.kwargs
        ]


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

    def _executeTask(self, task: Task) -> Task:
        func = self.tasks[task.functionName]

        start = time.perf_counter()
        try:
            result = func(**task.kwargs)
            task.status = "success"
            task.result = result
        except Exception:
            task.status = "failed"
            task.error = traceback.format_exc()
        finally:
            end = time.perf_counter()
            task.executionTime = timedelta(seconds=(end - start))

        return task

    def scheduleTask(
            self, 
            functionName:   str, 
            kwargs:         dict[str, Any] | list[dict[str, Any]], 
            deadline:       datetime, 
            factory:        TaskFactory | None = None
        ):
        
        factory = factory or self.defaultFactory

        if isinstance(kwargs, dict):
            kwargs = [kwargs]

        serialized = [
            factory._emit(
                functionName=functionName, 
                kwargs=kwarg, 
                deadline=deadline
            ).model_dump() 
            for kwarg in kwargs
        ]

        self.tasksCollection.insert_many(serialized)

    def scheduleValidatedTask(
            self, 
            functionName:   str, 
            kwargs:         dict[str, Any] | list[dict[str, Any]], 
            deadline:       datetime, 
            factory:        TaskFactory | None = None
        ):

        if isinstance(kwargs, dict):
            kwargs = [kwargs]

        for kwarg in kwargs:
            self._validateTask(functionName, kwarg)

        self.scheduleTask(
            functionName=functionName, 
            kwargs=kwargs, 
            deadline=deadline, 
            factory=factory
        )

    def addSchedluer(
            self, 
            functionName: str, 
            kwargs: dict[str, Any] | list[dict[str, Any]], 
            distribution: Distribution
        ):
        
        distFunc = self.distributions[distribution.name]
        deadline = datetime.now() + distFunc(**distribution.kwargs)

        if isinstance(kwargs, dict):
            kwargs = [kwargs]

        scheduler = Scheduler(
            functionName=functionName, 
            kwargs=kwargs, 
            distribution=distribution, 
            deadline=deadline
        )

        self.schedulersCollection.insert_one(
            scheduler.model_dump()
        )

    def addValidatedSchedluer(
            self, 
            functionName: str, 
            kwargs: dict[str, Any] | list[dict[str, Any]], 
            distribution: Distribution
        ):

        self._validateDistribution(distribution)

        if isinstance(kwargs, dict):
            kwargs = [kwargs]

        for kwarg in kwargs:
            self._validateTask(functionName, kwarg)

        self.addSchedluer(
            functionName=functionName,
            kwargs=kwargs,
            distribution=distribution
        )

    def _work(self):
        now = datetime.now()
        raw = self.tasksCollection.find_one_and_update(
            {"status": "pending", "deadline": {"$lte": now}},
            {"$set": {"status": "processing"}}
        )

        if not raw:
            return

        task = Task.model_validate(raw)
        task = self._executeTask(task)

        self.tasksCollection.replace_one(
            {"uid": task.uid},
            task.model_dump()
        )

    def _schedule(self):
        now = datetime.now()
        raw = self.schedulersCollection.find_one_and_update(
            {"status": "enabled", "deadline": {"$lte": now}},
            {"$set": {"status": "processing"}}
        )

        if not raw:
            return
        
        scheduler = Scheduler.model_validate(raw)

        distFunc = self.distributions[scheduler.distribution.name]
        deadline = scheduler.deadline + distFunc(**scheduler.distribution.kwargs)

        serialized = [
            task.model_dump()
            for task in scheduler._emit(deadline=deadline)
        ]

        self.tasksCollection.insert_many(serialized)

        self.schedulersCollection.update_one(
            {"uid": scheduler.uid},
            {"$set": {"status": "enabled", "deadline": deadline}}
        )

    def work(self):
        while True:
            self._work()
            self._schedule()
            time.sleep(1)
            print(list(self.tasksCollection.find()))