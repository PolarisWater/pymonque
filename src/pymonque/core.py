from __future__ import annotations
from pydantic import (
    create_model, field_validator, field_serializer, 
    ConfigDict, BaseModel, Field
)

from typing import (
    Any, Literal, Callable, TypeVar, Mapping,
    get_type_hints, overload
)

from types import FunctionType, MethodType

from pymongo.database import Database
from pymongo.collection import Collection
from pymongo import UpdateOne

from datetime import datetime, timedelta, timezone

import random
import math
import time
import uuid
import traceback
import threading
import inspect

from pymonque.mongo import MongoModel

from pymonque.exceptions import (
    TaskValidationError, TaskNotFound, DistributionValidationError, DistributionNotFound
)


T = TypeVar("T")
TASK_STATUS = Literal["pending", "success", "processing", "failed", "canceled", "outdated", "incompatible"]
SCHEDULER_STATUS = Literal["enabled", "disabled", "processing"]

OVERDUE_TASKS_POLICY = Literal["skip", "execute now"]
OVERDUE_SCHEDULES_POLICY = Literal["skip", "execute once", "execute reconstructed"]

def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


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
    return str(uuid.uuid4())


class CallSpec(MongoModel):
    functionName:   str
    kwargs:         dict[str, Any]

    @classmethod
    def new(cls, functionName: str, **kwargs):
        return cls(
            functionName=functionName,
            kwargs=kwargs
        )

    @overload
    def __call__(self, source: Mapping[str, Callable[..., T]]) -> T: ...
    
    @overload
    def __call__(self, source: Mapping[str, type[BaseModel]]) -> BaseModel: ...
    
    def __call__(self, source: type | Mapping[str, Callable] | Mapping[str, type[BaseModel]]) -> Any:
        func = (
            source.get(self.functionName)
            if isinstance(source, Mapping)
            else getattr(source, self.functionName, None)
        )
        
        if func is None:
            raise KeyError(f"{source} has no function {self.functionName}")
        
        return func(**self.kwargs)
    
    def __repr__(self) -> str:
        return f"{self.functionName}({self.kwargs})"

class BaseDistributions:
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

class DistributionEngine:
    def __init__(self, registry: type[BaseDistributions] = BaseDistributions):
        self.registry: type[BaseDistributions] = registry

        self.functions: dict[str, Callable] = registry._getDistributions()

        self.validators: dict[str, type[BaseModel]] = {
            name: buildValidator(func)
            for name, func in self.functions.items()
        }

    def validate(self, distribution: CallSpec):
        try:
            distribution(self.validators)
        except ValueError as e:
            raise DistributionValidationError(f"Failed to validate distribution {distribution}") from e
        except KeyError:
            raise DistributionNotFound(f"Distribution {distribution.functionName} does not exist in this Queue")

    def gen(self, distribution: CallSpec) -> timedelta:
        delta = distribution(self.functions)
        if isinstance(delta, timedelta):
            return delta
        
        raise DistributionValidationError(f"distribution {distribution} did not return a timedelta")
    
    def __call__(self, functionName: str, **kwargs) -> CallSpec:
        obj = CallSpec.new(functionName, **kwargs)
        self.validate(obj)
        return obj


class Task(MongoModel):
    uid:            str                 = Field(default_factory=uuid4str)
    status:         TASK_STATUS         = "pending"
    work:           CallSpec
    deadline:       datetime
    factory:        TaskFactory

    executionTime:  timedelta | None    = None
    result:         Any | None          = None
    error:          str | None          = None

    @field_serializer("executionTime")
    @staticmethod
    def serialize_executionTime(value: timedelta | None) -> float | None:
        if value is not None:
            return float(value.total_seconds())

    @field_validator("executionTime", mode="before")
    @staticmethod
    def validate_executionTime(value: int | float | str) -> timedelta:
        if isinstance(value, (int, float, str)):
            return timedelta(seconds=float(value))
        
        raise TypeError(f"executionTime should not be a {type(value)}  ({value})")
    
    def __repr__(self) -> str:
        return f"Task {self.work} from {self.factory}"

class TaskFactory(MongoModel):
    uid:    str    = Field(default_factory=uuid4str)
    name:   str

    def _emit(
            self, 
            work:       CallSpec,
            deadline:   datetime
        ) -> Task:

        return Task(
            work=work,
            deadline=deadline, 
            factory=self
        )
    
    def __repr__(self) -> str:
        return f"Factory {self.name}"

class TaskEngine:
    def __init__(
            self, 
            queue:              BaseQueue,
            tasksCollection:    Collection | None = None,
            poolInterval:       float = 1,
            policy:             OVERDUE_TASKS_POLICY = "execute now",
            defaultFactory:     TaskFactory | None = None
        ):

        self._queue = queue
        self.poolInterval = poolInterval
        self.tasksCollection: Collection = tasksCollection  if tasksCollection is not None else queue.tasksCollection
        self.defaultFactory: TaskFactory = defaultFactory or queue.defaultFactory
        self.distributionEngine: DistributionEngine = queue.distribution
        self.policy: OVERDUE_TASKS_POLICY = policy

        # resolve tasks and distributions
        self.functions: dict[str, Callable] = {
            **type(self._queue)._getStaticmethodTasks(),
            **{
                name: getattr(self._queue, name)
                for name in type(self._queue)._getInstanceTasks()
            }
        }

        # build kwarg validators
        self.validators: dict[str, type[BaseModel]] = {
            name: buildValidator(func)
            for name, func in self.functions.items()
        }

        self.createIndexes()

    def init(self):
        now = utc_now()

        self.tasksCollection.update_many(
            {"status": "processing"},
            {"$set": {"status": "canceled"}},
        )

        if self.policy == "execute now":
            return
        
        if self.policy == "skip":
            self.tasksCollection.update_many(
                {"status": "pending", "deadline": {"$lte": now}},
                {"$set": {"status": "outdated"}},
            )

    def createIndexes(self):
        self.tasksCollection.create_index([("status", 1), ("deadline", 1)])  # _work() claim + init() skip/outdate
        self.tasksCollection.create_index([("uid", 1)])  # _work() update result

    def _work(self):
        now = utc_now()
        raw = self.tasksCollection.find_one_and_update(
            {"status": "pending", "deadline": {"$lte": now}},
            {"$set": {"status": "processing"}},
            sort=[("deadline", 1)]
        )

        if not raw:
            return

        task = Task.model_validate(raw)
        task = self.execute(task)

        return self.tasksCollection.update_one(
            {"uid": task.uid},
            {"$set": task.model_dump()}
        )

    def work(self):
        while True:
            self._work()
            time.sleep(self.poolInterval)

    def startWorkers(self, workerCount: int):
        threads = [
            threading.Thread(
                target=self.work,
                daemon=True
            ) for i in range(workerCount)
        ]

        for t in threads:
            t.start()

    def execute(self, task: Task) -> Task:
        start = time.perf_counter()
        try:
            task.result = task.work(self.functions)
            task.status = "success"
        except Exception:
            task.status = "failed"
            task.error = traceback.format_exc()
        finally:
            end = time.perf_counter()
            task.executionTime = timedelta(seconds=(end - start))

        return task
    
    def validate(self, work: CallSpec):
        try:
            work(self.validators)
        except ValueError as e:
            raise TaskValidationError(f"Failed to validate task {work.functionName}") from e
        except KeyError:
            raise TaskNotFound(f"Task {work.functionName} does not exist in this Queue")

    def insert(self, task: Task):
        self.tasksCollection.insert_one(
            task.model_dump()
        )

    def _add(
            self, 
            work:           CallSpec,
            deadline:       datetime, 
            factory:        TaskFactory
        ):

        self.insert(
            factory._emit(
                deadline=deadline,
                work=work
            ) 
        )

    def schedule(
            self, 
            work:           CallSpec, 
            deadline:       datetime,
            factory:        TaskFactory | None = None
        ):

        factory = factory or self.defaultFactory

        self.validate(work)

        self._add(
            work=work,
            deadline=deadline, 
            factory=factory
        )

    def scheduleFromDistribution(
            self,
            work:           CallSpec,
            distribution:   CallSpec,
            factory:        TaskFactory | None = None
        ):

        self._queue.distribution.validate(distribution)
        deadline = utc_now() + self._queue.distribution.gen(distribution)

        self.schedule(
            work=work,
            deadline=deadline,
            factory=factory
        )

    def __call__(self, functionName: str, **kwargs) -> CallSpec:
        obj = CallSpec.new(functionName, **kwargs)
        self.validate(obj)
        return obj


class Scheduler(TaskFactory):
    name:           str                 = "Scheduler"
    status:         SCHEDULER_STATUS    = "enabled"
    work:           CallSpec
    distribution:   CallSpec
    deadline:       datetime

    def _emit(self, deadline: datetime) -> Task:
        return super()._emit(self.work, deadline)
    
    def __repr__(self) -> str:
        return f"Scheduler {self.name}: {self.work}"

class SchedulerEngine:
    def __init__(
            self, 
            queue:                  BaseQueue,
            schedulersCollection:   Collection | None = None,
            taskEngine:             TaskEngine | None = None,
            poolInterval:           float = 1,
            policy:                 OVERDUE_SCHEDULES_POLICY = "execute reconstructed"
        ):

        self._queue = queue
        self.poolInterval = poolInterval
        self.schedulersCollection: Collection = schedulersCollection if schedulersCollection is not None else queue.schedulersCollection
        self.taskEngine: TaskEngine = taskEngine or queue.task
        self.policy: OVERDUE_SCHEDULES_POLICY = policy

        self.createIndexes()

    def init(self):
        now = utc_now()

        self.schedulersCollection.update_many(
            {"status": "processing"},
            {"$set": {"status": "enabled"}}
        )

        if self.policy == "execute reconstructed":
            return
        
        if self.policy == "execute once":
            self.schedulersCollection.update_many(
                {"deadline": {"$lte": now}},
                {"$set": {"deadline": now}}
            )
        
        if self.policy == "skip":
            schedulers = [
                Scheduler.model_validate(raw)
                for raw in self.schedulersCollection.find(
                    {"deadline": {"$lte": now}}
                )
            ]

            if schedulers:
                self.schedulersCollection.bulk_write([
                    UpdateOne(
                        {"uid": s.uid},
                        {"$set": {"deadline": now + self.taskEngine.distributionEngine.gen(s.distribution)}}
                    )
                    for s in schedulers
                ])


    def createIndexes(self):
        self.schedulersCollection.create_index([("status", 1), ("deadline", 1)])  # _work() claim + init() policies
        self.schedulersCollection.create_index([("uid", 1)])  # _work() update deadline

    def _work(self):
        now = utc_now()
        raw = self.schedulersCollection.find_one_and_update(
            {"deadline": {"$lte": now}, "status": {"$ne": "processing"}},
            {"$set": {"status": "processing"}},
            sort=[("deadline", 1)]
        )

        if not raw:
            return
        
        scheduler = Scheduler.model_validate(raw)

        if scheduler.status == "enabled":
            self.taskEngine.insert(
                scheduler._emit(deadline=scheduler.deadline)
            )

        deadline = scheduler.deadline + self.taskEngine.distributionEngine.gen(scheduler.distribution)

        self.schedulersCollection.update_one(
            {"uid": scheduler.uid},
            {"$set": {"status": scheduler.status, "deadline": deadline}}
        )

        if deadline <= utc_now():
            pass  # logging.warn(f"{scheduler} is being throtled")

    def work(self):
        while True:
            self._work()
            time.sleep(self.poolInterval)

    def startWorkers(self, workerCount: int):
        threads = [
            threading.Thread(
                target=self.work,
                daemon=True
            ) for i in range(workerCount)
        ]

        for t in threads:
            t.start()

    def validate(
            self, 
            work:           CallSpec, 
            distribution:   CallSpec
        ):

        self.taskEngine.distributionEngine.validate(distribution)
        self.taskEngine.validate(work)

    def insert(self, scheduler: Scheduler):
        self.schedulersCollection.insert_one(
            scheduler.model_dump()
        )

    def _add(
            self,
            work:           CallSpec,
            distribution:   CallSpec
        ):

        deadline = utc_now() + self.taskEngine.distributionEngine.gen(distribution)

        self.insert(
            Scheduler(
                work=work,
                distribution=distribution, 
                deadline=deadline
            )
        )

    def add(
            self, 
            work:           CallSpec, 
            distribution:   CallSpec
        ):

        self.validate(
            distribution=distribution,
             work=work
        )

        self._add(
            distribution=distribution,
            work=work
        )



def task(obj):  # task decorator
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
            name: obj
            for name, obj in candidates.items()
            if inspect.isfunction(obj)
            and getattr(obj, "__is_task__", False)
        }

    def __init__(
            self, 
            queueDB:                    Database, 
            distributionsRegistry:      type[BaseDistributions] = BaseDistributions,
            taskPoolInterval:           float = 1,
            schedulerPoolInterval:      float = 1,
            overdueTaskPolicy:          OVERDUE_TASKS_POLICY = "execute now",
            overdueSchedulersPolicy:    OVERDUE_SCHEDULES_POLICY = "execute once"
        ):

        self.defaultFactory: TaskFactory = TaskFactory(name="default")
        
        # prepare DB
        self.tasksCollection: Collection = queueDB["pymonque_tasks"]
        self.schedulersCollection: Collection = queueDB["pymonque_schedulers"]

        self.distribution: DistributionEngine = DistributionEngine(distributionsRegistry)
        self.task: TaskEngine = TaskEngine(self, poolInterval=taskPoolInterval, policy=overdueTaskPolicy)
        self.scheduler: SchedulerEngine = SchedulerEngine(self, poolInterval=schedulerPoolInterval, policy=overdueSchedulersPolicy)

        """
        self.tasksCollection.update_many(
            {"status": "pending", "functionName": {"$nin": list(self.tasks.keys())}},
            {"$set": {"status": "incompatible", "error": "Function for this task does not exist in the TaskClass"}}
        )  # flag pending tasks that cannot be executed

        self.schedulersCollection.update_many(
            {"status": "enabled", "functionName": {"$nin": list(self.tasks.keys())}}, 
            {"$set": {"status": "disabled"}}
        )  # disable Schedulers that emit tasks that cannot be executed
        """

        self.task.init()
        self.scheduler.init()

    def startWorkers(self, taskWorkers: int | None = None, schedulerWorkers: int | None = None):
        self.task.startWorkers(taskWorkers or 0)
        self.scheduler.startWorkers(schedulerWorkers or 0)

    def _work(self):
        while True:
            self.task._work()
            self.scheduler._work()
