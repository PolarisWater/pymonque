from __future__ import annotations
from pydantic import (
    create_model, field_validator, field_serializer, model_validator,
    ConfigDict, BaseModel, Field, PrivateAttr, PositiveFloat
)

from typing import (
    Any, Literal, Callable, TypeVar, Mapping, Iterable, Sequence, Generic, Self,
    get_type_hints, overload
)

from types import MethodType

from pymongo.database import Database
from pymongo.collection import Collection
from pymongo import ReturnDocument, IndexModel
from pymongo.errors import DuplicateKeyError

from datetime import datetime, timedelta, timezone

import random
import math
import time
import uuid
import traceback
import threading
import inspect
import logging
import hashlib
import socket
import os
import signal

from contextlib import contextmanager

from pymonque.exceptions import (
    TaskValidationError, TaskNotFound,
    DistributionValidationError, DistributionNotFound,
    VersionMismatch, UnboundDocument, TaskTimeout
)


logger = logging.getLogger("pymonque")

T = TypeVar("T")
P = TypeVar("P")
M = TypeVar("M", bound="Document")
TASK_STATUS = Literal["pending", "success", "processing", "failed", "timeout", "canceled", "outdated", "incompatible"]
# "processing" is no longer written — a scheduler being worked is one with a live
# lease. It stays in the type so documents from older versions still validate.
SCHEDULER_STATUS = Literal["enabled", "disabled", "processing"]
ITEM_STATUS = Literal["pending", "claimed", "done", "failed"]

OVERDUE_TASKS_POLICY = Literal["skip", "execute now"]
OVERDUE_SCHEDULES_POLICY = Literal["skip", "execute once", "execute reconstructed"]
STALE_ITEMS_POLICY = Literal["retry", "fail"]

LEASE_SECONDS = 300         # how long a claim is held before it is considered abandoned
BACKLOG_WARN_AFTER = 60     # seconds work may sit due before the app says nobody is free
BACKLOG_INTERVAL = 30       # seconds between those checks. None as the threshold disables them
HEARTBEAT_INTERVAL = 15     # seconds between a worker process checking in
WORKER_STALE_AFTER = 60     # after this long without checking in, a worker is gone
HOSTNAME = socket.gethostname()

def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def getStaticmethods(cls: type) -> dict[str, Callable]:
    return {
        name: obj.__func__
        for base in reversed(cls.__mro__)  # walk trough all parents
        for name, obj in base.__dict__.items()
        if isinstance(obj, staticmethod) and not name.startswith("_")  # add only staticmethods
    }

def buildValidator(func: Callable) -> type[BaseModel]:
    """A pydantic model of the call's keyword arguments, used to check a CallSpec
    when it is scheduled rather than when a worker picks it up."""

    sig = inspect.signature(func)
    hints = get_type_hints(func, include_extras=True)

    fields = {}
    extra = "forbid"

    for name, param in sig.parameters.items():
        # A CallSpec is {functionName, kwargs}, so anything that can only be passed
        # positionally could never be filled in. Say so where it is written, not on
        # every call that tries to use it.
        if param.kind in (param.VAR_POSITIONAL, param.POSITIONAL_ONLY):
            raise TypeError(
                f"{func.__name__} takes {'*' if param.kind is param.VAR_POSITIONAL else ''}"
                f"{name} positionally; a task is called with keyword arguments only"
            )

        if param.kind is param.VAR_KEYWORD:
            extra = "allow"     # **kwargs: whatever else is passed is the function's business
            continue

        annotation = hints.get(name, object)    # unannotated means anything

        if param.default is inspect._empty:
            default = ...
        else:
            default = param.default

        fields[name] = (annotation, default)

    return create_model(
        f"{func.__name__}_Args",
        **fields,
        __config__ = ConfigDict(extra=extra)
    )

def uuid4str() -> str:
    return str(uuid.uuid4())

SCHEDULER_NAMESPACE = uuid.UUID("1f0e3d4c-5b6a-4798-8a9b-0c1d2e3f4a5b")

def schedulerUid(name: str) -> str:
    """A stable uid for a named scheduler, so declaring it twice declares it once."""

    return str(uuid.uuid5(SCHEDULER_NAMESPACE, name))


MONGO_CONFIG = ConfigDict(serialize_by_alias=True)  # aliases are how a model matches an existing schema


class Document(BaseModel):
    """A model stored in a collection, identified by `uid`.

    A document handed back by an engine remembers where it came from, so it can
    save and delete itself. One built by hand is unbound until you insert it.
    """

    model_config = MONGO_CONFIG

    uid: str = Field(default_factory=uuid4str)

    _engine: Any = PrivateAttr(default=None)
    _storedKey: Any = PrivateAttr(default=None)

    def bind(self, engine: CollectionEngine) -> Self:
        """Remember the engine, and the key this document is stored under.

        Keeping the key is what makes changing one a rename: save() writes over
        the row it came from instead of leaving the old one behind as a copy.
        """

        self._engine = engine
        self._storedKey = getattr(self, engine.key, None)

        return self

    @property
    def bound(self) -> bool:
        return self._engine is not None

    @property
    def storedKey(self) -> Any:
        """The key this document was last written under, or None if never written."""

        return self._storedKey

    def _requireEngine(self) -> CollectionEngine:
        if self._engine is None:
            raise UnboundDocument(
                f"{type(self).__name__} is not attached to a collection — insert it "
                f"through an engine, or fetch it from one, before saving it"
            )

        return self._engine

    def save(self) -> Self:
        return self._requireEngine().save(self)

    def delete(self) -> bool:
        engine = self._requireEngine()

        return engine.delete(self._storedKey)

    def reload(self) -> Self | None:
        engine = self._requireEngine()

        return engine.get(self._storedKey)


class CollectionEngine(Generic[M]):
    """Typed storage for one collection of documents.

    The base every other engine is built on: it knows a model and a collection,
    and nothing about tasks, schedules or work.
    """

    def __init__(
            self,
            app:          BaseApp,
            name:           str,
            model:          type[M],
            collection:     Collection | str | None = None,
            key:            str = "uid",
            extraIndexes:   Sequence[IndexModel] | None = None
        ):

        self._app = app
        self.name = name
        self.model: type[M] = model
        self.key = key
        self.extraIndexes = extraIndexes

        if collection is None:
            self.collection: Collection = app.db[self.defaultCollectionName()]
        elif isinstance(collection, str):
            self.collection = app.db[collection]
        else:
            self.collection = collection

        self.createIndexes()

    def defaultCollectionName(self) -> str:
        return self.name  # a collection of your own objects, in your own namespace

    def init(self):
        """Startup housekeeping. Nothing to do for plain storage."""

    def createIndexes(self):
        self.collection.create_index([(self.key, 1)], unique=True)

        if self.extraIndexes:
            self.collection.create_indexes(list(self.extraIndexes))

    # --- reading ---

    def load(self, raw: Mapping[str, Any]) -> M:
        return self.model.model_validate(raw).bind(self)

    def get(self, key: Any) -> M | None:
        raw = self.collection.find_one({self.key: key})

        return self.load(raw) if raw else None

    def findOne(self, where: Mapping[str, Any] | None = None) -> M | None:
        raw = self.collection.find_one(dict(where) if where else {})

        return self.load(raw) if raw else None

    def find(
            self,
            where:  Mapping[str, Any] | None = None,
            sort:   Sequence[tuple[str, int]] | None = None,
            limit:  int | None = None
        ) -> list[M]:

        cursor = self.collection.find(dict(where) if where else {})

        if sort:
            cursor = cursor.sort(list(sort))

        if limit:
            cursor = cursor.limit(limit)

        return [self.load(raw) for raw in cursor]

    def count(self, where: Mapping[str, Any] | None = None) -> int:
        return self.collection.count_documents(dict(where) if where else {})

    def exists(self, key: Any) -> bool:
        return self.collection.count_documents({self.key: key}, limit=1) > 0

    # --- writing ---

    def build(self, **fields) -> M:
        return self.model(**fields).bind(self)

    def create(self, **fields) -> M:
        """Build a document, store it, and hand it back bound."""

        return self.insert(self.build(**fields))

    def insert(self, document: M) -> M:
        self.collection.insert_one(document.model_dump())

        return document.bind(self)

    def insertMany(self, documents: Iterable[M]) -> list[M]:
        documents = list(documents)

        if documents:
            self.collection.insert_many([d.model_dump() for d in documents])

        return [d.bind(self) for d in documents]

    def save(self, document: M) -> M:
        """Store the document as it is now, creating it if it is not there yet.

        A document that came from here is written over the row it came from, so
        changing its key renames it rather than leaving a copy behind.
        """

        stored = document.storedKey if document.bound else None
        match = stored if stored is not None else getattr(document, self.key)

        self.collection.replace_one(
            {self.key: match},
            document.model_dump(),
            upsert=True
        )

        return document.bind(self)

    def update(self, key: Any, **fields) -> M | None:
        """Merge fields into a stored document, without reading it first."""

        self.collection.update_one(
            {self.key: key},
            {"$set": {
                name: value.model_dump() if isinstance(value, BaseModel) else value
                for name, value in fields.items()
            }}
        )

        return self.get(key)

    def delete(self, key: Any) -> bool:
        return self.collection.delete_one({self.key: key}).deleted_count > 0

    def deleteMany(self, where: Mapping[str, Any]) -> int:
        return self.collection.delete_many(dict(where)).deleted_count

    def __repr__(self) -> str:
        return f"{type(self).__name__} {self.name} ({self.collection.name})"


class CallSpec(BaseModel):
    model_config = MONGO_CONFIG

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
    
    def bind(self, **kwargs) -> CallSpec:
        """A copy of this call with extra kwargs merged in."""

        return CallSpec(
            functionName=self.functionName,
            kwargs={**self.kwargs, **kwargs}
        )

    def __repr__(self) -> str:
        return f"{self.functionName}({self.kwargs})"

class FuncSpec:
    def __init__(self, func: Callable):
        self.__is_task__ = True
        self._func: Callable = func

    def __call__(self, **kwargs):
        return CallSpec.new(self._func.__name__, **kwargs)

    def __repr__(self):
        return repr(self._func)


class BaseDistributions:
    @classmethod
    def _getDistributions(cls) -> dict[str, Callable]:
        return getStaticmethods(cls)
    
    # dailyFrequency is PositiveFloat throughout: zero divides, and a negative
    # interval walks a scheduler backwards, which no overdue policy can stop.

    @staticmethod
    def constant(dailyFrequency: PositiveFloat) -> timedelta:
        interval_sec = 86400 / dailyFrequency
        return timedelta(seconds=interval_sec)
    
    @staticmethod
    def normal(dailyFrequency: PositiveFloat, stdFraction: float) -> timedelta:
        mean_sec = 86400 / dailyFrequency
        std_sec = mean_sec * stdFraction
        interval_sec = random.gauss(mean_sec, std_sec)
        # a wide enough spread draws below zero; floor it well clear of it
        return timedelta(seconds=max(interval_sec, mean_sec / 100))

    @staticmethod
    def lognormal(dailyFrequency: PositiveFloat, sigma: float) -> timedelta:
        mean_sec = 86400 / dailyFrequency
        mu = math.log(mean_sec) - (sigma**2)/2
        interval_sec = random.lognormvariate(mu, sigma)
        return timedelta(seconds=interval_sec)

    @staticmethod
    def exponential(dailyFrequency: PositiveFloat) -> timedelta:
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
            raise DistributionValidationError(f"Failed to validate distribution {distribution!r}") from e
        except KeyError:
            raise DistributionNotFound(f"Distribution {distribution.functionName} does not exist in this app")

    def gen(self, distribution: CallSpec) -> timedelta:
        delta = distribution(self.functions)

        if not isinstance(delta, timedelta):
            raise DistributionValidationError(
                f"distribution {distribution!r} did not return a timedelta")

        # catches a custom distribution too: a scheduler on a non-positive interval
        # never moves forward, so it emits on every poll for as long as it exists
        if delta <= timedelta(0):
            raise DistributionValidationError(
                f"distribution {distribution!r} returned {delta}; an interval must be positive")

        return delta
    
    def __call__(self, functionName: str, **kwargs) -> CallSpec:
        obj = CallSpec.new(functionName, **kwargs)
        self.validate(obj)
        return obj


class Task(Document):
    status:         TASK_STATUS         = "pending"
    work:           CallSpec
    deadline:       datetime
    factory:        TaskFactory

    # when this becomes claimable: its deadline while pending, the end of the
    # holder's lease while processing. One field, so claiming is one comparison.
    leaseUntil:     datetime | None     = None

    executionTime:  timedelta | None    = None
    result:         Any | None          = None
    error:          str | None          = None

    # Both None means the engine's, and the engine's None means the app's.
    # Nearest wins.
    timeout:        float | None        = None   # seconds this call may run for
    skipAfter:      float | None        = None   # seconds past the deadline it stops being worth running

    @model_validator(mode="after")
    def defaultLease(self):
        if self.leaseUntil is None:
            self.leaseUntil = self.deadline

        return self

    @field_serializer("executionTime")
    @staticmethod
    def serialize_executionTime(value: timedelta | None) -> float | None:
        if value is not None:
            return float(value.total_seconds())

    @field_validator("executionTime", mode="before")
    @staticmethod
    def validate_executionTime(value: Any) -> timedelta | None:
        if value is None or isinstance(value, timedelta):
            return value

        if isinstance(value, (int, float, str)):
            return timedelta(seconds=float(value))

        raise TypeError(f"executionTime should not be a {type(value)}  ({value})")
    
    def __repr__(self) -> str:
        return f"Task {self.work!r} from {self.factory!r}"

class TaskFactory(Document):
    name:   str

    def _emit(
            self, 
            work:       CallSpec,
            deadline:   datetime,
            timeout:    float | None = None,
            skipAfter:  float | None = None
        ) -> Task:

        return Task(
            work=work,
            deadline=deadline, 
            factory=self,
            timeout=timeout,
            skipAfter=skipAfter
        )
    
    def __repr__(self) -> str:
        return f"Factory {self.name}"

class WorkerLoop:
    """Thread lifecycle shared by the engines that poll.

    Workers check for a stop request between iterations, never inside one, so a
    shutdown always finishes the work already claimed before it exits.
    """

    workerLabel: str = "worker"
    poolInterval: float
    leaseSeconds: float

    def _initWorkers(self):
        self.workerCount: int = 0
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._held: set[str] = set()
        self._heldLock = threading.Lock()

    def _work(self):
        raise NotImplementedError

    @property
    def workCollection(self) -> Collection:
        raise NotImplementedError

    # --- leases ---

    def _hold(self, uid: str):
        with self._heldLock:
            self._held.add(uid)

    def _releaseHold(self, uid: str):
        with self._heldLock:
            self._held.discard(uid)

    def renewLeases(self) -> int:
        """Push back the lease on everything this process is working on.

        Without this a task that outlives its lease would be claimed a second
        time while the first worker is still running it.
        """

        with self._heldLock:
            uids = list(self._held)

        if not uids:
            return 0

        return self.workCollection.update_many(
            {"uid": {"$in": uids}},
            {"$set": {"leaseUntil": utc_now() + timedelta(seconds=self.leaseSeconds)}}
        ).modified_count

    def _renewLoop(self):
        while not self._stop.is_set():
            self._stop.wait(max(1.0, self.leaseSeconds / 3))

            try:
                self.renewLeases()
            except Exception:
                logger.exception("%s lease renewal failed", self.workerLabel)

    @property
    def running(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def idleSleep(self):
        """Jittered, so workers started together don't wake in lockstep.

        Waits on the stop event rather than sleeping, so a shutdown does not have
        to sit through a whole poll interval.
        """

        self._stop.wait(self.poolInterval * (0.9 + random.random() * 0.2))

    def work(self):
        while not self._stop.is_set():
            try:
                worked = self._work()
            except Exception:
                logger.exception("%s worker iteration failed", self.workerLabel)
                worked = None

            if not worked:
                self.idleSleep()  # only wait when there was nothing to do

    def startWorkers(self, workerCount: int):
        if workerCount <= 0:
            return

        self.workerCount += workerCount
        self._stop.clear()

        threads = [
            threading.Thread(
                target=self.work,
                name=f"pymonque-{self.workerLabel}-{n}",
                daemon=True
            ) for n in range(workerCount)
        ]

        threads.append(threading.Thread(
            target=self._renewLoop,
            name=f"pymonque-{self.workerLabel}-lease",
            daemon=True
        ))

        for t in threads:
            t.start()

        self._threads.extend(threads)

    def stopWorkers(self, timeout: float | None = None) -> bool:
        """Ask workers to stop and wait for the work in flight to finish.

        Returns False if any worker was still busy when `timeout` ran out.
        """

        self._stop.set()

        deadline = None if timeout is None else time.monotonic() + timeout

        for t in self._threads:
            if not t.is_alive():
                continue

            t.join(None if deadline is None else max(0, deadline - time.monotonic()))

        self._threads = [t for t in self._threads if t.is_alive()]

        if not self._threads:
            self.workerCount = 0

        return not self._threads


class TaskEngine(CollectionEngine, WorkerLoop):
    workerLabel = "task"

    def __init__(
            self, 
            app:              BaseApp,
            tasksCollection:    Collection | str | None = None,
            poolInterval:       float = 1,
            policy:             OVERDUE_TASKS_POLICY = "execute now",
            defaultFactory:     TaskFactory | None = None,
            leaseSeconds:       float = LEASE_SECONDS,
            taskModel:          type[Task] = Task,
            extraIndexes:       Sequence[IndexModel] | None = None,
            timeout:            float | None = None,
            skipAfter:          float | None = None
        ):

        self.poolInterval = poolInterval
        self.leaseSeconds = leaseSeconds
        self.timeout: float | None = timeout if timeout is not None else app.taskTimeout
        self.skipAfter: float | None = skipAfter if skipAfter is not None else app.taskSkipAfter
        self.defaultFactory: TaskFactory = defaultFactory or app.defaultFactory
        self.distributionEngine: DistributionEngine = app.distribution
        self.policy: OVERDUE_TASKS_POLICY = policy

        # resolve tasks
        self.functions: dict[str, Callable] = {
            name: getattr(app, name)
            for name in type(app)._getTasks()
        }

        # build kwarg validators
        self.validators: dict[str, type[BaseModel]] = {
            name: buildValidator(func)
            for name, func in self.functions.items()
        }

        self._initWorkers()

        super().__init__(
            app,
            name="tasks",
            model=taskModel,
            collection=tasksCollection if tasksCollection is not None else app.tasksCollection,
            extraIndexes=extraIndexes
        )

    @property
    def tasksCollection(self) -> Collection:
        return self.collection

    @property
    def workCollection(self) -> Collection:
        return self.collection

    def backfill(self):
        """Give documents written before leases existed one, so they stay claimable."""

        self.tasksCollection.update_many(
            {"leaseUntil": None},   # missing or null
            [{"$set": {"leaseUntil": "$deadline"}}],
        )

    def init(self):
        """Startup housekeeping. Only ever run by a process that starts workers —
        never on construction, where it would disturb whatever else is running."""

        now = utc_now()
        self.backfill()

        self.tasksCollection.update_many(
            {"status": "pending", "work.functionName": {"$nin": list(self.functions)}},
            {"$set": {"status": "incompatible", "error": "Function for this task does not exist in this app"}},
        )  # flag pending tasks that can no longer be executed

        # Only the first workers back apply it: joining a running cluster is not a
        # return from downtime, and the backlog there belongs to somebody.
        if self.policy == "skip" and self._app.coldStart():
            skipped = self.tasksCollection.update_many(
                {"status": "pending", "deadline": {"$lte": now}},
                {"$set": {"status": "outdated"}},
            ).modified_count

            if skipped:
                logger.warning(
                    "%d task(s) were due before this cold start and are outdated "
                    "(overdueTaskPolicy)", skipped
                )

    def createIndexes(self):
        super().createIndexes()  # unique uid
        self.collection.create_index([("status", 1), ("leaseUntil", 1)])  # _work() claim
        self.collection.create_index([("status", 1), ("deadline", 1)])  # init() skip/outdate

    def _work(self):
        now = utc_now()
        raw = self.tasksCollection.find_one_and_update(
            {
                # due, or claimed by a worker that is no longer renewing its lease
                "status": {"$in": ["pending", "processing"]},
                "leaseUntil": {"$lte": now},
                "work.functionName": {"$in": list(self.functions)},  # never claim what we cannot run
            },
            {"$set": {"status": "processing", "leaseUntil": now + timedelta(seconds=self.leaseSeconds)}},
            sort=[("leaseUntil", 1)]
        )

        if not raw:
            return

        task = self.load(raw)
        task.leaseUntil = now + timedelta(seconds=self.leaseSeconds)  # raw is the pre-claim image

        if self._tooLate(task, now):
            return self._outdate(task, now)

        self._hold(task.uid)
        try:
            task = self.execute(task)
        finally:
            self._releaseHold(task.uid)

        try:
            self.tasksCollection.update_one({"uid": task.uid}, {"$set": task.model_dump()})
        except Exception:  # e.g. a result the driver cannot encode
            task.status = "failed"
            task.result = None
            task.error = traceback.format_exc()

            self.tasksCollection.update_one({"uid": task.uid}, {"$set": task.model_dump()})

        return task     # truthy, so the worker loop knows not to sleep

    def timeoutFor(self, task: Task) -> float | None:
        """Nearest wins: the task's own, then this engine's, then the app's."""

        return task.timeout if task.timeout is not None else self.timeout

    def skipAfterFor(self, task: Task) -> float | None:
        """Nearest wins, the same way."""

        return task.skipAfter if task.skipAfter is not None else self.skipAfter

    def _tooLate(self, task: Task, now: datetime) -> bool:
        """Whether this task went stale waiting to be claimed.

        The companion to overdueTaskPolicy, for the other way work goes stale:
        that one is about a cold start after downtime, this one about a queue
        nobody is keeping up with.
        """

        limit = self.skipAfterFor(task)

        return limit is not None and (now - task.deadline).total_seconds() > limit

    def _outdate(self, task: Task, now: datetime) -> Task:
        late = (now - task.deadline).total_seconds()

        task.status = "outdated"
        task.error = (
            f"claimed {late:.0f}s after its deadline, past the "
            f"{self.skipAfterFor(task)}s it was worth running for"
        )

        self.tasksCollection.update_one({"uid": task.uid}, {"$set": task.model_dump()})
        logger.warning("%r was outdated: %s", task, task.error)

        return task

    def _callWithTimeout(self, task: Task, timeout: float) -> Any:
        """Run the call, and give up waiting for it after `timeout` seconds.

        Python cannot interrupt a running call, so the thread is left to finish in
        its own time — it is a daemon and does not hold the process open. What the
        timeout guarantees is that the *worker* is freed and the task is written
        off, which is the part every other process can see.
        """

        outcome: dict[str, Any] = {}

        def run():
            try:
                outcome["result"] = task.work(self.functions)
            except BaseException as e:      # carried back to the claiming thread
                outcome["error"] = e

        thread = threading.Thread(
            target=run, name=f"pymonque-task-{task.work.functionName}", daemon=True
        )
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            raise TaskTimeout(
                f"{task.work.functionName} did not finish within {timeout}s and was "
                f"written off; the call itself cannot be interrupted and may still be running"
            )

        if "error" in outcome:
            raise outcome["error"]

        return outcome.get("result")

    def execute(self, task: Task) -> Task:
        timeout = self.timeoutFor(task)
        start = time.perf_counter()

        try:
            task.result = (
                task.work(self.functions) if timeout is None
                else self._callWithTimeout(task, timeout)
            )
            task.status = "success"
        except TaskTimeout as e:
            task.status = "timeout"
            task.error = str(e)
            logger.warning("%r timed out after %ss", task, timeout)
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
            raise TaskNotFound(f"Task {work.functionName} does not exist in this app")

    def _add(
            self, 
            work:           CallSpec,
            deadline:       datetime, 
            factory:        TaskFactory,
            timeout:        float | None = None,
            skipAfter:      float | None = None
        ) -> Task:

        return self.insert(
            factory._emit(
                deadline=deadline,
                work=work,
                timeout=timeout,
                skipAfter=skipAfter
            ) 
        )

    def _notStarted(self) -> dict[str, Any]:
        """Matches tasks nobody is running: waiting, or claimed by a worker that
        stopped renewing its lease."""

        return {"$or": [
            {"status": "pending"},
            {"status": "processing", "leaseUntil": {"$lte": utc_now()}},
        ]}

    def cancel(self, uid: str) -> bool:
        """Cancel a task that has not started.

        Returns False if it is already running, or already finished — a running
        task cannot be interrupted, only waited out.
        """

        return self.tasksCollection.update_one(
            {"uid": uid, **self._notStarted()},
            {"$set": {"status": "canceled"}}
        ).modified_count > 0

    def cancelMany(self, where: Mapping[str, Any] | None = None) -> int:
        """Cancel every task matching `where` that has not started yet."""

        notStarted = self._notStarted()
        query = {"$and": [dict(where), notStarted]} if where else notStarted

        return self.tasksCollection.update_many(
            query,
            {"$set": {"status": "canceled"}}
        ).modified_count

    def schedule(
            self, 
            work:           CallSpec, 
            deadline:       datetime | None = None,
            factory:        TaskFactory | None = None,
            timeout:        float | None = None,
            skipAfter:      float | None = None
        ) -> Task:

        """Queue one call. `timeout` and `skipAfter` override the engine's and the app's."""

        factory = factory or self.defaultFactory
        deadline = deadline or utc_now()

        self.validate(work)

        return self._add(
            work=work,
            deadline=deadline, 
            factory=factory,
            timeout=timeout,
            skipAfter=skipAfter
        )

    def scheduleFromDistribution(
            self,
            work:           CallSpec,
            distribution:   CallSpec,
            factory:        TaskFactory | None = None,
            timeout:        float | None = None,
            skipAfter:      float | None = None
        ) -> Task:

        self._app.distribution.validate(distribution)
        deadline = utc_now() + self._app.distribution.gen(distribution)

        return self.schedule(
            work=work,
            deadline=deadline,
            factory=factory,
            timeout=timeout,
            skipAfter=skipAfter
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
    leaseUntil:     datetime | None     = None
    timeout:        float | None        = None   # both stamped onto every task it emits
    skipAfter:      float | None        = None

    @model_validator(mode="after")
    def defaultLease(self):
        if self.leaseUntil is None:
            self.leaseUntil = self.deadline

        return self

    def emitWork(self) -> CallSpec:
        """The call this scheduler emits. Override to stamp context onto every task.

            class AccountScheduler(Scheduler):
                accountId: int

                def emitWork(self) -> CallSpec:
                    return self.work.bind(accountId=self.accountId)
        """

        return self.work

    def _emit(self, deadline: datetime) -> Task:
        return super()._emit(self.emitWork(), deadline, self.timeout, self.skipAfter)
    
    def __repr__(self) -> str:
        return f"Scheduler {self.name}: {self.work!r}"

class SchedulerEngine(CollectionEngine, WorkerLoop):
    workerLabel = "scheduler"

    def __init__(
            self, 
            app:                  BaseApp,
            schedulersCollection:   Collection | str | None = None,
            taskEngine:             TaskEngine | None = None,
            poolInterval:           float = 1,
            policy:                 OVERDUE_SCHEDULES_POLICY = "execute once",
            schedulerModel:         type[Scheduler] = Scheduler,
            extraIndexes:           Sequence[IndexModel] | None = None,
            leaseSeconds:           float = LEASE_SECONDS
        ):

        self.poolInterval = poolInterval
        self.leaseSeconds = leaseSeconds
        self.taskEngine: TaskEngine = taskEngine or app.task
        self.policy: OVERDUE_SCHEDULES_POLICY = policy

        self._initWorkers()

        super().__init__(
            app,
            name="schedulers",
            model=schedulerModel,
            collection=schedulersCollection if schedulersCollection is not None else app.schedulersCollection,
            extraIndexes=extraIndexes
        )

    @property
    def schedulersCollection(self) -> Collection:
        return self.collection

    @property
    def schedulerModel(self) -> type[Scheduler]:
        return self.model

    @property
    def workCollection(self) -> Collection:
        return self.collection

    @staticmethod
    def _deadline(deadline: datetime) -> dict[str, datetime]:
        """A scheduler that is not being worked is claimable exactly at its deadline,
        so every write of one has to move the lease with it."""

        return {"deadline": deadline, "leaseUntil": deadline}

    def backfill(self):
        self.schedulersCollection.update_many(
            {"leaseUntil": None},   # missing or null
            [{"$set": {"leaseUntil": "$deadline"}}],
        )

        # "processing" used to mean "being worked"; the lease says that now, so a
        # document left in it by an older version would otherwise never emit again
        self.schedulersCollection.update_many(
            {"status": "processing"},
            {"$set": {"status": "enabled"}},
        )

    def init(self):
        """Startup housekeeping. Only run by a process that starts workers.

        The overdue policy is not applied here: a scheduler falls behind while
        running just as easily as while nothing runs, so _work() applies it every
        time it claims one.
        """

        self.backfill()

        self.schedulersCollection.update_many(
            {"status": "enabled", "work.functionName": {"$nin": list(self.taskEngine.functions)}},
            {"$set": {"status": "disabled"}},
        )  # disable schedulers emitting tasks that can no longer be executed

    def createIndexes(self):
        super().createIndexes()  # unique uid: one scheduler per ensure() name
        self.collection.create_index([("status", 1), ("leaseUntil", 1)])  # _work() claim

    def _work(self):
        now = utc_now()
        # Holding a scheduler is the lease's job, so the claim leaves `status`
        # alone — it means enabled/disabled and nothing else. A scheduler whose
        # holder stopped renewing is picked up here with its status intact.
        raw = self.schedulersCollection.find_one_and_update(
            {"leaseUntil": {"$lte": now}},
            {"$set": {"leaseUntil": now + timedelta(seconds=self.leaseSeconds)}},
            sort=[("leaseUntil", 1)]
        )

        if not raw:
            return
        
        scheduler = self.schedulerModel.model_validate(raw)
        interval = self.taskEngine.distributionEngine.gen(scheduler.distribution)

        # A whole beat came and went unworked, so this scheduler cannot keep its
        # cadence — either nothing was running, or it is set faster than it can be
        # served. Which is the only moment the policy has anything to say.
        behind = scheduler.deadline + interval <= now
        replay = self.policy == "execute reconstructed"

        self._hold(scheduler.uid)
        try:
            if scheduler.status == "enabled" and not (behind and self.policy == "skip"):
                self.taskEngine.insert(
                    scheduler._emit(deadline=scheduler.deadline)
                )
        finally:
            self._releaseHold(scheduler.uid)

        # Only "execute reconstructed" walks the backlog beat by beat. The others
        # resume from now, so time spent behind is not time owed.
        deadline = scheduler.deadline + interval if replay or not behind else now + interval

        self.schedulersCollection.update_one(
            {"uid": scheduler.uid},
            {"$set": self._deadline(deadline)}
        )

        if behind:
            logger.warning(
                "%r missed a beat of %s (policy: %s)", scheduler, interval, self.policy
            )

        return scheduler

    def validate(
            self, 
            work:           CallSpec, 
            distribution:   CallSpec
        ):

        self.taskEngine.distributionEngine.validate(distribution)
        self.taskEngine.validate(work)

    def validateScheduler(self, scheduler: Scheduler):
        """Validate what the scheduler will actually emit, context included."""

        self.validate(
            work=scheduler.emitWork(),
            distribution=scheduler.distribution
        )

    def build(
            self,
            work:           CallSpec,
            distribution:   CallSpec,
            deadline:       datetime | None = None,
            **fields
        ) -> Scheduler:

        """An unsaved scheduler of this engine's model. Extra fields go to the subclass."""

        if deadline is None:
            self.taskEngine.distributionEngine.validate(distribution)  # before generating from it

        return self.schedulerModel(
            work=work,
            distribution=distribution,
            deadline=deadline or utc_now() + self.taskEngine.distributionEngine.gen(distribution),
            **fields
        )

    def add(
            self, 
            work:           CallSpec, 
            distribution:   CallSpec,
            **fields
        ) -> Scheduler:

        scheduler = self.build(work, distribution, **fields)

        self.validateScheduler(scheduler)
        self.insert(scheduler)

        return scheduler

    def byUid(self, uid: str) -> Scheduler | None:
        return self.get(uid)

    def byName(self, name: str) -> Scheduler | None:
        """The scheduler declared under this name by ensure()."""

        return self.get(schedulerUid(name))

    def upsert(self, scheduler: Scheduler) -> Scheduler:
        """Store a scheduler under its own uid, creating or replacing it."""

        self.validateScheduler(scheduler)

        return self.save(scheduler)

    def update(
            self,
            uid:            str,
            work:           CallSpec | None = None,
            distribution:   CallSpec | None = None,
            enabled:        bool | None = None,
            **fields
        ) -> Scheduler | None:

        """Change parts of a stored scheduler, or None if there is no such uid.

        A new distribution restarts the rhythm from now. Everything is validated as
        the merged scheduler, so context fields are checked against the work.
        """

        existing = self.byUid(uid)

        if existing is None:
            return None

        changes: dict[str, Any] = dict(fields)

        if work is not None:
            changes["work"] = work

        if distribution is not None:
            changes["distribution"] = distribution

        merged = existing.model_copy(update=changes)
        self.validateScheduler(merged)

        dumped = merged.model_dump()
        update: dict[str, Any] = {key: dumped[key] for key in changes if key in dumped}

        if distribution is not None:
            update.update(self._deadline(utc_now() + self.taskEngine.distributionEngine.gen(distribution)))

        if enabled is not None:
            update["status"] = "enabled" if enabled else "disabled"

        if update:
            self.schedulersCollection.update_one(
                {"uid": uid},
                {"$set": update}
            )

        return self.byUid(uid)

    def ensure(
            self,
            name:           str,
            work:           CallSpec,
            distribution:   CallSpec,
            enabled:        bool | None = None,
            **fields
        ) -> Scheduler:

        """Declare a scheduler that should always exist.

        Safe to call on every startup: the name identifies the scheduler, so it is
        created the first time and kept in step with the declaration afterwards.
        Changing the work updates it in place; changing the distribution also
        restarts its rhythm. The enabled/disabled state set in the database is left
        alone unless `enabled` is passed.
        """

        uid = schedulerUid(name)
        existing = self.schedulersCollection.find_one({"uid": uid})

        if existing is None:
            scheduler = self.build(
                work,
                distribution,
                uid=uid,
                name=name,
                status="disabled" if enabled is False else "enabled",
                **fields
            )

            self.validateScheduler(scheduler)

            try:
                self.schedulersCollection.update_one(
                    {"uid": uid},
                    {"$setOnInsert": scheduler.model_dump()},
                    upsert=True
                )  # processes booting together all declare it; only one inserts
            except DuplicateKeyError:
                pass  # another process won the race, its document stands

            return self.byName(name)

        changes: dict[str, Any] = {**fields, "work": work, "distribution": distribution}

        merged = self.schedulerModel.model_validate(existing).model_copy(update=changes)
        self.validateScheduler(merged)

        dumped = merged.model_dump()
        update: dict[str, Any] = {key: dumped[key] for key in changes if key in dumped}

        if existing.get("distribution") != distribution.model_dump():
            # the rhythm itself changed, so start the new one from now
            update.update(self._deadline(utc_now() + self.taskEngine.distributionEngine.gen(distribution)))

        if enabled is not None:
            update["status"] = "enabled" if enabled else "disabled"

        self.schedulersCollection.update_one(
            {"uid": uid},
            {"$set": update}
        )

        return self.byName(name)

    def removeNamed(self, name: str) -> bool:
        """Delete the scheduler declared under this name."""

        return self.delete(schedulerUid(name))

    remove = removeNamed


class Item(Document, Generic[P]):
    status:         ITEM_STATUS         = "pending"
    data:           P

    createdAt:      datetime            = Field(default_factory=utc_now)
    claimedAt:      datetime | None     = None
    finishedAt:     datetime | None     = None
    leaseUntil:     datetime | None     = None
    attempts:       int                 = 0

    result:         Any | None          = None
    error:          str | None          = None

    @model_validator(mode="after")
    def defaultLease(self):
        if self.leaseUntil is None:
            self.leaseUntil = self.createdAt  # claimable as soon as it exists

        return self

    def __repr__(self) -> str:
        return f"Item {self.uid} ({self.status})"

class PileEngine(CollectionEngine):
    """A pile of work items living in its own collection.

    Unlike TaskEngine and SchedulerEngine this engine runs no workers of its own:
    items are pulled by whatever is already running — typically a task that a
    scheduler fires on a rhythm.
    """

    def __init__(
            self,
            app:              BaseApp,
            name:               str,
            payload:            type[BaseModel] | None = None,
            itemsCollection:    Collection | str | None = None,
            policy:             STALE_ITEMS_POLICY = "retry",
            leaseSeconds:       float = LEASE_SECONDS
        ):

        self.leaseSeconds = leaseSeconds
        self.payload: type[BaseModel] | None = payload
        self.policy: STALE_ITEMS_POLICY = policy

        self._held: set[str] = set()
        self._heldLock = threading.Lock()
        self._renewer: threading.Thread | None = None

        super().__init__(
            app,
            name=name,
            # a concrete Item type whose `data` is validated against the payload model
            model=Item[payload] if payload is not None else Item[dict[str, Any]],
            collection=itemsCollection
        )

    def defaultCollectionName(self) -> str:
        return f"pymonque_pile_{self.name}"

    @property
    def itemsCollection(self) -> Collection:
        return self.collection

    def backfill(self):
        self.collection.update_many(
            {"leaseUntil": None},   # missing or null
            [{"$set": {"leaseUntil": "$createdAt"}}],
        )

    def init(self):
        """Under "retry" there is nothing to do — an expired lease is claimable
        again on its own. "fail" is the one that needs saying out loud, and claim()
        says it too, so a stale item does not wait for a boot to be resolved."""

        self.backfill()

        if self.policy == "fail":
            self._failStale(utc_now())

    def _failStale(self, now: datetime) -> int:
        """Finish items whose holder stopped renewing.

        A live worker renews, so a lapsed lease means nobody is holding it — which
        is why this can run from any process without disturbing another's work.
        """

        return self.collection.update_many(
            {"status": "claimed", "leaseUntil": {"$lte": now}},
            {"$set": {
                "status": "failed",
                "error": "the worker holding this item stopped renewing its lease",
                "finishedAt": now,
            }},
        ).modified_count

    def createIndexes(self):
        super().createIndexes()  # unique uid
        self.collection.create_index([("status", 1), ("leaseUntil", 1)])  # claim()

    # --- filling the pile ---

    def _build(self, data: Any) -> Item:
        return self.model(data=data)

    def add(self, data: Any = None, **kwargs) -> Item:
        return self.insert(self._build(data if data is not None else kwargs))

    def addMany(self, data: Iterable[Any]) -> list[Item]:
        return self.insertMany(self._build(d) for d in data)

    # --- taking work out of it ---

    def claim(self, where: Mapping[str, Any] | None = None) -> Item | None:
        """Atomically take the oldest pending item, or None if the pile is empty.

        The claim is a single find_one_and_update, so two workers racing on the
        same pile can never receive the same item.
        """

        now = utc_now()

        # "retry" picks up items whose holder stopped renewing; "fail" retires them
        # here instead, so the policy means the same thing between boots as at one.
        if self.policy == "fail":
            self._failStale(now)

        claimable = ["pending", "claimed"] if self.policy == "retry" else ["pending"]

        query: dict[str, Any] = {
            "status": {"$in": claimable},
            "leaseUntil": {"$lte": now},
        }
        if where:
            query.update(where)

        raw = self.collection.find_one_and_update(
            query,
            {
                "$set": {
                    "status": "claimed",
                    "claimedAt": now,
                    "leaseUntil": now + timedelta(seconds=self.leaseSeconds),
                },
                "$inc": {"attempts": 1},
            },
            # a pending item's lease is its createdAt, so this is still oldest-first
            sort=[("leaseUntil", 1)],
            return_document=ReturnDocument.AFTER
        )

        if not raw:
            return None

        return self.load(raw)

    @staticmethod
    def _uid(item: Item | str) -> str:
        return item if isinstance(item, str) else item.uid

    def _finish(
            self,
            item:       Item | str,
            status:     ITEM_STATUS,
            result:     Any = None,
            error:      str | None = None
        ) -> bool:

        update: dict[str, Any] = {"status": status, "finishedAt": utc_now()}

        if result is not None:
            update["result"] = result

        if error is not None:
            update["error"] = error

        # matched, not modified: the question is whether there is such an item,
        # not whether the bytes happened to change
        return self.collection.update_one(
            {"uid": self._uid(item)},
            {"$set": update}
        ).matched_count > 0

    def done(self, item: Item | str, result: Any = None) -> bool:
        """Mark an item finished. False if there is no such item."""

        return self._finish(item, "done", result=result)

    def fail(self, item: Item | str, error: str | None = None) -> bool:
        return self._finish(item, "failed", error=error)

    def release(self, item: Item | str) -> bool:
        """Put a claimed item back on the pile without consuming an outcome."""

        return self.collection.update_one(
            {"uid": self._uid(item)},
            # back to its own place in the pile, not the end of it
            [{"$set": {"status": "pending", "claimedAt": None, "leaseUntil": "$createdAt"}}]
        ).matched_count > 0

    def renewLease(self, item: Item | str) -> bool:
        """Hold on to an item for another lease period."""

        return self.collection.update_one(
            {"uid": self._uid(item)},
            {"$set": {"leaseUntil": utc_now() + timedelta(seconds=self.leaseSeconds)}}
        ).matched_count > 0

    def renewLeases(self) -> int:
        """Push back the lease on every item this process is working on."""

        with self._heldLock:
            uids = list(self._held)

        if not uids:
            return 0

        return self.collection.update_many(
            {"uid": {"$in": uids}},
            {"$set": {"leaseUntil": utc_now() + timedelta(seconds=self.leaseSeconds)}}
        ).modified_count

    def _renewLoop(self):
        """One loop for the whole pile, started on the first claim it has to hold."""

        while True:
            time.sleep(max(1.0, self.leaseSeconds / 3))

            with self._heldLock:
                idle = not self._held

            if idle:
                with self._heldLock:
                    self._renewer = None
                return

            try:
                self.renewLeases()
            except Exception:
                logger.exception("pile lease renewal failed")

    def _hold(self, item: Item):
        with self._heldLock:
            self._held.add(item.uid)

            if self._renewer is None:
                self._renewer = threading.Thread(
                    target=self._renewLoop,
                    name=f"pymonque-pile-{self.name}-lease",
                    daemon=True
                )
                self._renewer.start()

    def _releaseHold(self, item: Item):
        with self._heldLock:
            self._held.discard(item.uid)

    @contextmanager
    def work(self, where: Mapping[str, Any] | None = None):
        """Claim one item, mark it done on success and failed on exception.

        Yields None when the pile is empty. The exception is re-raised, so a task
        driving the pile fails alongside the item.
        """

        item = self.claim(where)

        if item is None:
            yield None
            return

        self._hold(item)

        try:
            yield item
        except Exception:
            self.fail(item, traceback.format_exc())
            raise
        else:
            self.done(item)
        finally:
            self._releaseHold(item)

    # --- managing the collection ---

    def count(
            self,
            where:  Mapping[str, Any] | None = None,
            status: ITEM_STATUS | None = None
        ) -> int:

        query = dict(where) if where else {}

        if status:
            query["status"] = status

        return super().count(query)

    def counts(self) -> dict[str, int]:
        return {
            status: self.count(status=status)
            for status in ("pending", "claimed", "done", "failed")
        }

    def purge(self, status: ITEM_STATUS = "done") -> int:
        return self.deleteMany({"status": status})

    def __repr__(self) -> str:
        return f"Pile {self.name} ({self.collection.name})"

class pile:
    """Declare a pile of work on a BaseApp subclass.

        class App(BaseApp):
            emails = pile(EmailPayload)
    """

    def __init__(
            self,
            payload:            type[BaseModel] | None = None,
            itemsCollection:    Collection | str | None = None,
            policy:             STALE_ITEMS_POLICY | None = None,
            leaseSeconds:       float | None = None
        ):

        self.__is_pile__: bool = True
        self.payload = payload
        self.itemsCollection = itemsCollection
        self.policy = policy
        self.leaseSeconds = leaseSeconds
        self.name: str = ""

    def __set_name__(self, owner, name: str):
        self.name = name

    def _engine(self, app: BaseApp, policy: STALE_ITEMS_POLICY, leaseSeconds: float) -> PileEngine:
        return PileEngine(
            app,
            name=self.name,
            payload=self.payload,
            itemsCollection=self.itemsCollection,
            policy=self.policy or policy,
            leaseSeconds=self.leaseSeconds if self.leaseSeconds is not None else leaseSeconds
        )

    def __get__(self, obj, objtype=None) -> PileEngine | pile:
        if obj is None:
            return self

        return obj.piles[self.name]

    def __repr__(self) -> str:
        return f"pile {self.name}"


class collection:
    """Declare a typed collection of documents on a BaseApp subclass.

        class App(BaseApp):
            groups = collection(Group)          # -> the "groups" collection
    """

    def __init__(
            self,
            model:          type[Document],
            collection:     Collection | str | None = None,
            key:            str = "uid",
            extraIndexes:   Sequence[IndexModel] | None = None
        ):

        if not (isinstance(model, type) and issubclass(model, Document)):
            raise TypeError(
                f"collection() needs a Document subclass, not {getattr(model, '__name__', model)!r}. "
                f"A stored model needs a uid and the ability to save itself; "
                f"subclass pymonque.Document rather than pydantic's BaseModel."
            )

        self.__is_collection__: bool = True
        self.model = model
        self.collection = collection
        self.key = key
        self.extraIndexes = extraIndexes
        self.name: str = ""

    def __set_name__(self, owner, name: str):
        self.name = name

    def _engine(self, app: BaseApp) -> CollectionEngine:
        return CollectionEngine(
            app,
            name=self.name,
            model=self.model,
            collection=self.collection,
            key=self.key,
            extraIndexes=self.extraIndexes
        )

    def __get__(self, obj, objtype=None) -> CollectionEngine | collection:
        if obj is None:
            return self

        return obj.collections[self.name]

    def __repr__(self) -> str:
        return f"collection {self.name} ({self.model.__name__})"


class schedulers:
    """Declare a scheduler engine on a BaseApp subclass.

        class App(BaseApp):
            accountOps = schedulers(AccountScheduler, "accountOperations")

    Declaring one named `scheduler` replaces the app's default engine.
    """

    def __init__(
            self,
            schedulerModel:         type[Scheduler] = Scheduler,
            schedulersCollection:   Collection | str | None = None,
            policy:                 OVERDUE_SCHEDULES_POLICY | None = None,
            poolInterval:           float | None = None,
            taskEngine:             TaskEngine | None = None,
            extraIndexes:           Sequence[IndexModel] | None = None,
            leaseSeconds:           float | None = None
        ):

        if not (isinstance(schedulerModel, type) and issubclass(schedulerModel, Scheduler)):
            raise TypeError(
                f"schedulers() needs a Scheduler subclass, not "
                f"{getattr(schedulerModel, '__name__', schedulerModel)!r}"
            )

        self.__is_schedulers__: bool = True
        self.leaseSeconds = leaseSeconds
        self.schedulerModel = schedulerModel
        self.schedulersCollection = schedulersCollection
        self.policy = policy
        self.poolInterval = poolInterval
        self.taskEngine = taskEngine
        self.extraIndexes = extraIndexes
        self.name: str = ""

    def __set_name__(self, owner, name: str):
        self.name = name

    def _engine(
            self,
            app:          BaseApp,
            policy:         OVERDUE_SCHEDULES_POLICY,
            poolInterval:   float,
            leaseSeconds:   float
        ) -> SchedulerEngine:

        collection = self.schedulersCollection
        if collection is None:
            collection = "pymonque_schedulers" if self.name == "scheduler" else f"pymonque_schedulers_{self.name}"

        return SchedulerEngine(
            app,
            schedulersCollection=collection,
            taskEngine=self.taskEngine,
            poolInterval=self.poolInterval if self.poolInterval is not None else poolInterval,
            policy=self.policy or policy,
            schedulerModel=self.schedulerModel,
            extraIndexes=self.extraIndexes,
            leaseSeconds=self.leaseSeconds if self.leaseSeconds is not None else leaseSeconds
        )

    def __get__(self, obj, objtype=None) -> SchedulerEngine | schedulers:
        if obj is None:
            return self

        return obj.schedulerEngines[self.name]

    def __repr__(self) -> str:
        return f"schedulers {self.name} ({self.schedulerModel.__name__})"


class task:
    def __init__(self, func: Callable):
        self.func: Callable = func.__func__ if isinstance(func, staticmethod) else func
        self.__is_staticmethod__: bool = isinstance(func, staticmethod)
        self.__is_task__: bool = True

    def __get__(self, obj, objtype=None) -> Callable:
        if obj is None:
            return FuncSpec(self.func)

        if self.__is_staticmethod__:
            return self.func  # nothing to bind

        return MethodType(self.func, obj)
    
class BaseApp:
    @classmethod
    def _getDeclared(cls, flag: str) -> dict[str, Any]:
        return {
            name: obj
            for base in reversed(cls.__mro__)  # walk trough all parents
            for name, obj in base.__dict__.items()
            if getattr(obj, flag, False)
        }  # resolve child overrides

    @classmethod
    def _getTasks(cls) -> dict[str, task]:
        return cls._getDeclared("__is_task__")

    @classmethod
    def _getPiles(cls) -> dict[str, pile]:
        return cls._getDeclared("__is_pile__")

    @classmethod
    def _getCollections(cls) -> dict[str, collection]:
        return cls._getDeclared("__is_collection__")

    @classmethod
    def _getSchedulerEngines(cls) -> dict[str, schedulers]:
        return cls._getDeclared("__is_schedulers__")

    # Policies are declared on the class, never passed in: every process that
    # imports this app must agree on them, and they are part of the fingerprint
    # so two that disagree cannot both run workers.
    overdueTaskPolicy:          OVERDUE_TASKS_POLICY      = "execute now"
    overdueSchedulersPolicy:    OVERDUE_SCHEDULES_POLICY  = "execute once"
    staleItemsPolicy:           STALE_ITEMS_POLICY        = "retry"

    # Seconds a task may run before it is written off, and seconds past its
    # deadline before it stops being worth running at all. None means no limit;
    # an engine or an individual task may set its own.
    taskTimeout:                float | None              = None
    taskSkipAfter:              float | None              = None

    def __init__(
            self, 
            db:                    Database, 
            distributionsRegistry:      type[BaseDistributions] = BaseDistributions,
            taskPoolInterval:           float = 1,
            schedulerPoolInterval:      float = 1,
            leaseSeconds:               float = LEASE_SECONDS,
            enforceVersion:             bool = True,
            heartbeatInterval:          float = HEARTBEAT_INTERVAL,
            workerStaleAfter:           float = WORKER_STALE_AFTER,
            backlogWarnAfter:           float | None = BACKLOG_WARN_AFTER,
            backlogInterval:            float = BACKLOG_INTERVAL
        ):

        self.defaultFactory: TaskFactory = TaskFactory(name="default")
        self.leaseSeconds = leaseSeconds

        self.enforceVersion = enforceVersion
        self.heartbeatInterval = heartbeatInterval
        self.workerStaleAfter = workerStaleAfter
        self.workerUid: str = uuid4str()

        # local diagnostics, not shared behaviour: two processes may log differently
        self.backlogWarnAfter = backlogWarnAfter
        self.backlogInterval = backlogInterval

        self._stopping: bool = False
        self._quit = threading.Event()      # what the monitor threads wait on
        self._monitors: dict[str, threading.Thread] = {}
        self._previousHandlers: dict[int, Any] = {}

        self._prepareDB(db)

        self.distribution: DistributionEngine = DistributionEngine(distributionsRegistry)

        self._buildStorage()
        self._buildEngines(taskPoolInterval, schedulerPoolInterval)

        # NB: init() is deliberately not called here. Constructing an app must be
        # safe from any process at any time; startup housekeeping belongs to a
        # process that is actually taking over as a worker. See init().

    def _prepareDB(self, db: Database):
        self.db: Database = db
        self.tasksCollection: Collection = db["pymonque_tasks"]
        self.schedulersCollection: Collection = db["pymonque_schedulers"]
        self.workersCollection: Collection = db["pymonque_workers"]

        self.workersCollection.create_index([("lastSeen", 1)])
        self.workersCollection.create_index([("uid", 1)], unique=True)

    def _buildStorage(self):
        """Collections and piles, resolved before the engines so tasks can reach them."""

        self.collections: dict[str, CollectionEngine] = {
            name: spec._engine(self)
            for name, spec in type(self)._getCollections().items()
        }

        self.piles: dict[str, PileEngine] = {
            name: spec._engine(self, self.staleItemsPolicy, self.leaseSeconds)
            for name, spec in type(self)._getPiles().items()
        }

    def _buildEngines(self, taskPoolInterval: float, schedulerPoolInterval: float):

        self.task: TaskEngine = TaskEngine(
            self, poolInterval=taskPoolInterval, policy=self.overdueTaskPolicy,
            leaseSeconds=self.leaseSeconds
        )

        self.schedulerEngines: dict[str, SchedulerEngine] = {
            name: spec._engine(self, self.overdueSchedulersPolicy, schedulerPoolInterval, self.leaseSeconds)
            for name, spec in type(self)._getSchedulerEngines().items()
        }

        # a declaration named `scheduler` replaces the default engine
        if "scheduler" not in self.schedulerEngines:
            self.scheduler: SchedulerEngine = SchedulerEngine(
                self, poolInterval=schedulerPoolInterval, policy=self.overdueSchedulersPolicy,
                leaseSeconds=self.leaseSeconds
            )
            self.schedulerEngines["scheduler"] = self.scheduler

    # --- one version of the code at a time ---

    @property
    def fingerprint(self) -> str:
        """Identifies the executable surface of this app: its task and distribution
        names with their signatures, and the policies its engines apply.

        Two processes that disagree on this are running different code and must
        not work the same collections. It cannot see a changed function *body* —
        nothing can, reliably — so this is a guard, not a proof. Deploy one
        version at a time.

        Policies are in here because they are shared behaviour: one process
        retrying stale pile items while another fails them is a split brain over
        the same documents, not two harmless local settings.
        """

        parts = [
            f"{name}{inspect.signature(func)}"
            for registry in (self.task.functions, self.distribution.functions)
            for name, func in sorted(registry.items())
        ]

        parts += [
            f"{engine.name}:{engine.policy}"
            for engine in (self.task, *self.schedulerEngines.values(), *self.piles.values())
        ]

        parts.append(f"taskTimeout:{self.task.timeout}")
        parts.append(f"taskSkipAfter:{self.task.skipAfter}")

        return hashlib.sha1("\n".join(parts).encode()).hexdigest()[:12]

    # --- is anyone keeping up ---

    def backlog(self) -> tuple[int, float]:
        """Work that is due and nobody has picked up, and how long the oldest piece
        has been waiting.

        A free worker claims within one poll interval, so anything waiting much
        longer than that means every worker is busy.
        """

        now = utc_now()
        due = {
            "status": {"$in": ["pending", "processing"]},
            "leaseUntil": {"$lte": now},
            "work.functionName": {"$in": list(self.task.functions)},
        }

        count = self.task.tasksCollection.count_documents(due)

        if not count:
            return 0, 0.0

        oldest = self.task.tasksCollection.find_one(due, sort=[("leaseUntil", 1)])

        return count, (now - oldest["leaseUntil"]).total_seconds()

    def otherLiveWorkers(self) -> list[dict]:
        """Every process checked in but this one."""

        return [w for w in self.liveWorkers() if w.get("uid") != self.workerUid]

    def coldStart(self) -> bool:
        """True when nothing else is running.

        overdueTaskPolicy is about coming back from downtime, so it must not fire
        for a worker joining a cluster that never went down — that one would drop
        work its colleagues were about to run. With enforceVersion off there is no
        registry to ask, and every start looks cold.
        """

        return not self.otherLiveWorkers()

    def taskWorkers(self) -> int:
        """Task workers across every process that has checked in.

        A scheduler-only process runs none of its own, so counting locally would
        cry wolf at a perfectly good deployment. Falls back to this process alone
        when it is not registering.
        """

        if not self.enforceVersion:
            return self.task.workerCount

        return sum(w.get("taskWorkers", 0) for w in self.liveWorkers())

    def _checkWorkers(self):
        """Say plainly when there is work due and nobody free to take it."""

        try:
            due, waiting = self.backlog()
            workers = self.taskWorkers()
        except Exception:
            logger.exception("backlog check failed")
            return

        if waiting < self.backlogWarnAfter:
            return

        if workers:
            logger.warning(
                "%d task(s) due, oldest waiting %.0fs, %d task worker(s) running — "
                "not enough workers, or they are all on long tasks",
                due, waiting, workers
            )
        else:
            logger.warning(
                "%d task(s) due, oldest waiting %.0fs, and no process is running task workers",
                due, waiting
            )

    def _watchBacklog(self):
        while not self._quit.wait(self.backlogInterval):
            self._checkWorkers()

    def liveWorkers(self) -> list[dict]:
        """Worker processes that have checked in recently."""

        return list(self.workersCollection.find(
            {"lastSeen": {"$gte": utc_now() - timedelta(seconds=self.workerStaleAfter)}},
            {"_id": 0}
        ))

    def _verifyVersion(self):
        """Raise if a live worker is running different code.

        Anything that writes on other processes' behalf goes through here, not
        just startWorkers(): housekeeping decided by this process's task list is
        only safe if this process's task list is everyone's.
        """

        fingerprint = self.fingerprint

        conflict = next(
            (w for w in self.liveWorkers() if w.get("fingerprint") != fingerprint),
            None
        )

        if conflict is not None:
            raise VersionMismatch(
                f"a live worker is running a different version of this app "
                f"({conflict.get('fingerprint')} on {conflict.get('host')}:{conflict.get('pid')}, "
                f"this process is {fingerprint}). Only one version may run at a time — "
                f"stop the old workers before starting these."
            )

    def _claimVersion(self):
        """Verify, then register this process as a worker."""

        self._verifyVersion()

        self.workersCollection.update_one(
            {"uid": self.workerUid},
            {"$set": {
                "uid":          self.workerUid,
                "fingerprint":  self.fingerprint,
                "host":         HOSTNAME,
                "pid":          os.getpid(),
                "startedAt":    utc_now(),
                "lastSeen":     utc_now(),
                "taskWorkers":  0,      # filled in once the engines are up
            }},
            upsert=True
        )

    def _deregisterWorker(self):
        """Free this process's slot immediately, rather than waiting for it to go stale."""

        try:
            self.workersCollection.delete_one({"uid": self.workerUid})
        except Exception:
            logger.exception("could not deregister worker")

    def _monitor(self, name: str, target: Callable[[], None]):
        """Start one background loop, once. A second startWorkers() must not leave
        a second heartbeat behind."""

        running = self._monitors.get(name)

        if running is not None and running.is_alive():
            return

        thread = threading.Thread(target=target, name=f"pymonque-{name}", daemon=True)
        self._monitors[name] = thread
        thread.start()

    def _heartbeat(self):
        while not self._quit.wait(self.heartbeatInterval):

            try:
                self.workersCollection.update_one(
                    {"uid": self.workerUid},
                    {"$set": {"lastSeen": utc_now()}}
                )
            except Exception:
                logger.exception("worker heartbeat failed")

    @property
    def engines(self) -> list[WorkerLoop]:
        return [self.task, *self.schedulerEngines.values()]

    def init(self):
        """Startup housekeeping: backfill leases, and resolve documents whose task
        no longer exists on this app.

        Run by startWorkers(), not by the constructor — a process that only
        enqueues or reads must never disturb what the workers are doing. It is
        version-checked for the same reason: it decides what is runnable from this
        process's task list, so a process holding a different one must not run it.
        """

        if self.enforceVersion:
            self._verifyVersion()

        self.task.init()

        for engine in self.schedulerEngines.values():
            engine.init()

        for pile in self.piles.values():
            pile.init()

        for store in self.collections.values():
            store.init()

    def startWorkers(self, taskWorkers: int | None = None, schedulerWorkers: int | None = None):
        """Start workers on the task engine and on every scheduler engine."""

        self._stopping = False
        self._quit.clear()

        if self.enforceVersion:
            self._claimVersion()
            self._monitor("heartbeat", self._heartbeat)

        self.init()

        self.task.startWorkers(taskWorkers or 0)

        for engine in self.schedulerEngines.values():
            engine.startWorkers(schedulerWorkers or 0)

        if self.enforceVersion:
            self.workersCollection.update_one(
                {"uid": self.workerUid},
                {"$set": {"taskWorkers": self.task.workerCount}}
            )

        if self.backlogWarnAfter is not None:
            self._checkWorkers()    # say it now if the backlog is already old
            self._monitor("backlog", self._watchBacklog)

    # --- shutting down ---

    @property
    def running(self) -> bool:
        return any(engine.running for engine in self.engines)

    @property
    def stopping(self) -> bool:
        return self._stopping

    def requestStop(self):
        """Stop claiming new work, without waiting for what is in flight."""

        self._stopping = True
        self._quit.set()        # heartbeat and backlog wake immediately

        for engine in self.engines:
            engine._stop.set()

    def stopWorkers(self, timeout: float | None = 30) -> bool:
        """Stop claiming, let the work already claimed finish, then return.

        Returns False if anything was still busy when `timeout` ran out — the
        threads are daemons, so leaving the process at that point abandons them
        and their tasks are recovered on the next startup.
        """

        self.requestStop()

        deadline = None if timeout is None else time.monotonic() + timeout
        drained = True

        for engine in self.engines:
            remaining = None if deadline is None else max(0, deadline - time.monotonic())

            if not engine.stopWorkers(remaining):
                drained = False

        self._deregisterWorker()

        if not drained:
            logger.warning("shutdown timed out with work still in flight")

        return drained

    def joinWorkers(self, timeout: float | None = None) -> bool:
        """Block until the workers stop. Use this instead of a sleep loop."""

        deadline = None if timeout is None else time.monotonic() + timeout

        for engine in self.engines:
            for t in engine._threads:
                t.join(None if deadline is None else max(0, deadline - time.monotonic()))

        return not self.running

    def handleSignals(
            self,
            timeout:    float | None = 30,
            signals:    Sequence[int] = (signal.SIGINT, signal.SIGTERM)
        ):

        """Turn SIGINT/SIGTERM into a graceful shutdown.

        The first signal stops claiming and lets in-flight work finish; a second
        one exits immediately. Opt-in, because a host framework may want to own
        these — call it only from a process pymonque is running.

        SIGKILL cannot be caught: the process dies with work in flight, which is
        what the startup recovery policies are for.
        """

        def onSignal(signum, frame):
            if self._stopping:
                logger.warning("second signal (%s), exiting now", signum)
                os._exit(128 + signum)

            logger.info(
                "signal %s: finishing work in flight, not claiming more", signum
            )
            self.requestStop()

        for s in signals:
            self._previousHandlers[s] = signal.getsignal(s)
            signal.signal(s, onSignal)

        self._shutdownTimeout = timeout

    def restoreSignals(self):
        for s, handler in self._previousHandlers.items():
            signal.signal(s, handler)

        self._previousHandlers.clear()

    def run(self, taskWorkers: int | None = None, schedulerWorkers: int | None = None,
            timeout: float | None = 30) -> bool:
        """Start workers, handle signals, and block until shutdown.

            if __name__ == "__main__":
                q.run(taskWorkers=4, schedulerWorkers=1)
        """

        self.handleSignals(timeout=timeout)
        self.startWorkers(taskWorkers, schedulerWorkers)

        try:
            self.joinWorkers()
        finally:
            drained = self.stopWorkers(timeout)
            self.restoreSignals()

        return drained
