from __future__ import annotations
from pydantic import (
    create_model, field_validator, field_serializer, model_validator,
    ConfigDict, BaseModel, Field, PrivateAttr, PositiveFloat, ValidationError
)

from typing import (
    Any, Literal, Callable, TypeVar, Mapping, Iterable, Sequence, Generic, Self,
    get_type_hints, get_args, overload
)

from types import MethodType

from pymongo.database import Database
from pymongo.collection import Collection
from pymongo import ReturnDocument, IndexModel
from pymongo.errors import DuplicateKeyError
import bson

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
import re

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
SCHEDULER_STATUS = Literal["enabled", "disabled"]   # being worked is the lease's job, not a status
ITEM_STATUS = Literal["pending", "claimed", "done", "failed"]
FINAL_TASK_STATUSES = ("success", "failed", "timeout", "canceled", "outdated", "incompatible")

# what a scheduler owes for beats that went by unworked: none of them, one, or every one
MISSED_BEATS = Literal["skip", "once", "replay"]

LEASE_SECONDS = 300         # how long a claim is held before it is considered abandoned
BACKLOG_WARN_AFTER = 60     # seconds work may sit due before the app says nobody is free
BACKLOG_INTERVAL = 30       # seconds between those checks. None as the threshold disables them
HEARTBEAT_INTERVAL = 15     # seconds between a worker process checking in
WORKER_STALE_AFTER = 60     # after this long without checking in, a worker is gone
MAX_ATTEMPTS = 1            # runs a task or item gets before a failure is final: retrying is opt-in
RETRY_DELAY = 60            # seconds a failed task or item waits before it is claimable again

HOSTNAME = socket.gethostname()


def checkMissed(missed: str) -> str:
    if missed not in get_args(MISSED_BEATS):
        raise ValueError(
            f"missed is one of {', '.join(map(repr, get_args(MISSED_BEATS)))}, not {missed!r}"
        )

    return missed

def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def nearestAttributes(cls: type) -> dict[str, Any]:
    """Every name defined on a class or its parents, resolved to the definition that
    wins — so a subclass that overrides a declaration with something else replaces
    it, rather than leaving the parent's registered beside it. Kept in the order
    names were first defined."""

    resolved: dict[str, Any] = {}

    for base in reversed(cls.__mro__):  # furthest first, so nearer definitions overwrite
        resolved.update(base.__dict__)

    return resolved

def getStaticmethods(cls: type) -> dict[str, Callable]:
    return {
        name: obj.__func__
        for name, obj in nearestAttributes(cls).items()
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

def callValidated(func: Callable, validator: type[BaseModel], kwargs: Mapping[str, Any]) -> Any:
    """Call with the arguments as the signature declares them.

    A CallSpec is stored as plain data, so a model argument comes back as a dict,
    and a value that validated by coercion ("3" for an int) is still the original.
    Validating again at the call hands the function what its annotations promise.
    """

    validated = validator.model_validate(dict(kwargs))
    arguments = {name: getattr(validated, name) for name in type(validated).model_fields}

    return func(**arguments, **(validated.model_extra or {}))

def uuid4str() -> str:
    return str(uuid.uuid4())

SCHEDULER_NAMESPACE = uuid.UUID("1f0e3d4c-5b6a-4798-8a9b-0c1d2e3f4a5b")

def schedulerUid(name: str) -> str:
    """A stable uid for a named scheduler, so declaring it twice declares it once."""

    return str(uuid.uuid5(SCHEDULER_NAMESPACE, name))

def beatUid(scheduler: str, deadline: datetime) -> str:
    """The uid of the task a scheduler emits for one deadline. Fixed, so a worker
    that dies after emitting but before moving the deadline on cannot emit it twice."""

    return str(uuid.uuid5(SCHEDULER_NAMESPACE, f"{scheduler}@{deadline.isoformat()}"))


MONGO_CONFIG = ConfigDict(serialize_by_alias=True)  # aliases are how a model matches an existing schema


class TaskLimits(BaseModel):
    """What a task may do, declared on @task and filled in from the app's defaults.

    Limits describe the function — how long it may run, whether it is safe to run
    again — not one call of it, so they live in code rather than on stored tasks.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    timeout:        float | None    = Field(default=None, gt=0)   # seconds a call may run; None: no limit
    skipAfter:      float | None    = Field(default=None, ge=0)   # seconds past its deadline it is still worth running; None: always
    maxAttempts:    int             = Field(default=MAX_ATTEMPTS, ge=1)  # runs before a failure is final
    retryDelay:     float           = Field(default=RETRY_DELAY, ge=0)   # seconds before a retry is claimable


class _Unset:
    """A limit left off @task, as opposed to one given as None, which means no limit."""

    def __repr__(self) -> str:
        return "<the app's default>"

UNSET = _Unset()


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

    def _prepare(self, document: M) -> M:
        """Keep derived fields in step before a document is written. Nothing to do
        for plain storage."""

        return document

    def _assign(self, document: M, fields: Mapping[str, Any]) -> M:
        """Set fields the way constructing the model would: validated and coerced,
        raising before anything is written."""

        for name, value in fields.items():
            document.__pydantic_validator__.validate_assignment(document, name, value)

        return document

    def save(self, document: M) -> M:
        """Store the document as it is now, creating it if it is not there yet.

        A document that came from here is written over the row it came from, so
        changing its key renames it rather than leaving a copy behind.
        """

        stored = document.storedKey if document.bound else None
        match = stored if stored is not None else getattr(document, self.key)

        self.collection.replace_one(
            {self.key: match},
            self._prepare(document).model_dump(),
            upsert=True
        )

        return document.bind(self)

    def update(self, key: Any, **fields) -> M | None:
        """Merge fields into a stored document, or None if there is no such one.

        The fields are validated against the model first, so nothing invalid is
        written, and only what changed is — the fields given, and anything kept in
        step with them.
        """

        document = self.get(key)

        if document is None:
            return None

        before = document.model_dump()
        after = self._prepare(self._assign(document, fields)).model_dump()
        changed = {name: value for name, value in after.items() if before.get(name) != value}

        if changed:
            self.collection.update_one({self.key: key}, {"$set": changed})

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
    # interval walks a scheduler backwards, which no missed-beats rule can stop.

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
        name = distribution.functionName

        if name not in self.functions:
            raise DistributionNotFound(f"Distribution {name} does not exist in this app")

        try:
            delta = callValidated(self.functions[name], self.validators[name], distribution.kwargs)
        except ValidationError as e:
            raise DistributionValidationError(f"Failed to validate distribution {distribution!r}") from e

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

    attempts:       int                 = 0      # claims so far, including ones that crashed

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

    def _emit(self, work: CallSpec, deadline: datetime) -> Task:
        return Task(work=work, deadline=deadline, factory=self)
    
    def __repr__(self) -> str:
        return f"Factory {self.name}"

class WorkerLoop:
    """Thread lifecycle shared by the engines that poll.

    Workers check for a stop request between iterations, never inside one, so a
    shutdown always finishes the work already claimed before it exits.
    """

    workerLabel: str = "worker"
    pollInterval: float
    leaseSeconds: float

    # what a held document must still match to be renewed, so a renewal racing the
    # write of an outcome cannot overwrite what that write set
    renewOnly: Mapping[str, Any] = {}

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
            {"uid": {"$in": uids}, **self.renewOnly},
            {"$set": {"leaseUntil": utc_now() + timedelta(seconds=self.leaseSeconds)}}
        ).modified_count

    def _renewLoop(self, workers: list[threading.Thread]):
        """Renew until these workers have stopped — not merely been asked to. A
        shutdown lets work in flight finish, and that work must keep its lease."""

        interval = max(1.0, self.leaseSeconds / 3)

        while alive := [t for t in workers if t.is_alive()]:
            alive[0].join(interval)     # returns early if it exits, so this ends promptly

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

        self._stop.wait(self.pollInterval * (0.9 + random.random() * 0.2))

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
            args=(list(threads),),
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
    renewOnly = {"status": "processing"}    # not a retry, whose lease is its retry time

    def __init__(
            self, 
            app:              BaseApp,
            tasksCollection:    Collection | str | None = None,
            pollInterval:       float = 1,
            defaultFactory:     TaskFactory | None = None,
            leaseSeconds:       float = LEASE_SECONDS,
            taskModel:          type[Task] = Task,
            extraIndexes:       Sequence[IndexModel] | None = None
        ):

        self.pollInterval = pollInterval
        self.leaseSeconds = leaseSeconds
        self.defaultFactory: TaskFactory = defaultFactory or app.defaultFactory
        self.distributionEngine: DistributionEngine = app.distribution

        declared = type(app)._getTasks()

        # resolve tasks
        self.functions: dict[str, Callable] = {
            name: getattr(app, name)
            for name in declared
        }

        # each task's limits: what its @task declares, and the app's defaults for the rest
        defaults = {
            "timeout": app.taskTimeout, "skipAfter": app.taskSkipAfter,
            "maxAttempts": app.taskMaxAttempts, "retryDelay": app.taskRetryDelay,
        }
        TaskLimits(**defaults)      # checked even when no task uses them, so a bad default can't lie in wait

        self.limits: dict[str, TaskLimits] = {
            name: TaskLimits(**{**defaults, **declaration.limits})
            for name, declaration in declared.items()
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

    def init(self):
        """Startup housekeeping. Only ever run by a process that starts workers —
        never on construction, where it would disturb whatever else is running."""

        self.tasksCollection.update_many(
            {"status": "pending", "work.functionName": {"$nin": list(self.functions)}},
            {"$set": {"status": "incompatible", "error": "Function for this task does not exist in this app"}},
        )  # flag pending tasks that can no longer be executed

    def createIndexes(self):
        super().createIndexes()  # unique uid
        self.collection.create_index([("status", 1), ("leaseUntil", 1)])  # _work() claim

    def _prepare(self, document: Task) -> Task:
        """A task waiting for its first run is claimable at its deadline, so the
        lease moves with a deadline changed by save() or update(). One waiting for
        a retry keeps its retry time, and a claimed one its lease."""

        if document.status == "pending" and document.attempts == 0:
            document.leaseUntil = document.deadline

        return document

    def _call(self, work: CallSpec) -> Any:
        """Run a call in this thread, its arguments validated into the types the
        function declares."""

        if work.functionName not in self.functions:
            raise TaskNotFound(f"Task {work.functionName} does not exist in this app")

        return callValidated(self.functions[work.functionName], self.validators[work.functionName], work.kwargs)

    def _work(self):
        now = utc_now()
        raw = self.tasksCollection.find_one_and_update(
            {
                # due, or claimed by a worker that is no longer renewing its lease
                "status": {"$in": ["pending", "processing"]},
                "leaseUntil": {"$lte": now},
                "work.functionName": {"$in": list(self.functions)},  # never claim what we cannot run
            },
            {
                "$set": {"status": "processing", "leaseUntil": now + timedelta(seconds=self.leaseSeconds)},
                "$inc": {"attempts": 1},
            },
            sort=[("leaseUntil", 1)],
            return_document=ReturnDocument.AFTER
        )

        if not raw:
            return

        task = self.load(raw)

        if self._tooLate(task, now):
            return self._outdate(task, now)

        # A failure is retried by writing it back as pending, so running out of
        # attempts at the claim means the last one never reported back at all:
        # its worker died. Running it again is how a task kills every worker.
        limits = self.limitsFor(task)

        if task.attempts > limits.maxAttempts:
            return self._giveUp(task)

        self._hold(task.uid)
        try:
            task = self.execute(task)
        finally:
            self._releaseHold(task.uid)

        if task.status == "failed" and task.attempts < limits.maxAttempts:
            logger.warning(
                "%r failed on attempt %d of %d, retrying in %ss",
                task, task.attempts, limits.maxAttempts, limits.retryDelay
            )
            task.status = "pending"
            task.leaseUntil = utc_now() + timedelta(seconds=limits.retryDelay)

        try:
            bson.encode(task.model_dump())
        except Exception:  # a result the driver cannot encode: checked before writing, not after a half-write
            task.status = "failed"
            task.result = None
            task.error = traceback.format_exc()

        if not self._record(task):
            logger.warning(
                "%r finished after its claim was lost — canceled, or taken over by another "
                "worker when its lease lapsed; this outcome was not recorded", task
            )

        return task     # truthy, so the worker loop knows not to sleep

    def _record(self, task: Task) -> bool:
        """Write a claimed task back, but only while this claim still holds it.

        Every claim bumps `attempts`, so it identifies the claim: a worker whose
        lease lapsed cannot overwrite a cancel, or the worker that took over.
        """

        return self.tasksCollection.update_one(
            {"uid": task.uid, "status": "processing", "attempts": task.attempts},
            {"$set": task.model_dump()}
        ).matched_count > 0

    def limitsFor(self, task: Task) -> TaskLimits:
        """The limits a task runs under: those its function declares on @task."""

        try:
            return self.limits[task.work.functionName]
        except KeyError:
            raise TaskNotFound(f"Task {task.work.functionName} does not exist in this app") from None

    def _giveUp(self, task: Task) -> Task:
        task.status = "failed"
        task.error = (
            f"gave up after {task.attempts - 1} attempt(s): the worker running the last "
            f"one died without reporting back — killed, or taken down by the task itself"
        )

        self._record(task)
        logger.error("%r %s", task, task.error)

        return task

    def _tooLate(self, task: Task, now: datetime) -> bool:
        """Whether this task went stale waiting to be claimed — after downtime, or
        in a queue nobody is keeping up with; it is the same condition."""

        limit = self.limitsFor(task).skipAfter

        return limit is not None and (now - task.deadline).total_seconds() > limit

    def _outdate(self, task: Task, now: datetime) -> Task:
        late = (now - task.deadline).total_seconds()

        task.status = "outdated"
        task.error = (
            f"claimed {late:.0f}s after its deadline, past the "
            f"{self.limitsFor(task).skipAfter}s it was worth running for"
        )

        self._record(task)
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
                outcome["result"] = self._call(task.work)
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
        timeout = self.limitsFor(task).timeout
        start = time.perf_counter()

        try:
            task.result = (
                self._call(task.work) if timeout is None
                else self._callWithTimeout(task, timeout)
            )
            task.status = "success"
            task.error = None       # from an earlier attempt
        except TaskTimeout as e:
            task.status = "timeout"
            task.error = str(e)
            logger.warning("%r timed out after %ss", task, timeout)
        except (Exception, SystemExit):     # sys.exit() in a task would otherwise end the worker thread
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

    def _add(self, work: CallSpec, deadline: datetime, factory: TaskFactory) -> Task:
        return self.insert(factory._emit(work=work, deadline=deadline))

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

    def wait(self, task: Task | str, timeout: float | None = None, interval: float = 0.1) -> Task:
        """Block until a task has finished, and return it as it finished.

        Reads the document, so it works from any process — the one that queued
        the task need not be the one running it. A task waiting to be retried has
        not finished. Raises TimeoutError if `timeout` runs out first, and
        TaskNotFound if there is no such task.
        """

        uid = task if isinstance(task, str) else task.uid
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            current = self.get(uid)

            if current is None:
                raise TaskNotFound(f"there is no task {uid}")

            if current.status in FINAL_TASK_STATUSES:
                return current

            if deadline is None:
                time.sleep(interval)
                continue

            remaining = deadline - time.monotonic()

            if remaining <= 0:
                raise TimeoutError(f"{current!r} is still {current.status} after {timeout}s")

            time.sleep(min(interval, remaining))

    def schedule(
            self,
            work:           CallSpec,
            deadline:       datetime | None = None,
            factory:        TaskFactory | None = None
        ) -> Task:

        """Queue one call. It runs under the limits its task declares."""

        self.validate(work)

        return self._add(work=work, deadline=deadline or utc_now(), factory=factory or self.defaultFactory)

    def scheduleFromDistribution(
            self,
            work:           CallSpec,
            distribution:   CallSpec,
            factory:        TaskFactory | None = None
        ) -> Task:

        self._app.distribution.validate(distribution)
        deadline = utc_now() + self._app.distribution.gen(distribution)

        return self.schedule(work=work, deadline=deadline, factory=factory)

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
        return super()._emit(self.emitWork(), deadline)
    
    def __repr__(self) -> str:
        return f"Scheduler {self.name}: {self.work!r}"

class SchedulerEngine(CollectionEngine, WorkerLoop):
    workerLabel = "scheduler"

    def __init__(
            self, 
            app:                  BaseApp,
            schedulersCollection:   Collection | str | None = None,
            taskEngine:             TaskEngine | None = None,
            pollInterval:           float = 1,
            missed:                 MISSED_BEATS = "once",
            schedulerModel:         type[Scheduler] = Scheduler,
            extraIndexes:           Sequence[IndexModel] | None = None,
            leaseSeconds:           float = LEASE_SECONDS,
            name:                   str = "scheduler"
        ):

        self.pollInterval = pollInterval
        self.leaseSeconds = leaseSeconds
        self.taskEngine: TaskEngine = taskEngine or app.task
        self.missed: MISSED_BEATS = checkMissed(missed)

        self._initWorkers()

        super().__init__(
            app,
            name=name,          # the attribute it is declared as, so engines tell apart
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
        # served. Which is the only moment `missed` has anything to say.
        behind = scheduler.deadline + interval <= now
        replay = self.missed == "replay"

        # Not held for renewal: emitting takes milliseconds against a lease of
        # minutes, and a renewal landing after the deadline write below would push
        # the next beat out by a whole lease. A stall that outlasts the lease is
        # covered by the beat's fixed uid.
        if scheduler.status == "enabled" and not (behind and self.missed == "skip"):
            task = scheduler._emit(deadline=scheduler.deadline)
            task.uid = beatUid(scheduler.uid, scheduler.deadline)

            if task.work.functionName not in self.taskEngine.functions:
                # skipped, not disabled: disabling is the operator's word, and
                # would outlast the task coming back
                logger.warning(
                    "%r emits %s, which this app has no task for; skipped this beat",
                    scheduler, task.work.functionName
                )
            else:
                try:
                    self.taskEngine.insert(task)
                except DuplicateKeyError:
                    pass    # emitted by a worker that died before moving the deadline on

        # Only "replay" walks the backlog beat by beat. The others
        # resume from now, so time spent behind is not time owed.
        deadline = scheduler.deadline + interval if replay or not behind else now + interval

        self.schedulersCollection.update_one(
            # only if nobody moved the deadline meanwhile: ensure() or update()
            # restarting the rhythm mid-claim wrote its own, lease included
            {"uid": scheduler.uid, "deadline": scheduler.deadline},
            {"$set": self._deadline(deadline)}
        )

        # a disabled scheduler still walks its deadline, so its distribution keeps
        # its shape for when it is enabled again — but it owes nothing, so it
        # has nothing to warn about
        if behind and scheduler.status == "enabled":
            logger.warning(
                "%r missed a beat of %s (missed=%r)", scheduler, interval, self.missed
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

    def byName(self, name: str) -> Scheduler | None:
        """The scheduler declared under this name by ensure()."""

        return self.get(schedulerUid(name))

    def _prepare(self, document: Scheduler) -> Scheduler:
        """The lease moves with the deadline, or a deadline changed by hand would
        still fire at the old time."""

        document.leaseUntil = document.deadline

        return document

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

        existing = self.get(uid)

        if existing is None:
            return None

        changes: dict[str, Any] = dict(fields)

        if work is not None:
            changes["work"] = work

        if distribution is not None:
            changes["distribution"] = distribution

        merged = self._assign(existing.model_copy(), changes)   # raises before writing anything
        self.validateScheduler(merged)

        dumped = merged.model_dump()
        update: dict[str, Any] = {key: dumped[key] for key in changes if key in dumped}

        if "deadline" in update:
            update.update(self._deadline(update["deadline"]))

        if distribution is not None:
            update.update(self._deadline(utc_now() + self.taskEngine.distributionEngine.gen(distribution)))

        if enabled is not None:
            update["status"] = "enabled" if enabled else "disabled"

        if update:
            self.schedulersCollection.update_one(
                {"uid": uid},
                {"$set": update}
            )

        return self.get(uid)

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

        merged = self._assign(self.schedulerModel.model_validate(existing), changes)
        self.validateScheduler(merged)

        dumped = merged.model_dump()
        update: dict[str, Any] = {key: dumped[key] for key in changes if key in dumped}

        if "deadline" in update:
            update.update(self._deadline(update["deadline"]))

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


class Item(Document, Generic[P]):
    status:         ITEM_STATUS         = "pending"
    data:           P

    createdAt:      datetime            = Field(default_factory=utc_now)
    claimedAt:      datetime | None     = None
    finishedAt:     datetime | None     = None
    leaseUntil:     datetime | None     = None
    attempts:       int                 = 0
    claimId:        str | None          = None   # which claim holds it; a new one on every claim

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
            maxAttempts:        int | None = None,
            retryDelay:         float | None = None,
            leaseSeconds:       float = LEASE_SECONDS
        ):

        self.leaseSeconds = leaseSeconds
        self.payload: type[BaseModel] | None = payload
        self.maxAttempts: int = maxAttempts if maxAttempts is not None else app.itemMaxAttempts
        self.retryDelay: float = retryDelay if retryDelay is not None else app.itemRetryDelay

        if self.maxAttempts < 1:
            raise ValueError(f"maxAttempts must be at least 1, not {self.maxAttempts}")

        if self.retryDelay < 0:
            raise ValueError(f"retryDelay is seconds and cannot be negative, not {self.retryDelay}")

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
        """Atomically take the oldest claimable item, or None if there is none.

        Claimable is pending and due, or claimed by a worker that stopped renewing.
        The claim is a single find_one_and_update, so two workers racing on the
        same pile can never receive the same item.
        """

        query: dict[str, Any] = {"status": {"$in": ["pending", "claimed"]}}
        if where:
            query.update(where)

        while True:
            now = utc_now()
            raw = self.collection.find_one_and_update(
                {**query, "leaseUntil": {"$lte": now}},
                {
                    "$set": {
                        "status": "claimed",
                        "claimId": uuid4str(),
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

            item = self.load(raw)

            # A failure with attempts left goes back as pending, so running out at
            # the claim means the last one never reported back: its worker died.
            if item.attempts <= self.maxAttempts:
                return item

            self._giveUp(item)      # and look for the next one

    def _giveUp(self, item: Item):
        error = (
            f"gave up after {item.attempts - 1} attempt(s): the worker holding the last "
            f"one died without reporting back — killed, or taken down by the item itself"
        )

        self._finish(item, "failed", error=error)
        logger.error("%s item %s %s", self.name, item.uid, error)

    @staticmethod
    def _uid(item: Item | str) -> str:
        return item if isinstance(item, str) else item.uid

    @staticmethod
    def _matching(item: Item | str) -> dict[str, Any]:
        """Which document an action on `item` may touch.

        An Item stands for the claim it came from, so a worker whose lease lapsed
        cannot finish work another worker has since taken over. A bare uid is an
        operator acting on the item whatever state it is in.
        """

        if isinstance(item, str):
            return {"uid": item}

        if item.claimId is None:
            raise ValueError(
                f"item {item.uid} was not handed out by claim(), so it stands for no claim — "
                f"claim it first, or pass its uid to act on it by hand"
            )

        return {"uid": item.uid, "claimId": item.claimId}

    def _finish(
            self,
            item:       Item | str,
            status:     ITEM_STATUS,
            result:     Any = None,
            error:      str | None = None
        ) -> bool:

        update: dict[str, Any] = {"status": status, "finishedAt": utc_now(), "claimId": None}

        if status == "done":
            update["error"] = None      # from an earlier attempt, as a task clears it

        if result is not None:
            update["result"] = result

        if error is not None:
            update["error"] = error

        # matched, not modified: the question is whether there is such an item,
        # not whether the bytes happened to change
        return self.collection.update_one(
            self._matching(item),
            {"$set": update}
        ).matched_count > 0

    def done(self, item: Item | str, result: Any = None) -> bool:
        """Mark an item finished. False if there is no such item."""

        return self._finish(item, "done", result=result)

    def fail(self, item: Item | str, error: str | None = None) -> bool:
        """Record a failure. False if nothing matched, as for done().

        A claimed Item with attempts left goes back on the pile, claimable after
        retryDelay — the same rule as a task. A bare uid is an operator's verdict
        and is final.
        """

        if isinstance(item, str) or item.attempts >= self.maxAttempts:
            return self._finish(item, "failed", error=error)

        retried = self.collection.update_one(
            self._matching(item),
            {"$set": {
                "status": "pending",
                "error": error,
                "claimId": None,
                "claimedAt": None,
                "leaseUntil": utc_now() + timedelta(seconds=self.retryDelay),
            }}
        ).matched_count > 0

        if retried:
            logger.warning(
                "%s item %s failed on attempt %d of %d, retrying in %ss",
                self.name, item.uid, item.attempts, self.maxAttempts, self.retryDelay
            )

        return retried

    def release(self, item: Item | str) -> bool:
        """Put a claimed item back on the pile without consuming an outcome, or
        an attempt."""

        return self.collection.update_one(
            {**self._matching(item), "status": "claimed"},
            # back to its own place in the pile, not the end of it
            [{"$set": {
                "status": "pending",
                "claimId": None,
                "claimedAt": None,
                "leaseUntil": "$createdAt",
                "attempts": {"$subtract": ["$attempts", 1]},
            }}]
        ).matched_count > 0

    def renewLease(self, item: Item | str) -> bool:
        """Hold on to an item for another lease period."""

        return self.collection.update_one(
            self._matching(item),
            {"$set": {"leaseUntil": utc_now() + timedelta(seconds=self.leaseSeconds)}}
        ).matched_count > 0

    def renewLeases(self) -> int:
        """Push back the lease on every item this process is still holding."""

        with self._heldLock:
            claims = list(self._held)

        if not claims:
            return 0

        return self.collection.update_many(
            {"claimId": {"$in": claims}},   # not one another worker has taken since
            {"$set": {"leaseUntil": utc_now() + timedelta(seconds=self.leaseSeconds)}}
        ).modified_count

    def _renewLoop(self):
        """One loop for the whole pile, started on the first claim it has to hold."""

        while True:
            time.sleep(max(1.0, self.leaseSeconds / 3))

            # one lock for the check and the exit, or a claim held in between would
            # find a renewer still set, start none, and never be renewed
            with self._heldLock:
                if not self._held:
                    self._renewer = None
                    return

            try:
                self.renewLeases()
            except Exception:
                logger.exception("pile lease renewal failed")

    def _hold(self, item: Item):
        with self._heldLock:
            self._held.add(item.claimId)

            if self._renewer is None:
                self._renewer = threading.Thread(
                    target=self._renewLoop,
                    name=f"pymonque-pile-{self.name}-lease",
                    daemon=True
                )
                self._renewer.start()

    def _releaseHold(self, item: Item):
        with self._heldLock:
            self._held.discard(item.claimId)

    @contextmanager
    def work(self, where: Mapping[str, Any] | None = None):
        """Claim one item, mark it done on success, and fail() it on exception —
        which retries it if it has attempts left.

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
        except (Exception, SystemExit):     # as execute() does: sys.exit() is a failure too
            if not self.fail(item, traceback.format_exc()):
                self._lostClaim(item)
            raise
        else:
            if not self.done(item):
                self._lostClaim(item)
        finally:
            self._releaseHold(item)

    def _lostClaim(self, item: Item):
        logger.warning(
            "%s item %s finished after its claim passed to another worker; "
            "this outcome was not recorded", self.name, item.uid
        )

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
            maxAttempts:        int | None = None,
            retryDelay:         float | None = None,
            leaseSeconds:       float | None = None
        ):

        self.__is_pile__: bool = True
        self.payload = payload
        self.itemsCollection = itemsCollection
        self.maxAttempts = maxAttempts      # None: the app's itemMaxAttempts
        self.retryDelay = retryDelay        # None: the app's itemRetryDelay
        self.leaseSeconds = leaseSeconds
        self.name: str = ""

    def __set_name__(self, owner, name: str):
        self.name = name

    def _engine(self, app: BaseApp, leaseSeconds: float) -> PileEngine:
        return PileEngine(
            app,
            name=self.name,
            payload=self.payload,
            itemsCollection=self.itemsCollection,
            maxAttempts=self.maxAttempts,
            retryDelay=self.retryDelay,
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
            missed:                 MISSED_BEATS | None = None,
            pollInterval:           float | None = None,
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
        self.missed = checkMissed(missed) if missed is not None else None    # None: the app's schedulerMissed
        self.pollInterval = pollInterval
        self.extraIndexes = extraIndexes
        self.name: str = ""

    def __set_name__(self, owner, name: str):
        self.name = name

    def _engine(
            self,
            app:          BaseApp,
            missed:         MISSED_BEATS,
            pollInterval:   float,
            leaseSeconds:   float
        ) -> SchedulerEngine:

        collection = self.schedulersCollection
        if collection is None:
            collection = "pymonque_schedulers" if self.name == "scheduler" else f"pymonque_schedulers_{self.name}"

        return SchedulerEngine(
            app,
            schedulersCollection=collection,
            pollInterval=self.pollInterval if self.pollInterval is not None else pollInterval,
            missed=self.missed or missed,
            schedulerModel=self.schedulerModel,
            extraIndexes=self.extraIndexes,
            leaseSeconds=self.leaseSeconds if self.leaseSeconds is not None else leaseSeconds,
            name=self.name
        )

    def __get__(self, obj, objtype=None) -> SchedulerEngine | schedulers:
        if obj is None:
            return self

        return obj.schedulerEngines[self.name]

    def __repr__(self) -> str:
        return f"schedulers {self.name} ({self.schedulerModel.__name__})"


class task:
    """Declare a method as a task, bare or with its limits:

        @task
        @staticmethod
        def ping(): ...

        @task(timeout=60, maxAttempts=3, retryDelay=30)
        def sync(self, accountId: int): ...

    A limit left out takes the app's default (taskTimeout, taskSkipAfter,
    taskMaxAttempts, taskRetryDelay); a timeout or skipAfter given as None means
    no limit, whatever the default. Limits belong to the function rather than to
    one call of it, so this is the only place they are set.
    """

    def __init__(
            self,
            func:           Callable | None = None,
            *,
            timeout:        float | None | _Unset = UNSET,
            skipAfter:      float | None | _Unset = UNSET,
            maxAttempts:    int | _Unset = UNSET,
            retryDelay:     float | _Unset = UNSET
        ):

        given = {"timeout": timeout, "skipAfter": skipAfter, "maxAttempts": maxAttempts, "retryDelay": retryDelay}
        self.limits: dict[str, Any] = {name: value for name, value in given.items() if value is not UNSET}

        TaskLimits(**self.limits)       # a bad limit fails where it is written

        self.func: Callable | None = None

        if func is not None:
            self._declare(func)

    def _declare(self, func: Callable):
        self.__is_staticmethod__: bool = isinstance(func, staticmethod)
        self.__is_classmethod__: bool = isinstance(func, classmethod)
        self.func = func.__func__ if isinstance(func, (staticmethod, classmethod)) else func
        self.__is_task__: bool = True

    def __call__(self, func: Callable) -> task:
        """The @task(...) form: the limits came first, and the function comes here."""

        if self.func is not None:
            raise TypeError(f"{self.func.__name__} is already declared as a task")

        self._declare(func)

        return self

    def __get__(self, obj, objtype=None) -> Callable:
        if obj is None:
            return FuncSpec(self.func)

        if self.__is_staticmethod__:
            return self.func  # nothing to bind

        if self.__is_classmethod__:
            return MethodType(self.func, type(obj))

        return MethodType(self.func, obj)
    
class BaseApp:
    @classmethod
    def _getDeclared(cls, flag: str) -> dict[str, Any]:
        return {
            name: obj
            for name, obj in nearestAttributes(cls).items()
            if getattr(obj, flag, False)
        }

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

    # set by __init__, so a declaration under one of these names would be shadowed
    _INSTANCE_ATTRIBUTES = frozenset({
        "db", "task", "scheduler", "distribution", "piles", "collections", "schedulerEngines",
        "defaultFactory", "leaseSeconds", "enforceVersion", "heartbeatInterval",
        "workerStaleAfter", "workerUid", "backlogWarnAfter", "backlogInterval",
        "tasksCollection", "schedulersCollection", "workersCollection",
    })

    def __init_subclass__(cls, **kwargs):
        """Refuse a declaration that would replace part of the app itself: a pile
        named `backlog` would break the backlog check, a task named `init` the
        startup. Declaring schedulers named `scheduler` is the one sanctioned
        replacement."""

        super().__init_subclass__(**kwargs)

        reserved = set(dir(BaseApp)) | BaseApp._INSTANCE_ATTRIBUTES

        for name, obj in cls.__dict__.items():
            declared = any(getattr(obj, flag, False) for flag in
                           ("__is_task__", "__is_pile__", "__is_collection__", "__is_schedulers__"))

            if not declared or name not in reserved:
                continue

            if name == "scheduler" and getattr(obj, "__is_schedulers__", False):
                continue

            raise TypeError(
                f"{cls.__name__}.{name} would replace BaseApp.{name}; declare it under another name"
            )

    # Defaults, declared in code like everything they apply to: every process that
    # imports this app must agree on them, and they are part of the fingerprint so
    # two that disagree cannot both run workers.

    # What a scheduler owes for beats that went by unworked. schedulers(missed=...)
    # sets it for one engine.
    schedulerMissed:            MISSED_BEATS              = "once"

    # Limits for every task, each overridable on its own @task(...). A timeout or
    # skipAfter of None means no limit. One attempt by default: rerunning a side
    # effect nobody asked to rerun is worse than a failure you can see. A crash
    # counts as an attempt, so a task that takes its worker down with it is given
    # up on, not rerun forever.
    taskTimeout:                float | None              = None
    taskSkipAfter:              float | None              = None
    taskMaxAttempts:            int                       = MAX_ATTEMPTS
    taskRetryDelay:             float                     = RETRY_DELAY

    # The same rule for pile items, overridable on each pile(...).
    itemMaxAttempts:            int                       = MAX_ATTEMPTS
    itemRetryDelay:             float                     = RETRY_DELAY

    def __init__(
            self, 
            db:                    Database, 
            distributionsRegistry:      type[BaseDistributions] = BaseDistributions,
            taskPollInterval:           float = 1,
            schedulerPollInterval:      float = 1,
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
        self._buildEngines(taskPollInterval, schedulerPollInterval)

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
            name: spec._engine(self, self.leaseSeconds)
            for name, spec in type(self)._getPiles().items()
        }

    def _buildEngines(self, taskPollInterval: float, schedulerPollInterval: float):

        self.task: TaskEngine = TaskEngine(
            self, pollInterval=taskPollInterval, leaseSeconds=self.leaseSeconds
        )

        self.schedulerEngines: dict[str, SchedulerEngine] = {
            name: spec._engine(self, self.schedulerMissed, schedulerPollInterval, self.leaseSeconds)
            for name, spec in type(self)._getSchedulerEngines().items()
        }

        # a declaration named `scheduler` replaces the default engine
        if "scheduler" not in self.schedulerEngines:
            self.scheduler: SchedulerEngine = SchedulerEngine(
                self, pollInterval=schedulerPollInterval, missed=self.schedulerMissed,
                leaseSeconds=self.leaseSeconds
            )
            self.schedulerEngines["scheduler"] = self.scheduler

    # --- one version of the code at a time ---

    @property
    def fingerprint(self) -> str:
        """Identifies the executable surface of this app: its task and distribution
        names with their signatures, the limits every task and pile runs under, and
        what each scheduler engine owes for missed beats.

        Two processes that disagree on this are running different code and must
        not work the same collections. It cannot see a changed function *body* —
        nothing can, reliably — so this is a guard, not a proof. Deploy one
        version at a time.

        Limits and missed-beats rules are in here because they are shared
        behaviour: one process retrying a failure another treats as final is a
        split brain over the same documents, not two harmless local settings.
        """

        parts = [
            # a default like object() prints its address, which differs per process
            re.sub(r" at 0x[0-9a-fA-F]+", "", f"{name}{inspect.signature(func)}")
            for registry in (self.task.functions, self.distribution.functions)
            for name, func in sorted(registry.items())
        ]

        parts += [f"{name} {limits.model_dump()}" for name, limits in sorted(self.task.limits.items())]
        parts += [f"{engine.name}:missed={engine.missed}" for engine in self.schedulerEngines.values()]
        parts += [
            f"{p.name}:maxAttempts={p.maxAttempts}:retryDelay={p.retryDelay}"
            for p in self.piles.values()
        ]

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

    def taskWorkers(self) -> int:
        """Task workers across every process that has checked in.

        A scheduler-only process runs none of its own, so counting locally would
        cry wolf at a perfectly good deployment.
        """

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

    def _register(self):
        """Check this process in, so others can count its workers and check their
        version against it. Every worker process does, whether or not it enforces."""

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
        """Startup housekeeping: flag pending tasks whose function no longer
        exists on this app.

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

        self.init()             # refuses here if a live worker is running other code
        self._register()
        self._monitor("heartbeat", self._heartbeat)

        self.task.startWorkers(taskWorkers or 0)

        for engine in self.schedulerEngines.values():
            engine.startWorkers(schedulerWorkers or 0)

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
        """Stop claiming new work, without waiting for what is in flight.

        The heartbeat keeps going: work still finishing belongs to a live worker,
        and the version check must go on seeing it. stopWorkers() ends it.
        """

        self._stopping = True

        for engine in self.engines:
            engine._stop.set()

    def stopWorkers(self, timeout: float | None = 30) -> bool:
        """Stop claiming, let the work already claimed finish, then return.

        Returns False if anything was still busy when `timeout` ran out — the
        threads are daemons, so leaving the process at that point abandons them,
        as a kill would: their leases lapse and the next worker deals with them.
        """

        self.requestStop()

        deadline = None if timeout is None else time.monotonic() + timeout
        drained = True

        for engine in self.engines:
            remaining = None if deadline is None else max(0, deadline - time.monotonic())

            if not engine.stopWorkers(remaining):
                drained = False

        self._quit.set()

        # joined, so a startWorkers() straight after finds them gone and starts
        # fresh ones, rather than trusting a heartbeat that is on its way out
        for thread in self._monitors.values():
            thread.join(1)

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

    def handleSignals(self, signals: Sequence[int] = (signal.SIGINT, signal.SIGTERM)):
        """Turn SIGINT/SIGTERM into a graceful shutdown.

        The first signal stops claiming and lets in-flight work finish; a second
        one exits immediately. Opt-in, because a host framework may want to own
        these — call it only from a process pymonque is running.

        SIGKILL cannot be caught: the process dies with work in flight, and its
        leases lapse for the next worker to deal with.
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
            # a second call must not record our own handler as the one to restore
            self._previousHandlers.setdefault(s, signal.getsignal(s))
            signal.signal(s, onSignal)

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

        self.handleSignals()
        self.startWorkers(taskWorkers, schedulerWorkers)

        try:
            self.joinWorkers()
        finally:
            drained = self.stopWorkers(timeout)
            self.restoreSignals()

        return drained
