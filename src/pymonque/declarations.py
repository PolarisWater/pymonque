"""Declarations: what an app class declares, checked where it is written.

    class App(BaseApp):
        groups  = collection(Group, key="groupId")
        heavy   = tasks(RenderTask, leaseSeconds=900)
        nightly = schedulers(emitsInto=heavy, missed="skip")
        outbox  = pile(Email, maxAttempts=3)

        @task(timeout=60)
        def sync(self, accountId: int): ...

Every storage declaration takes the same leading arguments — the model, then the collection — and
after them, by keyword, only what its kind needs. A bad value raises pydantic's ValidationError
naming the setting; a setting that does not exist raises TypeError, and one that was removed says
what became of it. Leaving a setting out is the only way to take the app's default.
"""

from __future__ import annotations

import functools
import inspect
from types import MethodType
from typing import Annotated, Any, Callable, ClassVar, Sequence, TypeVar

from pydantic import AfterValidator, BaseModel, ConfigDict, InstanceOf, ValidationError, validate_call
from pydantic_core import InitErrorDetails, PydanticCustomError
from pymongo import IndexModel
from pymongo.collection import Collection
from pymongo.database import Database

from .calls import FuncSpec, nearestAttributes, refusePositional
from .documents import Document
from .schedulers import Scheduler
from .settings import (
    UNSET, AppDefaults, MaxAttempts, Missed, PileSettings, SchedulerEngineSettings, Seconds, SkipAfter,
    TaskEngineSettings, TaskLimits, Timeout, orDefault,
)
from .tasks import Task


D = TypeVar("D")

# a Collection or an IndexModel has no schema of its own, so it is checked as an instance
DECLARATION = ConfigDict(arbitrary_types_allowed=True)


def checked(init: Callable) -> Callable:
    """Check a declaration's settings where it is written.

    Arguments are bound to their names first, so an error names the setting even when it was passed
    by position — `collection(Group)` is the usual spelling, and "1" would tell nobody which argument
    was wrong. A setting that was removed says what became of it.
    """

    validated = validate_call(init, config=DECLARATION)
    signature = inspect.signature(init)

    @functools.wraps(init)
    def wrapper(self, *args, **kwargs):
        retired = type(self).retired

        for name in kwargs:
            if name in retired:
                raise TypeError(f"{type(self).__name__}({name}=...) is not a setting: {retired[name]}")

        arguments = signature.bind(self, *args, **kwargs).arguments    # TypeError, naming an unknown one
        arguments.pop("self")

        return validated(self, **arguments)

    return wrapper


def invalidSetting(declaration: object, setting: str, value: Any, message: str) -> ValidationError:
    """A ValidationError for a check pydantic cannot make on one setting alone, shaped like its own."""

    return ValidationError.from_exception_data(
        f"{type(declaration).__name__}.__init__",
        [InitErrorDetails(
            type=PydanticCustomError("invalid_setting", "{message}", {"message": message}),
            loc=(setting,),
            input=value,
        )],
    )


def _collection(value: Any) -> Any:
    if value is UNSET:
        return value

    if value is None:
        raise ValueError("leave it out for the default name; None is not a collection")

    if isinstance(value, str):
        if not value:
            raise ValueError("a collection name cannot be empty")

        return value

    if hasattr(value, "name") and hasattr(value, "find_one_and_update"):
        return value    # a Collection, or anything that behaves like one

    raise ValueError(f"expected a collection or the name of one, not {type(value).__name__}")


CollectionRef = Annotated[Any, AfterValidator(_collection)]
Indexes = Sequence[InstanceOf[IndexModel]] | None


class Declaration:
    """One part of an app, declared as an attribute of its class. Reached through the class, it is
    the declaration itself."""

    kind: ClassVar[str]
    retired: ClassVar[dict[str, str]] = {}
    misuse: ClassVar[str] = ""     # said when a declaration is used as a decorator

    model: type[BaseModel] | None
    collection: Any
    name: str = ""
    owner: type | None = None

    def __set_name__(self, owner: type, name: str):
        self.owner = owner
        self.name = name

    def __call__(self, *args: Any, **kwargs: Any):
        raise TypeError(f"{self.kind}(...) is a declaration, not a decorator{self.misuse}")

    def defaultCollectionName(self) -> str:
        raise NotImplementedError

    def collectionIn(self, db: Database) -> Collection:
        """The collection this declaration stores in: the one it was given, the one it names, or its
        default."""

        if isinstance(self.collection, str):
            return db[self.collection]

        if self.collection is not UNSET:
            return self.collection

        if not self.name:
            raise TypeError(f"{self!r} is not declared on a class, so it has no default collection")

        return db[self.defaultCollectionName()]

    def __repr__(self) -> str:
        model = self.model.__name__ if self.model is not None else "dict"

        return f"{self.kind} {self.name} ({model})" if self.name else f"{self.kind} ({model})"


class collection(Declaration):
    """A typed collection of documents.

        groups   = collection(Group)                        # -> the "groups" collection
        accounts = collection(Account, key="accountId")     # keyed by a field of your own
        legacy   = collection(Group, "old_groups")          # an existing collection, by name or object
    """

    kind = "collection"

    @checked
    def __init__(
            self,
            model:          type[Document],     # a stored model needs a uid: subclass Document
            collection:     CollectionRef = UNSET,
            *,
            key:            str = "uid",
            extraIndexes:   Indexes = None
        ):

        if key not in model.model_fields:
            raise invalidSetting(self, "key", key, f"{model.__name__} has no field {key!r}")

        self.model = model
        self.collection = collection
        self.key = key
        self.extraIndexes: list[IndexModel] = list(extraIndexes or ())

    def defaultCollectionName(self) -> str:
        return self.name    # your own documents, in your own namespace


class pile(Declaration):
    """Items waiting to be claimed, drained by tasks.

        outbox = pile(Email)                    # data validated against Email
        scraps = pile()                         # data is any dict
        tried  = pile(Email, maxAttempts=3)     # left out: the app's pileMaxAttempts
    """

    kind = "pile"
    retired = {
        "payload":          "the payload model is the first argument, `model`",
        "itemsCollection":  "renamed `collection`",
        "retryDelay":       "items have no retry delay: fail() is final, and release() hands an item back at once",
    }

    @checked
    def __init__(
            self,
            model:          type[BaseModel] | None = None,  # None: items carry any dict
            collection:     CollectionRef = UNSET,          # left out: pymonque_pile_<name>
            *,
            extraIndexes:   Indexes = None,
            maxAttempts:    MaxAttempts = UNSET,            # left out: the app's pileMaxAttempts
            leaseSeconds:   Seconds = UNSET                 # left out: the app's pileLeaseSeconds
        ):

        self.model = model
        self.collection = collection
        self.extraIndexes: list[IndexModel] = list(extraIndexes or ())
        self.maxAttempts = maxAttempts
        self.leaseSeconds = leaseSeconds

    def defaultCollectionName(self) -> str:
        return f"pymonque_pile_{self.name}"

    def settingsWith(self, defaults: AppDefaults) -> PileSettings:
        return PileSettings(
            maxAttempts=orDefault(self.maxAttempts, defaults.pileMaxAttempts),
            leaseSeconds=orDefault(self.leaseSeconds, defaults.pileLeaseSeconds),
        )


class tasks(Declaration):
    """A task engine: calls due at a time, run by worker threads. Any task engine runs any of the
    app's tasks.

        heavy = tasks(RenderTask, leaseSeconds=900)     # -> pymonque_task_heavy
        task  = tasks(AccountTask)                      # replaces the default engine, pymonque_task

    A declaration named `task` hides the @task decorator for the rest of the class body, so declare
    it below the tasks.
    """

    kind = "tasks"
    misuse = (
        " — `task = tasks(...)` hides the @task decorator for the rest of the class body, "
        "so declare it below the tasks"
    )
    retired = {
        "tasksCollection":  "renamed `collection`",
        "taskModel":        "the Task subclass is the first argument, `model`",
    }

    @checked
    def __init__(
            self,
            model:          type[Task] = Task,
            collection:     CollectionRef = UNSET,          # left out: pymonque_task_<name>
            *,
            extraIndexes:   Indexes = None,
            leaseSeconds:   Seconds = UNSET,                # left out: the app's taskLeaseSeconds
            pollInterval:   Seconds = UNSET                 # left out: the process's own
        ):

        self.model = model
        self.collection = collection
        self.extraIndexes: list[IndexModel] = list(extraIndexes or ())
        self.leaseSeconds = leaseSeconds
        self.pollInterval = pollInterval

    def defaultCollectionName(self) -> str:
        return "pymonque_task" if self.name == "task" else f"pymonque_task_{self.name}"

    def settingsWith(self, defaults: AppDefaults) -> TaskEngineSettings:
        return TaskEngineSettings(leaseSeconds=orDefault(self.leaseSeconds, defaults.taskLeaseSeconds))


def _taskEngine(value: Any) -> Any:
    if value is UNSET or isinstance(value, tasks):
        return value

    if value is None:
        raise ValueError("leave it out for the default task engine")

    if isinstance(value, str):
        raise ValueError(f"give the declaration itself, emitsInto={value}, not its name")

    raise ValueError(f"expected a tasks(...) declaration, not {value!r}")


TaskEngineRef = Annotated[Any, AfterValidator(_taskEngine)]


class schedulers(Declaration):
    """A scheduler engine: recurring schedules, each emitting a task on its rhythm.

        nightly = schedulers(emitsInto=heavy, missed="skip")    # -> pymonque_scheduler_nightly
        scheduler = schedulers(AccountScheduler)                # replaces the default engine

    `emitsInto` is the task engine it emits into, declared above it on the same app; left out, the
    default task engine.
    """

    kind = "schedulers"
    retired = {
        "schedulersCollection": "renamed `collection`",
        "schedulerModel":       "the Scheduler subclass is the first argument, `model`",
        "policy":               "renamed `missed`, with the values skip, once and replay",
        "taskEngine":           "renamed `emitsInto`, given the tasks(...) declaration",
    }

    @checked
    def __init__(
            self,
            model:          type[Scheduler] = Scheduler,
            collection:     CollectionRef = UNSET,          # left out: pymonque_scheduler_<name>
            *,
            extraIndexes:   Indexes = None,
            emitsInto:      TaskEngineRef = UNSET,          # left out: the default task engine
            missed:         Missed = UNSET,                 # left out: the app's schedulerMissed
            leaseSeconds:   Seconds = UNSET,                # left out: the app's schedulerLeaseSeconds
            pollInterval:   Seconds = UNSET                 # left out: the process's own
        ):

        self.model = model
        self.collection = collection
        self.extraIndexes: list[IndexModel] = list(extraIndexes or ())
        self.emitsInto = emitsInto
        self.missed = missed
        self.leaseSeconds = leaseSeconds
        self.pollInterval = pollInterval

    def __set_name__(self, owner: type, name: str):
        super().__set_name__(owner, name)

        target = self.emitsInto

        if target is UNSET:
            return

        # a reference is only written after the declaration it names, so one this class resolves
        # the name to was declared above — here or on a parent — and anything else belongs elsewhere
        if target.owner is None or nearestAttributes(owner).get(target.name) is not target:
            where = f"{target.owner.__name__}.{target.name}" if target.owner is not None else "an undeclared tasks(...)"

            raise invalidSetting(
                self, "emitsInto", target,
                f"{owner.__name__}.{name} emits into {where}, which is not a task engine declared above it on {owner.__name__}",
            )

    def defaultCollectionName(self) -> str:
        return "pymonque_scheduler" if self.name == "scheduler" else f"pymonque_scheduler_{self.name}"

    def settingsWith(self, defaults: AppDefaults) -> SchedulerEngineSettings:
        return SchedulerEngineSettings(
            missed=orDefault(self.missed, defaults.schedulerMissed),
            leaseSeconds=orDefault(self.leaseSeconds, defaults.schedulerLeaseSeconds),
        )


class task:
    """Declare a method as a task, bare or with its limits, above @staticmethod or @classmethod:

        @task
        @staticmethod
        def ping(): ...

        @task(timeout=60, skipAfter=None)
        def sync(self, accountId: int): ...

    A limit left out takes the app's default (taskTimeout, taskSkipAfter); None means no limit,
    whatever the default. Limits belong to the function rather than to one call of it, so this is the
    only place they are set.

    Reached through the class, a task builds the call that names it — App.sync(accountId=7). Reached
    through an app, it is the function itself, run now.
    """

    retired: ClassVar[dict[str, str]] = {
        "maxAttempts":  "tasks are not retried; a task that must succeed retries inside its own code",
        "retryDelay":   "tasks are not retried, so there is no retry delay",
    }

    @checked
    def __init__(
            self,
            func:       Any = None,
            *,
            timeout:    Timeout = UNSET,        # left out: the app's taskTimeout
            skipAfter:  SkipAfter = UNSET       # left out: the app's taskSkipAfter
        ):

        given = {"timeout": timeout, "skipAfter": skipAfter}

        self.limits: dict[str, Any] = {name: value for name, value in given.items() if value is not UNSET}
        self.func: Callable | None = None
        self.binding: str = ""
        self.name: str = ""
        self.owner: type | None = None

        if func is not None:
            self._declare(func)

    def _declare(self, func: Any):
        if isinstance(func, staticmethod):
            binding, function = "static", func.__func__
        elif isinstance(func, classmethod):
            binding, function = "class", func.__func__
        elif inspect.isfunction(func):
            binding, function = "instance", func
        else:
            hint = "; a task engine is declared with tasks(...)" if isinstance(func, type) else ""

            raise TypeError(f"@task declares a function, a staticmethod or a classmethod, not {func!r}{hint}")

        refusePositional(function, bound=binding != "static")

        self.binding = binding
        self.func = function

    def __call__(self, func: Any) -> task:
        """The @task(...) form: the limits came first, and the function comes here."""

        if self.func is not None:
            raise TypeError(f"{self.func.__name__} is already declared as a task")

        self._declare(func)

        return self

    def __set_name__(self, owner: type, name: str):
        if self.func is None:
            raise TypeError(f"{owner.__name__}.{name} = task(...) declares limits but no function; use it as a decorator")

        self.owner = owner
        self.name = name

    def __get__(self, obj: Any, objtype: type | None = None) -> Any:
        if self.func is None:
            return self

        if obj is None:
            return FuncSpec(self.name or self.func.__name__, self.func)

        if self.binding == "static":
            return self.func

        if self.binding == "class":
            return MethodType(self.func, type(obj))

        return MethodType(self.func, obj)

    def limitsWith(self, defaults: AppDefaults) -> TaskLimits:
        """The limits this task runs under: what it declares, and the app's defaults for the rest."""

        return TaskLimits(**{"timeout": defaults.taskTimeout, "skipAfter": defaults.taskSkipAfter, **self.limits})

    def __repr__(self) -> str:
        return f"task {self.name or getattr(self.func, '__name__', '')}".rstrip()


def declaredOn(cls: type, kind: type[D]) -> dict[str, D]:
    """Every declaration of a kind on a class and its parents, the nearest definition winning — so a
    subclass that redefines a name as anything else replaces the parent's declaration."""

    return {name: value for name, value in nearestAttributes(cls).items() if isinstance(value, kind)}
