"""Tasks: a call due at a time, run at most once — the Task model and the task engine."""

from __future__ import annotations

import logging
import time
import traceback
from datetime import datetime, timedelta
from typing import Any, Callable, Literal, Mapping, Self, Sequence, TypeVar

import bson
from pydantic import Field, model_validator
from pymongo import IndexModel
from pymongo.collection import Collection

from .calls import CallSpec, Functions
from .claims import Leases, claimNext, writeClaimed
from .distributions import DistributionEngine
from .documents import CollectionEngine, Document, Duration, UtcDatetime, WorkStatus, utc_now
from .exceptions import TaskNotFound, TaskValidationError
from .settings import TaskEngineSettings, TaskLimits


logger = logging.getLogger("pymonque")

# the statuses tasks share with pile items, and the ways a task ends without being run to its end
TaskStatus = Literal[WorkStatus, "timeout", "outdated", "incompatible"]

FINAL_TASK_STATUSES = frozenset({"done", "failed", "canceled", "timeout", "outdated", "incompatible"})


class TaskFactory(Document):
    """Who emitted a task — a scheduler, or a name such as "web-api" — so tasks can be found by source."""

    name: str

    def __repr__(self) -> str:
        return f"Factory {self.name}"


class Task(Document):
    """A call due at a time.

    Subclass it to store fields of your own on every task of an engine — context to query and index
    by, such as an account. The fields are data only; `runWork()` is where they reach the call.
    """

    status:         TaskStatus          = "pending"
    work:           CallSpec
    deadline:       UtcDatetime
    factory:        TaskFactory         # stored as a TaskFactory, so a scheduler's own fields stay off the task

    # the same three moments a pile item records
    createdAt:      UtcDatetime         = Field(default_factory=utc_now)
    claimedAt:      UtcDatetime | None  = None
    finishedAt:     UtcDatetime | None  = None      # set once the status is final

    # when this becomes claimable: its deadline while pending, the end of the holder's lease while
    # running. One field, so a claim is one comparison.
    leaseUntil:     UtcDatetime | None  = None
    claimId:        str | None          = None      # the claim holding it; only that claim writes an outcome

    executionTime:  Duration | None     = None
    result:         Any                 = None
    error:          str | None          = None

    @model_validator(mode="after")
    def defaultLease(self) -> Self:
        if self.leaseUntil is None:
            self.leaseUntil = self.deadline

        return self

    def runWork(self) -> CallSpec:
        """The call this task runs: its work, as stored.

        The one place context reaches a call. A subclass with fields of its own stamps them on here,
        and the call is checked as this returns it, both when the task is scheduled and when it runs:

            class AccountTask(Task):
                accountId: int

                def runWork(self) -> CallSpec:
                    return self.work.bind(accountId=self.accountId)
        """

        return self.work

    def __repr__(self) -> str:
        return f"Task {self.work!r} from {self.factory!r}"


T = TypeVar("T", bound=Task)

# what running a task writes back: its outcome, and nothing a user may have changed meanwhile
OUTCOME_FIELDS = frozenset({"status", "claimedAt", "finishedAt", "executionTime", "result", "error"})

# what schedule() refuses by name, rather than as a field the model lacks
LIMITS = frozenset({"timeout", "skipAfter"})

# the error of a task written off because its worker died; when is in its leaseUntil, which stays
WORKER_DIED = (
    "its worker stopped renewing the lease while running it — killed, or taken down by the task "
    "itself; not run again, since it may have done part of its work"
)


def taskFunctions(functions: Mapping[str, Callable]) -> Functions:
    """An app's tasks, as every task engine of the app runs them."""

    return Functions(functions, what="task", notFound=TaskNotFound, invalid=TaskValidationError)


class TaskEngine(CollectionEngine[T]):
    """Calls due at a time, stored in one collection and run by whoever claims them.

    Every task engine of an app runs any of the app's tasks: `functions` is the app's, and the engine
    only chooses where tasks wait. A task runs at most once — a failure is final, and a task whose
    worker died is written off rather than run again, since it may have done part of its work.
    """

    def __init__(
            self,
            collection:     Collection,
            model:          type[T] = Task,
            *,
            name:           str,
            functions:      Functions,
            settings:       TaskEngineSettings,
            limits:         Mapping[str, TaskLimits],
            distributions:  DistributionEngine | None = None,
            factory:        TaskFactory | None = None,
            extraIndexes:   Sequence[IndexModel] | None = None
        ):

        if not (isinstance(model, type) and issubclass(model, Task)):
            raise TypeError(f"a task engine stores a Task or a subclass of it, not {model!r}")

        unknown = sorted(set(limits) - set(functions))
        missing = sorted(set(functions) - set(limits))

        if unknown:
            raise TypeError(f"limits given for {', '.join(unknown)}, which {name} has no task for")

        # every task's limits, defaults filled in: a task left out would silently run without the
        # app's taskTimeout and taskSkipAfter
        if missing:
            raise TypeError(f"no limits given for {', '.join(missing)}; every task runs under its resolved limits")

        self.functions = functions
        self.settings = settings
        self.limits: dict[str, TaskLimits] = dict(limits)
        self.distributions = distributions or DistributionEngine()
        self.factory: TaskFactory = factory or TaskFactory(name="default")

        super().__init__(collection, model, name=name, extraIndexes=extraIndexes)

        self.leases = Leases(self.collection, settings.leaseSeconds, name=f"task-{name}")

    def createIndexes(self):
        super().createIndexes()
        self.collection.create_index([("status", 1), ("leaseUntil", 1)])    # the claim

    def __call__(self, functionName: str, /, **kwargs: Any) -> CallSpec:
        """A call of the task of this name, the arguments given checked: no unknown names, and each of
        the type the function declares.

        Whether any are missing is left to schedule(), which checks the call as the task's runWork()
        gives it, since a Task subclass may supply arguments of its own. A distribution call is
        checked in full, because nothing adds to it.
        """

        return self.functions.validate(CallSpec.new(functionName, **kwargs), complete=False)

    def limitsFor(self, task: Task) -> TaskLimits:
        """The limits a task runs under: those its function declares, with the app's defaults for the rest."""

        name = task.work.functionName

        if name not in self.functions:
            raise TaskNotFound(f"there is no task {name}")

        return self.limits[name]

    # --- scheduling ---

    def _prepare(self, document: T) -> T:
        """A waiting task is claimable at its deadline, so a deadline moved by save() or update() moves
        the lease with it. A claimed task keeps the lease its holder renews."""

        if document.status == "pending":
            document.leaseUntil = document.deadline

        return document

    def _refuseFields(self, fields: Mapping[str, Any]):
        own = {
            label
            for name, field in self.model.model_fields.items() if name not in Task.model_fields
            for label in (name, field.alias) if label
        }

        for name in fields:
            if name in own:
                continue

            if name in LIMITS:
                raise TypeError(f"a call carries no limits: {name} belongs to the function, declared on @task")

            if name in Task.model_fields:
                raise TypeError(f"{name} is not given to a task; the engine keeps it")

            adds = f"{self.model.__name__} adds {', '.join(sorted(own))}" if own else f"{self.model.__name__} adds no fields"

            raise TypeError(f"{self.model.__name__} has no field {name}; {adds}")

    def _newTask(
            self,
            work:       CallSpec,
            deadline:   datetime | None = None,
            factory:    TaskFactory | None = None,
            **fields:   Any
        ) -> T:

        """A task of this engine's model, checked as it will run but not stored — what schedule() stores,
        and what a scheduler emits.

        `fields` are the ones the model adds to Task. The call is checked as `runWork()` returns it, so
        a task whose context supplies an argument is checked with that argument in place.
        """

        self._refuseFields(fields)

        task = self.model(
            work=work,
            deadline=deadline if deadline is not None else utc_now(),
            factory=factory or self.factory,
            **fields,
        )

        self.functions.validate(task.runWork())

        return task._bind(self, stored=False)

    def schedule(
            self,
            work:       CallSpec,
            deadline:   datetime | None = None,
            factory:    TaskFactory | None = None,
            **fields:   Any
        ) -> T:

        """Queue one call, due at `deadline` or now. Nothing is stored unless it is valid.

        It runs under the limits its task declares; `fields` are the ones this engine's model adds.
        """

        return self.insert(self._newTask(work, deadline, factory, **fields))

    def scheduleFromDistribution(
            self,
            work:           CallSpec,
            distribution:   CallSpec,
            factory:        TaskFactory | None = None,
            **fields:       Any
        ) -> T:

        """Queue one call, due an interval drawn from `distribution` from now."""

        return self.schedule(work, utc_now() + self.distributions.gen(distribution), factory, **fields)

    # --- running ---

    def work(self) -> T | None:
        """Claim the most overdue task and see it through. Returns the task as it ended here, or None
        if nothing was due.

        Only tasks this engine has a function for are claimed. A worker drains the queue by calling
        this until it returns None.
        """

        now = utc_now()
        claim = claimNext(
            self.collection,
            {
                # due, or held by a worker that stopped renewing its lease
                "status": {"$in": ["pending", "running"]},
                "work.functionName": {"$in": list(self.functions)},
            },
            leaseSeconds=self.settings.leaseSeconds,
            fields={"status": "running", "claimedAt": now},
            now=now,
        )

        if claim is None:
            return None

        task = self._load(claim.after)

        if claim.before["status"] == "running":
            return self._writeOff(task, claim.before)

        if self._tooLate(task, now):
            return self._outdate(task, now)

        # held until the outcome is written, not only until the call returns, so the lease cannot lapse
        # in between. No time limit is enforced yet: timeouts arrive with the workers, in layer 4.
        with self.leases.holding(task.uid, task.claimId):
            self._run(task)
            self._record(task)

        return task

    def _run(self, task: T):
        start = time.perf_counter()

        try:
            task.result = self.functions.call(task.runWork())

            # a result the driver cannot store is found here, not by a write that fails with the task still claimed
            bson.encode(task.model_dump(include={"result"}))

            task.status = "done"
        except (Exception, SystemExit):     # sys.exit() in a task would otherwise end the worker's thread
            task.status = "failed"
            task.result = None
            task.error = traceback.format_exc()
        finally:
            task.executionTime = timedelta(seconds=time.perf_counter() - start)
            task.finishedAt = utc_now()

    def _record(self, task: T) -> bool:
        """Write a task's outcome, if the claim that ran it still holds it."""

        recorded = writeClaimed(
            self.collection, task.uid, task.claimId,
            task.model_dump(include=OUTCOME_FIELDS),
            where={"status": "running"},
        )

        if recorded:
            task.claimId = None
        else:
            logger.warning(
                "%r ended %s after its claim was lost — cancelled, or taken over when its lease lapsed; "
                "this outcome was not recorded", task, task.status
            )

        return recorded

    def _writeOff(self, task: T, before: Mapping[str, Any]) -> T:
        task.status = "failed"
        task.claimedAt = before.get("claimedAt")     # when it was started, not when it was found abandoned
        task.finishedAt = utc_now()
        task.error = WORKER_DIED

        if self._record(task):
            logger.error("%r failed: %s", task, task.error)

        return task

    def _tooLate(self, task: T, now: datetime) -> bool:
        """Whether a task went stale waiting to be claimed — after downtime, or in a queue nobody kept up
        with; it is the same condition."""

        limit = self.limitsFor(task).skipAfter

        return limit is not None and (now - task.deadline).total_seconds() > limit

    def _outdate(self, task: T, now: datetime) -> T:
        task.status = "outdated"
        task.finishedAt = utc_now()
        task.error = (
            f"claimed {(now - task.deadline).total_seconds():.0f}s after its deadline, past the "
            f"{self.limitsFor(task).skipAfter:g}s it was worth running for"
        )

        if self._record(task):
            logger.warning("%r was outdated: %s", task, task.error)

        return task

    # --- managing tasks ---

    @staticmethod
    def _notStarted() -> dict[str, Any]:
        """Tasks nobody is running: waiting, or held by a worker that stopped renewing its lease."""

        return {"$or": [
            {"status": "pending"},
            {"status": "running", "leaseUntil": {"$lte": utc_now()}},
        ]}

    @staticmethod
    def _cancelled() -> dict[str, Any]:
        # the claimId goes too, so a worker whose lease lapsed cannot write an outcome over the cancel
        return {"$set": {"status": "canceled", "finishedAt": utc_now(), "claimId": None}}

    def cancel(self, uid: str) -> bool:
        """Cancel a task that has not started. False if there is no such task, or it is running or
        finished — running work cannot be interrupted, only waited out."""

        return self.collection.update_one({"uid": uid, **self._notStarted()}, self._cancelled()).matched_count > 0

    def cancelMany(self, where: Mapping[str, Any] | None = None) -> int:
        """Cancel every task matching `where` that has not started. Returns how many were."""

        query = {"$and": [dict(where), self._notStarted()]} if where else self._notStarted()

        return self.collection.update_many(query, self._cancelled()).matched_count

    def wait(self, task: Task | str, timeout: float | None = None, interval: float = 0.1) -> T:
        """Block until a task has ended, and return it as it ended.

        Reads the collection, so any process can wait on a task any other runs. Raises TimeoutError if
        `timeout` runs out first, and TaskNotFound if there is no such task.
        """

        uid = task if isinstance(task, str) else task.uid
        deadline = None if timeout is None else time.monotonic() + timeout

        while True:
            current = self.get(uid)

            if current is None:
                raise TaskNotFound(f"there is no task {uid}")

            if current.status in FINAL_TASK_STATUSES:
                return current

            remaining = None if deadline is None else deadline - time.monotonic()

            if remaining is not None and remaining <= 0:
                raise TimeoutError(f"{current!r} is still {current.status} after {timeout}s")

            time.sleep(interval if remaining is None else min(interval, remaining))

    def flagIncompatible(self) -> int:
        """Mark waiting tasks whose function this engine does not have as incompatible. Returns how many.

        Housekeeping that decides from this process's tasks, so it is only safe while no process with
        other tasks is running workers; the app checks that before calling it.
        """

        return self.collection.update_many(
            {"status": "pending", "work.functionName": {"$nin": list(self.functions)}},
            {"$set": {
                "status": "incompatible",
                "error": "this app has no task of this name",
                "finishedAt": utc_now(),
            }},
        ).matched_count

    def writeOffStuck(self) -> int:
        """Write off tasks a dead worker left running whose function this engine does not have. Returns
        how many.

        A claim only takes tasks it can run, so none would ever find these. They fail with the error a
        claim would write — not incompatible, since the worker died before the function went.
        Housekeeping like flagIncompatible(), and checked by the app the same way.
        """

        now = utc_now()

        return self.collection.update_many(
            {"status": "running", "leaseUntil": {"$lte": now}, "work.functionName": {"$nin": list(self.functions)}},
            {"$set": {"status": "failed", "error": WORKER_DIED, "finishedAt": now, "claimId": None}},
        ).matched_count
