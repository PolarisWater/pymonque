"""Tasks: a call due at a time, run at most once — the Task model and the task engine."""

from __future__ import annotations

import ctypes
import logging
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Literal, Mapping, Self, Sequence, TypeVar

import bson
from pydantic import Field, model_validator
from pymongo import IndexModel
from pymongo.collection import Collection

from .calls import CallSpec, Functions
from .claims import Leases, WorkerLoop, abandonHolds, cancelled, claimNext, notStarted, writeClaimed
from .distributions import DistributionEngine
from .documents import CollectionEngine, Document, Duration, UtcDatetime, WorkStatus, utc_now
from .exceptions import TaskNotFound, TaskStopped, TaskTimeout, TaskValidationError
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


def stackOf(thread: threading.Thread) -> str:
    """Where a thread is right now, as a traceback reads."""

    frame = sys._current_frames().get(thread.ident)

    return "".join(traceback.format_stack(frame)) if frame is not None else "(the thread has ended)\n"


class _StoppableCall:
    """A call run in a thread of its own, which a timeout can ask to stop.

    Stopping raises TaskStopped by thread id, and an ended thread's id can be reused at once by a new
    one. So the call's thread marks itself finished under a lock, and a stop is only sent under that
    lock while it is not: until the thread has marked itself, it has not ended, and its id is its own.
    """

    def __init__(self, target: Callable[[], None], *, name: str):
        self._target = target
        self._guard = threading.Lock()
        self._finished = False
        self.thread = threading.Thread(target=self._run, name=name, daemon=True)

    def _run(self):
        try:
            try:
                self._target()
            finally:
                with self._guard:
                    self._finished = True
        except TaskStopped:
            pass    # a stop that arrived as the call was ending; the task was written off already

    def stop(self) -> bool:
        """Stop renewing what the call's thread holds, and raise TaskStopped in it at the next line of
        Python it runs. False if it had already finished.

        Python cannot kill a thread; this is the most it can do. A thread blocked in C, I/O or a sleep
        only sees the exception once that call returns.
        """

        with self._guard:
            if self._finished:
                return False

            # nobody waits for it now, so a pile item it holds is left to lapse — a spent try
            abandonHolds(self.thread.ident)

            return ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(self.thread.ident), ctypes.py_object(TaskStopped)
            ) == 1


@dataclass(frozen=True)
class Abandoned:
    """A call that outlived its timeout and would not stop: its task is written off, its thread lives on."""

    task:       Task
    thread:     threading.Thread
    since:      float       # time.monotonic() when the task started

    @property
    def runningFor(self) -> float:
        return time.monotonic() - self.since

    def where(self) -> str:
        return stackOf(self.thread)

    def __repr__(self) -> str:
        return f"Abandoned {self.task!r} ({self.task.uid}, running {self.runningFor:.0f}s)"


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
            extraIndexes:   Sequence[IndexModel] | None = None,
            pollInterval:   float = 1.0
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
        self.workers = WorkerLoop(self.work, name=f"task-{name}", pollInterval=pollInterval)

        # calls that outlived their timeout and would not stop, by thread; `onAbandoned` is told of each
        self._abandoned: dict[int, Abandoned] = {}
        self._abandonedLock = threading.Lock()
        self.onAbandoned: Callable[[TaskEngine], None] | None = None

    # how long a stopped call gets to stop before its thread is counted abandoned: per process, pacing
    stopGrace: float = 3.0

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
        # in between
        with self.leases.holding(task.uid, task.claimId):
            self._run(task)
            self._record(task)

        return task

    def _run(self, task: T):
        timeout = self.limitsFor(task).timeout
        start = time.perf_counter()

        try:
            task.result = self.functions.call(task.runWork()) if timeout is None else self._callWithin(task, timeout)

            # a result the driver cannot store is found here, not by a write that fails with the task still claimed
            bson.encode(task.model_dump(include={"result"}))

            task.status = "done"
        except TaskTimeout as e:
            task.status = "timeout"
            task.result = None
            task.error = str(e)
        except (Exception, SystemExit):     # sys.exit() in a task would otherwise end the worker's thread
            task.status = "failed"
            task.result = None
            task.error = traceback.format_exc()
        finally:
            task.executionTime = timedelta(seconds=time.perf_counter() - start)
            task.finishedAt = utc_now()

    def _callWithin(self, task: T, timeout: float) -> Any:
        """Run the call in a thread of its own, and stop waiting for it after `timeout` seconds.

        What a timeout guarantees is what every other process sees: the worker is freed and the task is
        written off. On top of that the call is asked to stop, and a pile item it holds stops being
        renewed; whether it did stop is logged a few seconds later.
        """

        outcome: dict[str, Any] = {}

        def call():
            try:
                outcome["result"] = self.functions.call(task.runWork())
            except TaskStopped:
                pass    # stopped at its timeout; the task was written off already
            except BaseException as e:      # carried back to the worker, which records it
                outcome["error"] = e

        started = time.monotonic()
        stoppable = _StoppableCall(call, name=f"pymonque-task-{task.work.functionName}")
        thread = stoppable.thread
        thread.start()
        thread.join(timeout)

        if not thread.is_alive():
            if "error" in outcome:
                raise outcome["error"]

            return outcome.get("result")

        where = stackOf(thread)

        stoppable.stop()

        logger.warning(
            "%r (%s) timed out after %gs: written off, its worker freed, and the call stopped. Where it was:\n%s",
            task, task.uid, timeout, where
        )

        threading.Thread(
            target=self._watchStopped, args=(task, thread, started),
            name=f"pymonque-task-{self.name}-stopping", daemon=True,
        ).start()

        raise TaskTimeout(
            f"{task.work.functionName} did not finish within {timeout:g}s, so it was written off and "
            f"stopped. Where it was:\n{where}"
        )

    def _watchStopped(self, task: T, thread: threading.Thread, started: float):
        """Say whether a call stopped at its timeout, and count it abandoned if it did not."""

        thread.join(self.stopGrace)

        if not thread.is_alive():
            logger.warning("%r (%s) stopped after its timeout", task, task.uid)

            return

        abandoned = Abandoned(task, thread, started)

        with self._abandonedLock:
            self._abandoned[thread.ident] = abandoned

        logger.error(
            "%r (%s) is still running after being stopped — blocked outside Python — so its thread is "
            "abandoned. Where it is:\n%s", task, task.uid, abandoned.where()
        )

        if self.onAbandoned is not None:
            self.onAbandoned(self)

    def abandoned(self) -> list[Abandoned]:
        """Calls that outlived their timeout and have not stopped yet, oldest first."""

        with self._abandonedLock:
            for threadId, each in list(self._abandoned.items()):
                if not each.thread.is_alive():
                    del self._abandoned[threadId]

            return sorted(self._abandoned.values(), key=lambda each: each.since)

    def _record(self, task: T, fields: frozenset[str] = OUTCOME_FIELDS) -> bool:
        """Write a task's outcome, if the claim that ran it still holds it."""

        recorded = writeClaimed(
            self.collection, task.uid, task.claimId,
            task.model_dump(include=fields),
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
        # when it was started, and when its worker's lease ran out — not the claim that found it abandoned
        task.status = "failed"
        task.claimedAt = before.get("claimedAt")
        task.leaseUntil = before.get("leaseUntil")
        task.finishedAt = utc_now()
        task.error = WORKER_DIED

        if self._record(task, OUTCOME_FIELDS | {"leaseUntil"}):
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

    def cancel(self, uid: str) -> bool:
        """Cancel a task that has not started. False if there is no such task, or it is running or
        finished — running work cannot be interrupted, only waited out."""

        return self.collection.update_one({"uid": uid, **notStarted()}, cancelled()).matched_count > 0

    def cancelMany(self, where: Mapping[str, Any] | None = None) -> int:
        """Cancel every task matching `where` that has not started. Returns how many were."""

        query = {"$and": [dict(where), notStarted()]} if where else notStarted()

        return self.collection.update_many(query, cancelled()).matched_count

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

    def backlog(self) -> tuple[int, float]:
        """Tasks due that nobody has picked up, and how long the oldest has waited, in seconds.

        A free worker claims within one poll interval, so anything waiting much longer than that means
        every worker is busy. A task held by a worker whose lease lapsed counts: nobody is running it.
        """

        now = utc_now()
        due = {
            "status": {"$in": ["pending", "running"]},
            "leaseUntil": {"$lte": now},
            "work.functionName": {"$in": list(self.functions)},
        }

        count = self.collection.count_documents(due)

        if not count:
            return 0, 0.0

        oldest = self.collection.find_one(due, sort=[("leaseUntil", 1)])

        return count, (now - oldest["leaseUntil"]).total_seconds()

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
