"""Schedulers: a recurring schedule that emits a task on every beat — the Scheduler model and the
scheduler engine."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, ClassVar, Literal, Mapping, Self, Sequence, TypeVar

from pydantic import model_validator
from pymongo import IndexModel
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError

from .calls import CallSpec
from .claims import WorkerLoop, claimNext, writeClaimed
from .documents import CollectionEngine, UtcDatetime, utc_now
from .settings import Missed, SchedulerEngineSettings
from .tasks import Task, TaskEngine, TaskFactory


logger = logging.getLogger("pymonque")

# being worked is a lease, not a status: `status` says whether the scheduler emits, nothing else
SchedulerStatus = Literal["enabled", "disabled"]

# unchanged since 2.0, so a scheduler declared by name keeps its uid across the upgrade
SCHEDULER_NAMESPACE = uuid.UUID("1f0e3d4c-5b6a-4798-8a9b-0c1d2e3f4a5b")


def schedulerUid(name: str) -> str:
    """A stable uid for a named scheduler, so declaring it on every startup declares it once."""

    return str(uuid.uuid5(SCHEDULER_NAMESPACE, name))


def _sameMoment(stored: datetime, given: datetime) -> bool:
    # MongoDB keeps milliseconds, so a deadline read back can differ from the one written by less than one
    return abs(stored.replace(tzinfo=None) - given.replace(tzinfo=None)) < timedelta(milliseconds=1)


def beatUid(scheduler: str, deadline: datetime) -> str:
    """The uid of the task a scheduler emits for one beat. Fixed, so a worker that dies after emitting
    but before moving the deadline on cannot emit the beat twice."""

    return str(uuid.uuid5(SCHEDULER_NAMESPACE, f"{scheduler}@{deadline.isoformat()}"))


class Scheduler(TaskFactory):
    """A call emitted as a task on a rhythm drawn from a distribution.

    Schedulers are assumed never to fail: emitting is a database write. Subclass it to store fields of
    your own on every scheduler of an engine; `taskFields()` hands them to the tasks it emits.
    """

    # its identity and claim are the engine's; whether it emits is set with enabled=
    _kept: ClassVar[frozenset[str]] = frozenset({"uid", "status", "claimId", "leaseUntil"})

    name:           str                 = "Scheduler"
    status:         SchedulerStatus     = "enabled"
    work:           CallSpec            # emitted unchanged
    distribution:   CallSpec            # the interval between beats
    deadline:       UtcDatetime         # the next beat

    # claimable at its deadline while waiting, the end of the holder's lease while held
    leaseUntil:     UtcDatetime | None  = None
    claimId:        str | None          = None

    @model_validator(mode="after")
    def defaultLease(self) -> Self:
        if self.leaseUntil is None:
            self.leaseUntil = self.deadline

        return self

    def taskFields(self) -> dict[str, Any]:
        """The fields this scheduler gives every task it emits: none, by default.

        A scheduler never changes the call it emits. Context reaches the call through the task — a
        field here, that the target engine's Task subclass stores and stamps on in its runWork():

            class AccountScheduler(Scheduler):
                accountId: int

                def taskFields(self) -> dict[str, Any]:
                    return {"accountId": self.accountId}
        """

        return {}

    def __repr__(self) -> str:
        return f"Scheduler {self.name}: {self.work!r}"


S = TypeVar("S", bound=Scheduler)

class SchedulerEngine(CollectionEngine[S]):
    """Recurring schedulers in one collection, each emitting a task into one task engine on its rhythm.

    A beat is emitted once: its task's uid is fixed to the scheduler and the deadline. A deadline is
    only moved on if nobody moved it meanwhile, and a beat that came and went unworked is dealt with
    as `missed` says.
    """

    def __init__(
            self,
            collection:     Collection,
            model:          type[S] = Scheduler,
            *,
            name:           str,
            settings:       SchedulerEngineSettings,
            tasks:          TaskEngine,
            extraIndexes:   Sequence[IndexModel] | None = None,
            pollInterval:   float = 1.0
        ):

        if not (isinstance(model, type) and issubclass(model, Scheduler)):
            raise TypeError(f"a scheduler engine stores a Scheduler or a subclass of it, not {model!r}")

        self.settings = settings
        self.tasks = tasks
        self.distributions = tasks.distributions    # the app has one registry, which its task engines carry

        super().__init__(collection, model, name=name, extraIndexes=extraIndexes)

        self.workers = WorkerLoop(self.work, name=f"scheduler-{name}", pollInterval=pollInterval)

    @property
    def missed(self) -> Missed:
        return self.settings.missed

    def createIndexes(self):
        super().createIndexes()
        self.collection.create_index([("leaseUntil", 1)])      # the claim: due, whatever the status

    @staticmethod
    def _at(deadline: datetime) -> dict[str, datetime]:
        # a scheduler nobody holds is claimable exactly at its deadline, so every write of one moves
        # the lease with it
        return {"deadline": deadline, "leaseUntil": deadline}

    def _prepare(self, document: S) -> S:
        # a held scheduler keeps its holder's lease; any other is claimable at its deadline
        if document.claimId is None:
            document.leaseUntil = document.deadline

        return document

    _keptHints: ClassVar[dict[str, str]] = {
        "status": "status is not given to a scheduler; enable or disable it with enabled=",
    }

    def _keepStored(self, document: S, stored: S) -> S:
        """A saved scheduler keeps its stored claim and lease — so its holder's deadline write still lands
        — unless its deadline moved: that releases the claim, as update() and ensure() do, and the lease
        follows the new deadline."""

        deadline = document.deadline
        super()._keepStored(document, stored)

        if not _sameMoment(stored.deadline, deadline):
            document.claimId = None

        return document

    # --- checking ---

    def _beat(self, scheduler: S) -> Task:
        """The task a scheduler emits for its current beat: its work as stored, its taskFields(), and a
        uid fixed to the beat. Checked as it will run, and not stored."""

        task = self.tasks._newTask(scheduler.work, scheduler.deadline, scheduler, **scheduler.taskFields())
        task.uid = beatUid(scheduler.uid, scheduler.deadline)

        return task

    def validateScheduler(self, scheduler: S) -> S:
        """Raise unless a scheduler would run: its distribution, and the task it emits — the fields its
        taskFields() supplies against the target engine's Task model, and the call as that task's
        runWork() gives it. So a scheduler whose context the target engine cannot take is refused."""

        self.distributions.validate(scheduler.distribution)
        self._beat(scheduler)

        return scheduler

    # --- adding ---

    def _refuseFields(self, fields: Mapping[str, Any]):
        self._refuseKept(fields)

        known = {
            label
            for name, field in self.model.model_fields.items()
            for label in (name, field.alias) if label
        }

        for name in fields:
            if name not in known:
                raise TypeError(f"{self.model.__name__} has no field {name}")

    def build(self, work: CallSpec, distribution: CallSpec, deadline: datetime | None = None, **fields: Any) -> S:
        """A scheduler of this engine's model, not stored and not checked; its first beat one interval
        from now unless given a deadline. `fields` are the model's own, such as `name`; what the engine
        keeps — uid, claim, lease and status — is refused."""

        self._refuseFields(fields)

        return self._build(work, distribution, deadline, **fields)

    def _build(self, work: CallSpec, distribution: CallSpec, deadline: datetime | None = None, **fields: Any) -> S:
        if deadline is None:
            deadline = utc_now() + self.distributions.gen(distribution)     # gen checks the call first

        return self.model(work=work, distribution=distribution, deadline=deadline, **fields)._bind(self, stored=False)

    def add(self, work: CallSpec, distribution: CallSpec, **fields: Any) -> S:
        """Store a new scheduler, checked first, its first beat one interval from now."""

        return self.insert(self.validateScheduler(self.build(work, distribution, **fields)))

    def upsert(self, scheduler: S) -> S:
        """Store a scheduler under its own uid, creating or replacing it, checked first."""

        return self.save(self.validateScheduler(scheduler))

    def byName(self, name: str) -> S | None:
        """The scheduler declared under this name by ensure()."""

        return self.get(schedulerUid(name))

    def removeNamed(self, name: str) -> bool:
        """Delete the scheduler declared under this name."""

        return self.delete(schedulerUid(name))

    def _changes(self, existing: S, work: CallSpec | None, distribution: CallSpec | None, enabled: bool | None, fields: dict[str, Any]) -> dict[str, Any]:
        """What to write to change a stored scheduler: the fields given, validated as the merged scheduler
        before anything is written, with the lease moved with any new deadline."""

        changes = dict(fields)

        if work is not None:
            changes["work"] = work

        if distribution is not None:
            changes["distribution"] = distribution

        merged = self.validateScheduler(self._assign(existing.model_copy(), changes))
        dumped = merged.model_dump()
        write = {name: dumped[name] for name in changes if name in dumped}

        if "deadline" in write:
            write.update(self._at(write["deadline"]))

        if distribution is not None and distribution != existing.distribution:
            # the rhythm itself changed, so the new one starts from now
            write.update(self._at(utc_now() + self.distributions.gen(distribution)))

        if "deadline" in write:
            # a deadline moved by hand releases whatever claim held the beat it replaces, whose own
            # deadline write will not land
            write["claimId"] = None

        if enabled is not None:
            write["status"] = "enabled" if enabled else "disabled"

        return write

    def update(
            self,
            uid:            str,
            /,                                  # so a uid among the fields is refused, not confused with this one
            work:           CallSpec | None = None,
            distribution:   CallSpec | None = None,
            enabled:        bool | None = None,
            **fields:       Any
        ) -> S | None:

        """Change parts of a stored scheduler, or return None if there is no such uid.

        Everything is checked as the merged scheduler, so its fields are checked against the task it
        emits. A new distribution restarts the rhythm from now; new work keeps it. Enabling goes through
        `enabled`; a deadline may be moved by hand.
        """

        self._refuseFields(fields)
        existing = self.get(uid)

        if existing is None:
            return None

        write = self._changes(existing, work, distribution, enabled, fields)

        if write:
            self.collection.update_one({"uid": uid}, {"$set": write})

        return self.get(uid)

    def ensure(
            self,
            name:           str,
            work:           CallSpec,
            distribution:   CallSpec,
            enabled:        bool | None = None,
            **fields:       Any
        ) -> S:

        """Declare a scheduler that should always exist, safe to call on every startup.

        The name gives it its uid, so it is created the first time and kept in step afterwards. New work
        or fields update it in place; a new distribution also restarts its rhythm. Whether it is enabled
        is left as the database has it unless `enabled` is given.
        """

        self._refuseFields(fields)
        uid = schedulerUid(name)
        existing = self.get(uid)

        if existing is None:
            scheduler = self.validateScheduler(self._build(
                work, distribution, uid=uid, name=name, status="disabled" if enabled is False else "enabled", **fields
            ))

            try:
                # processes booting together all declare it; only one inserts
                self.collection.update_one({"uid": uid}, {"$setOnInsert": self._prepare(scheduler).model_dump()}, upsert=True)
            except DuplicateKeyError:
                pass    # another process won the race, and its scheduler stands

            return self.byName(name)

        self.collection.update_one({"uid": uid}, {"$set": self._changes(existing, work, distribution, enabled, fields)})

        return self.byName(name)

    # --- firing ---

    def work(self) -> S | None:
        """Claim the scheduler whose beat is most overdue, emit its task, and move its deadline on.
        Returns the scheduler as it was claimed, or None if none was due.

        A disabled scheduler walks its deadline without emitting, so its distribution keeps its shape
        for when it is enabled again. A held scheduler is not renewed: emitting takes milliseconds
        against a lease of minutes, and a stall that outlasts the lease is covered by the beat's uid.
        """

        now = utc_now()
        claim = claimNext(self.collection, {}, leaseSeconds=self.settings.leaseSeconds, now=now)

        if claim is None:
            return None

        scheduler = self._load(claim.after)
        interval = self.distributions.gen(scheduler.distribution)

        # a whole beat came and went unworked, so the scheduler cannot keep its cadence — nothing was
        # running, or it is set faster than it can be served. The only moment `missed` has a say.
        behind = scheduler.deadline + interval <= now

        if scheduler.status == "enabled" and not (behind and self.missed == "skip"):
            self._emit(scheduler)

        # only "replay" walks the backlog beat by beat; the others resume from now, so time spent
        # behind is not time owed
        deadline = scheduler.deadline + interval if self.missed == "replay" or not behind else now + interval

        # the claim is let go with the write, which lands only if nobody moved the deadline meanwhile:
        # ensure() or update() restarting the rhythm mid-claim wrote their own, lease included
        writeClaimed(self.collection, scheduler.uid, scheduler.claimId, self._at(deadline), where={"deadline": scheduler.deadline})

        # a disabled scheduler owes nothing, so it has nothing to warn about
        if behind and scheduler.status == "enabled":
            logger.warning("%r missed a beat of %s (missed=%r)", scheduler, interval, self.missed)

        return scheduler

    def _emit(self, scheduler: S):
        try:
            task = self._beat(scheduler)
        except Exception as e:
            # whatever stops the beat being built — its task gone, or its call or fields no longer fitting
            # a model changed since it was stored — skips this beat, not the scheduler: raising would leave
            # it claimed with its deadline unmoved, failing again after every lease. Skipped, not disabled:
            # disabling is the operator's word, and would outlast the fix.
            logger.warning("%r emits a task this app cannot build, so this beat was skipped: %r", scheduler, e)

            return

        try:
            self.tasks.insert(task)
        except DuplicateKeyError:
            pass    # emitted by a worker that died before moving the deadline on
