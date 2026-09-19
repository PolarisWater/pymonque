"""Claims: taking a document to work on, holding it while the work runs, and writing its outcome.

Anything claimable — a task, a pile item, a scheduler — has a `leaseUntil`: when it may be claimed.
While it waits, that is when it is due; while it is held, the end of the holder's lease. A claim is
one atomic find_one_and_update on that field, and gives the document a new `claimId`. Only a write
that still matches the claimId lands, so a holder whose lease lapsed cannot overwrite a cancel, or
the worker that took the document over.
"""

from __future__ import annotations

import logging
import random
import threading
import time
import weakref
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable, Iterator, Mapping

from pymongo import ReadPreference, ReturnDocument, WriteConcern
from pymongo.collection import Collection

from .documents import utc_now, uuid4str


logger = logging.getLogger("pymonque")


def durable(collection: Collection) -> Collection:
    """A work collection as claims need it, whatever the app's database was given: writes acknowledged
    by a majority of the replica set, reads from the primary.

    A claim acknowledged by a primary alone is lost if that primary fails over before replicating it,
    and two workers then hold the same work; a secondary can show a lease or a status that has since
    moved. On a single server, majority is that server, and nothing changes.
    """

    return collection.with_options(write_concern=WriteConcern("majority"), read_preference=ReadPreference.PRIMARY)


@dataclass(frozen=True)
class Claim:
    """A document as a claim found it, and as the claim left it."""

    before: dict[str, Any]
    after:  dict[str, Any]

    @property
    def claimId(self) -> str:
        return self.after["claimId"]


def claimNext(
        collection:     Collection,
        where:          Mapping[str, Any],
        *,
        leaseSeconds:   float,
        fields:         Mapping[str, Any] | None = None,
        inc:            Mapping[str, int] | None = None,
        now:            datetime | None = None
    ) -> Claim | None:

    """Claim the document matching `where` whose lease ran out first, or return None if none has.

    The claim sets a new claimId, a lease of `leaseSeconds` from now, and `fields`; `inc` counts
    something up, such as an item's tries. Both name top-level fields. The document comes back as the
    claim found it too, so the caller can tell what it took over — a task still `running` was held by
    a worker that stopped renewing its lease.
    """

    now = now or utc_now()
    claimed = {**(fields or {}), "claimId": uuid4str(), "leaseUntil": now + timedelta(seconds=leaseSeconds)}
    update: dict[str, Any] = {"$set": claimed}

    if inc:
        update["$inc"] = dict(inc)

    before = collection.find_one_and_update(
        {**where, "leaseUntil": {"$lte": now}},
        update,
        sort=[("leaseUntil", 1)],   # a waiting document's lease is its due time, so the most overdue goes first
        return_document=ReturnDocument.BEFORE,
    )

    if before is None:
        return None

    after = {**before, **claimed}

    for name, step in (inc or {}).items():
        after[name] = before.get(name, 0) + step

    return Claim(before=before, after=after)


def writeClaimed(
        collection: Collection,
        uid:        str,
        claimId:    str | None,
        fields:     Mapping[str, Any],
        *,
        where:      Mapping[str, Any] | None = None
    ) -> bool:

    """Write an outcome if this claim still holds the document, and let the document go.

    The claimId is cleared with the write, so nothing more lands under this claim: not a second
    outcome, and not a renewal racing the first one. False if the claim was lost — cancelled, taken
    over when its lease lapsed, or already ended.
    """

    if claimId is None:
        raise ValueError(f"document {uid} stands for no claim, so there is no claim to write under")

    return collection.update_one(
        {**(where or {}), "uid": uid, "claimId": claimId},
        {"$set": {**fields, "claimId": None}}
    ).matched_count > 0


def ageOf(olderThan: float | timedelta) -> timedelta:
    """An age given as seconds or a timedelta, refused if it is neither, or negative."""

    if isinstance(olderThan, bool) or not isinstance(olderThan, (int, float, timedelta)):
        raise TypeError(f"an age is a number of seconds or a timedelta, not {olderThan!r}")

    age = olderThan if isinstance(olderThan, timedelta) else timedelta(seconds=olderThan)

    if age < timedelta(0):
        raise ValueError(f"an age cannot be negative: {age}")

    return age


def endedBefore(statuses: Iterable[str], cutoff: datetime) -> dict[str, Any]:
    """Matches work that finished, in one of `statuses`, before `cutoff`. Work finished before its end
    was recorded — a 2.0 document — counts by its createdAt, or failing that its deadline."""

    return {
        "status": {"$in": sorted(statuses)},
        "$or": [
            {"finishedAt": {"$lt": cutoff}},
            {"finishedAt": None, "createdAt": {"$lt": cutoff}},
            {"finishedAt": None, "createdAt": None, "deadline": {"$lt": cutoff}},
        ],
    }


def notStarted() -> dict[str, Any]:
    """Matches work nobody is running: waiting, or held by a worker that stopped renewing its lease."""

    return {"$or": [
        {"status": "pending"},
        {"status": "running", "leaseUntil": {"$lte": utc_now()}},
    ]}


def cancelled() -> dict[str, Any]:
    """The write that cancels work. The claimId goes with it, so a holder whose lease lapsed cannot
    write an outcome over the cancel."""

    return {"$set": {"status": "canceled", "finishedAt": utc_now(), "claimId": None}}


# every Leases of the process, so a timed-out call's holds can be dropped wherever they were taken
_everyLeases: weakref.WeakSet[Leases] = weakref.WeakSet()
_everyLeasesLock = threading.Lock()


def abandonHolds(threadId: int) -> int:
    """Stop renewing every claim the thread `threadId` holds, in any collection. Returns how many.

    For a call that outlived its timeout: it may still be running, but nobody waits for it any more,
    so a pile item it holds is left to lapse — a spent try — and another worker can take it.
    """

    with _everyLeasesLock:
        everyLeases = list(_everyLeases)

    return sum(leases._dropThread(threadId) for leases in everyLeases)


class Leases:
    """The claims this process holds in one collection, and the thread that keeps them held.

    A claim is held for exactly as long as its work runs, so its lease is renewed through a shutdown
    that waits for the work to finish, and stops being renewed when the work ends — whoever ends it.
    The renewing thread starts with the first claim held and ends with the last one let go. Each hold
    remembers the thread that took it, so a call abandoned at its timeout stops holding anything.
    """

    def __init__(self, collection: Collection, leaseSeconds: float, *, name: str):
        self.collection = collection
        self.leaseSeconds = leaseSeconds
        self.name = name

        self._held: dict[str, tuple[str, int]] = {}     # claimId -> (uid, the holding thread)
        self._lock = threading.Lock()
        self._changed = threading.Condition(self._lock)
        self._renewer: threading.Thread | None = None

        with _everyLeasesLock:
            _everyLeases.add(self)

    @property
    def interval(self) -> float:
        # a few renewals per lease, so one slow or failed write does not lose the claim
        return self.leaseSeconds / 3

    def __len__(self) -> int:
        with self._lock:
            return len(self._held)

    @property
    def renewing(self) -> bool:
        with self._lock:
            return self._renewer is not None

    def hold(self, uid: str, claimId: str):
        with self._lock:
            self._held[claimId] = (uid, threading.get_ident())

            if self._renewer is None:
                self._renewer = threading.Thread(target=self._renewLoop, name=f"pymonque-{self.name}-lease", daemon=True)
                self._renewer.start()

    def release(self, claimId: str):
        with self._lock:
            self._held.pop(claimId, None)

            if not self._held:
                self._changed.notify_all()      # so the renewer ends now, not an interval from now

    def _dropThread(self, threadId: int) -> int:
        with self._lock:
            dropped = [claimId for claimId, (_, holder) in self._held.items() if holder == threadId]

            for claimId in dropped:
                del self._held[claimId]

            if dropped and not self._held:
                self._changed.notify_all()

        for claimId in dropped:
            logger.warning("%s: stopped renewing claim %s, held by a call abandoned at its timeout", self.name, claimId)

        return len(dropped)

    @contextmanager
    def holding(self, uid: str, claimId: str) -> Iterator[None]:
        self.hold(uid, claimId)

        try:
            yield
        finally:
            self.release(claimId)

    def renew(self) -> int:
        """Push out the lease of every claim held here that still holds its document. Returns how many do."""

        with self._lock:
            held = dict(self._held)

        if not held:
            return 0

        return self.collection.update_many(
            # the uid for the index; the claimId so a document another worker has taken since, or
            # whose outcome is written, is left alone
            {"uid": {"$in": sorted({uid for uid, _ in held.values()})}, "claimId": {"$in": list(held)}},
            {"$set": {"leaseUntil": utc_now() + timedelta(seconds=self.leaseSeconds)}}
        ).matched_count

    def _renewLoop(self):
        while True:
            # one lock for the check and the exit, or a claim held in between would find a renewer
            # still set, start none, and never be renewed
            with self._lock:
                if self._held:
                    self._changed.wait(self.interval)

                if not self._held:
                    self._renewer = None
                    return

            try:
                self.renew()
            except Exception:
                logger.exception("%s lease renewal failed", self.name)

    def __repr__(self) -> str:
        return f"Leases {self.name} ({len(self)} held)"


class WorkerLoop:
    """Worker threads that call one engine's `work()` until asked to stop.

    A worker checks for a stop request between pieces of work, never inside one, so a shutdown always
    finishes what was already claimed. It sleeps only when there was nothing to do, so a backlog
    drains without waiting between tasks, and a stop request wakes it at once.
    """

    def __init__(self, work: Callable[[], Any], *, name: str, pollInterval: float = 1.0):
        self.work = work
        self.name = name
        self.pollInterval = pollInterval

        self.count: int = 0     # threads started since the last full stop
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._busy = 0
        self._busyLock = threading.Lock()

    @property
    def running(self) -> bool:
        return any(t.is_alive() for t in self._threads)

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    @property
    def busy(self) -> int:
        """Workers in the middle of a piece of work."""

        with self._busyLock:
            return self._busy

    @property
    def threads(self) -> list[threading.Thread]:
        return list(self._threads)

    def _idle(self):
        # jittered, so workers started together do not poll in lockstep; a wait on the stop event, so a
        # shutdown does not sit through a whole interval
        self._stop.wait(self.pollInterval * (0.9 + random.random() * 0.2))

    def _loop(self):
        while not self._stop.is_set():
            with self._busyLock:
                self._busy += 1

            try:
                worked = self.work()
            except Exception:
                # a failing iteration — the database away, a stored document that no longer fits its
                # model — is logged, and the worker carries on
                logger.exception("%s worker iteration failed", self.name)
                worked = None
            finally:
                with self._busyLock:
                    self._busy -= 1

            if worked is None:
                self._idle()

    def start(self, count: int):
        if count <= 0:
            return

        self._stop.clear()
        self._threads = [t for t in self._threads if t.is_alive()]

        threads = [
            threading.Thread(target=self._loop, name=f"pymonque-{self.name}-{self.count + n}", daemon=True)
            for n in range(count)
        ]

        self.count += count
        self._threads.extend(threads)

        for thread in threads:
            thread.start()

    def requestStop(self):
        """Stop claiming, without waiting for the work in flight."""

        self._stop.set()

    def stop(self, timeout: float | None = None) -> bool:
        """Stop claiming and wait for the work in flight. False if any worker was still busy when
        `timeout` ran out."""

        self._stop.set()

        deadline = None if timeout is None else time.monotonic() + timeout

        for thread in self._threads:
            thread.join(None if deadline is None else max(0.0, deadline - time.monotonic()))

        self._threads = [t for t in self._threads if t.is_alive()]

        if not self._threads:
            self.count = 0

        return not self._threads

    def __repr__(self) -> str:
        state = "stopping" if self.stopping else "running" if self.running else "idle"

        return f"WorkerLoop {self.name} ({self.count} workers, {state})"
