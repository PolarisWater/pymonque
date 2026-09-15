"""Claims: taking a document to work on, holding it while the work runs, and writing its outcome.

Anything claimable — a task, a pile item, a scheduler — has a `leaseUntil`: when it may be claimed.
While it waits, that is when it is due; while it is held, the end of the holder's lease. A claim is
one atomic find_one_and_update on that field, and gives the document a new `claimId`. Only a write
that still matches the claimId lands, so a holder whose lease lapsed cannot overwrite a cancel, or
the worker that took the document over.
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Iterator, Mapping

from pymongo import ReturnDocument
from pymongo.collection import Collection

from .documents import utc_now, uuid4str


logger = logging.getLogger("pymonque")


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


class Leases:
    """The claims this process holds in one collection, and the thread that keeps them held.

    A claim is held for exactly as long as its work runs, so its lease is renewed through a shutdown
    that waits for the work to finish, and stops being renewed when the work ends — whoever ends it.
    The renewing thread starts with the first claim held and ends with the last one let go.
    """

    def __init__(self, collection: Collection, leaseSeconds: float, *, name: str):
        self.collection = collection
        self.leaseSeconds = leaseSeconds
        self.name = name

        self._held: dict[str, str] = {}     # claimId -> uid
        self._lock = threading.Lock()
        self._changed = threading.Condition(self._lock)
        self._renewer: threading.Thread | None = None

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
            self._held[claimId] = uid

            if self._renewer is None:
                self._renewer = threading.Thread(target=self._renewLoop, name=f"pymonque-{self.name}-lease", daemon=True)
                self._renewer.start()

    def release(self, claimId: str):
        with self._lock:
            self._held.pop(claimId, None)

            if not self._held:
                self._changed.notify_all()      # so the renewer ends now, not an interval from now

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
            {"uid": {"$in": sorted(set(held.values()))}, "claimId": {"$in": list(held)}},
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
