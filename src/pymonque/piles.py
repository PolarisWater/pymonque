"""Piles: items waiting to be claimed, drained by tasks — the Item model, the pile engine, and the
work() block that holds one item for as long as it is being worked."""

from __future__ import annotations

import logging
import traceback
from contextlib import contextmanager
from datetime import timedelta
from typing import Annotated, Any, Generic, Iterable, Iterator, Mapping, NoReturn, Self, Sequence, TypeVar

from pydantic import BaseModel, Field, model_validator
from pymongo import IndexModel
from pymongo.collection import Collection

from .claims import Leases, cancelled, claimNext, notStarted
from .documents import CollectionEngine, Document, UtcDatetime, WorkStatus, utc_now
from .settings import PileSettings


logger = logging.getLogger("pymonque")

P = TypeVar("P")


class Item(Document, Generic[P]):
    """A piece of work on a pile, carrying its data and no function or deadline.

    Items have tries because the task holding one can die before it reports back: a claim uses a
    try, and a released item gets its try back.
    """

    status:         WorkStatus          = "pending"
    data:           P

    createdAt:      UtcDatetime         = Field(default_factory=utc_now)
    claimedAt:      UtcDatetime | None  = None
    finishedAt:     UtcDatetime | None  = None

    # claimable from the moment it exists, oldest first; the end of the holder's lease while held
    leaseUntil:     UtcDatetime | None  = None
    attempts:       Annotated[int, Field(ge=0)] = 0
    claimId:        str | None          = None

    result:         Any                 = None
    error:          str | None          = None

    @model_validator(mode="after")
    def defaultLease(self) -> Self:
        if self.leaseUntil is None:
            self.leaseUntil = self.createdAt

        return self

    def __repr__(self) -> str:
        return f"Item {self.uid} ({self.status})"


def _delay(delay: float | timedelta | None) -> timedelta | None:
    """A release delay as a timedelta, refused unless it is a number of seconds or a timedelta, not negative."""

    if delay is None:
        return None

    if isinstance(delay, bool) or not isinstance(delay, (int, float, timedelta)):
        raise TypeError(f"a release delay is a number of seconds or a timedelta, not {delay!r}")

    delay = delay if isinstance(delay, timedelta) else timedelta(seconds=delay)

    if delay < timedelta(0):
        raise ValueError(f"a release delay cannot be negative: {delay}")

    return delay


class _Ended(BaseException):
    """Raised by w.done(), w.fail() and w.release() once the outcome is written, to leave that block
    and nothing else.

    A BaseException, so `except Exception:` inside the block does not swallow it, and it carries the
    claim it ends, so a nested block passes on an outer block's early end.
    """

    def __init__(self, claimId: str):
        super().__init__(claimId)
        self.claimId = claimId


class Work:
    """The item a `work()` block holds.

        with app.outbox.work() as w:
            if w is None:
                return                  # the pile is empty
            if rateLimited():
                w.release()             # back on the pile, try returned — and the block ends here
            send(w.data)
    """

    def __init__(self, pile: PileEngine, item: Item):
        self.pile = pile
        self.item = item
        self.ended: str | None = None   # the outcome that ended the block, if one did

    @property
    def data(self) -> Any:
        return self.item.data

    @property
    def uid(self) -> str:
        return self.item.uid

    @property
    def attempts(self) -> int:
        return self.item.attempts

    def done(self, result: Any = None) -> NoReturn:
        """Mark the item done, and end the block here."""

        self._end("done", result=result)

    def fail(self, error: str | None = None) -> NoReturn:
        """Mark the item failed — which is final — and end the block here."""

        self._end("fail", error=error)

    def release(self, delay: float | timedelta | None = None) -> NoReturn:
        """Hand the item back unfinished, with its try, and end the block here — claimable again at once,
        or after `delay` seconds."""

        self._end("release", delay=delay)

    def confirm(self):
        """Make sure this block still holds its item, just before a side effect that must not happen twice.

        A holder that was frozen — a paused VM, a suspended laptop, the database out of reach — for
        longer than its lease cannot tell, and another worker may have taken the item meanwhile. This
        asks the database now: if the claim still holds the item, its lease is renewed and the block
        goes on; if not — taken over, cancelled, given up — the block ends here, nothing is written,
        and it is logged.

            with app.outbox.work() as w:
                body = render(w.data)       # slow, safe to redo
                w.confirm()                 # still mine? if not, the block ends here
                send(w.data.to, body)       # the side effect

        It narrows the window to the moment between this call and the side effect; only the receiving
        system can close it, by refusing a duplicate — give it `w.uid` as an idempotency key.
        """

        if self.pile.renewLease(self.item):
            return

        logger.warning(
            "%r: w.confirm() found its claim lost — taken over when its lease lapsed, cancelled, or given "
            "up on; the block ends here and nothing is written", self.item
        )

        self.ended = "confirm"

        raise _Ended(self.item.claimId)

    def _end(self, outcome: str, **fields: Any) -> NoReturn:
        # written first, then the block is left: an outcome recorded is one nothing can take back
        if not getattr(self.pile, outcome)(self.item, **fields):
            self.pile._lostClaim(self.item)

        self.ended = outcome

        raise _Ended(self.item.claimId)

    def __repr__(self) -> str:
        return f"Work {self.item!r}"


class PileEngine(CollectionEngine[Item]):
    """A pile of items in its own collection, claimed one at a time.

    A pile runs no workers of its own: its items are drained by whatever is already running, usually a
    task a scheduler fires on a rhythm. A claim uses a try, so work whose holder died is claimed again
    while it has tries left, and given up once it has none.
    """

    def __init__(
            self,
            collection:     Collection,
            model:          type[BaseModel] | None = None,      # None: items carry any dict
            *,
            name:           str,
            settings:       PileSettings,
            extraIndexes:   Sequence[IndexModel] | None = None
        ):

        self.payload = model
        self.settings = settings

        # a concrete Item whose data is validated against the payload model
        super().__init__(
            collection,
            Item[model] if model is not None else Item[dict[str, Any]],
            name=name,
            extraIndexes=extraIndexes,
        )

        self.leases = Leases(self.collection, settings.leaseSeconds, name=f"pile-{name}")

    @property
    def maxAttempts(self) -> int:
        return self.settings.maxAttempts

    def createIndexes(self):
        super().createIndexes()
        self.collection.create_index([("status", 1), ("leaseUntil", 1)])    # the claim

    # --- filling the pile ---

    def _item(self, data: Any) -> Item:
        return self.model(data=data)

    def add(self, data: Any = None, **kwargs: Any) -> Item:
        """Add one item, from a payload model, a dict, or keyword arguments."""

        return self.insert(self._item(data if data is not None else kwargs))

    def addMany(self, data: Iterable[Any]) -> list[Item]:
        """Add items in one write, every payload validated first."""

        return self.insertMany(self._item(each) for each in data)

    # --- taking work out of it ---

    def claim(self, where: Mapping[str, Any] | None = None) -> Item | None:
        """Take the item that has waited longest, or None if there is none to take.

        Claimable is waiting, or held by a worker that stopped renewing its lease. One atomic write, so
        two workers racing on a pile can never receive the same item.
        """

        query = {"status": {"$in": ["pending", "running"]}, **(dict(where) if where else {})}

        while True:
            now = utc_now()
            claim = claimNext(
                self.collection, query,
                leaseSeconds=self.settings.leaseSeconds,
                fields={"status": "running", "claimedAt": now},
                inc={"attempts": 1},
                now=now,
            )

            if claim is None:
                return None

            item = self._load(claim.after)

            if item.attempts <= self.maxAttempts:
                return item

            self._giveUp(item)      # and look for the next one, rather than blocking on this

    def _giveUp(self, item: Item):
        spent = item.attempts - 1
        error = (
            f"gave up after {spent} {'try' if spent == 1 else 'tries'} whose outcome never came back — a holder "
            f"that stopped renewing its lease, or a block ended from outside it"
        )

        self._finish(item, "failed", error=error)
        logger.error("%r %s", item, error)

    # --- finishing ---

    @staticmethod
    def _matching(item: Item | str) -> dict[str, Any]:
        """Which document an action on `item` may touch.

        An Item stands for the claim it came from, so a holder whose lease lapsed cannot finish work
        another worker has taken over. A bare uid is an operator's verdict on unfinished work, whatever
        holds it; an item already done, failed or cancelled is left as it ended.
        """

        if isinstance(item, str):
            return {"uid": item, "status": {"$in": ["pending", "running"]}}

        if item.claimId is None:
            raise ValueError(
                f"item {item.uid} was not handed out by claim(), so it stands for no claim — claim it "
                f"first, or pass its uid to act on it by hand"
            )

        return {"uid": item.uid, "claimId": item.claimId}

    def _finish(self, item: Item | str, status: WorkStatus, result: Any = None, error: str | None = None) -> bool:
        write: dict[str, Any] = {"status": status, "finishedAt": utc_now(), "claimId": None}

        if result is not None:
            write["result"] = result

        if error is not None:
            write["error"] = error

        # matched, not modified: the question is whether there is such an item to finish, not whether
        # the bytes happened to change
        return self.collection.update_one(self._matching(item), {"$set": write}).matched_count > 0

    def done(self, item: Item | str, result: Any = None) -> bool:
        """Mark an item done. False if the claim no longer holds it, or there is no such item."""

        return self._finish(item, "done", result=result)

    def fail(self, item: Item | str, error: str | None = None) -> bool:
        """Record a failure, which is final: the item is never claimed again. False as done() is."""

        return self._finish(item, "failed", error=error)

    def release(self, item: Item | str, delay: float | timedelta | None = None) -> bool:
        """Hand a held item back unfinished, and give its try back.

        The holder is saying the work did not happen — shutting down, rate limited, not ready yet. With
        no `delay` the item goes back to its own place in the pile, claimable at once. With one — seconds,
        or a timedelta — it is claimable only once the delay has passed, and queues by that time: an item
        that is not ready yet does not sit at the front of the pile, taken and handed back by every claim.
        """

        return self._handBack(item, returnTry=True, delay=_delay(delay))

    def _handBack(self, item: Item | str, *, returnTry: bool, delay: timedelta | None = None) -> bool:
        # back to its own place in the pile, not the end of it — or, delayed, due when the delay has passed
        due: Any = "$createdAt" if delay is None else utc_now() + delay
        back: dict[str, Any] = {"status": "pending", "claimId": None, "claimedAt": None, "leaseUntil": due}

        if returnTry:
            back["attempts"] = {"$subtract": ["$attempts", 1]}

        return self.collection.update_one(
            {**self._matching(item), "status": "running"},
            [{"$set": back}],
        ).matched_count > 0

    def renewLease(self, item: Item | str) -> bool:
        """Hold on to an item for another lease. False if this claim no longer holds it."""

        return self.collection.update_one(
            {**self._matching(item), "status": "running"},
            {"$set": {"leaseUntil": utc_now() + timedelta(seconds=self.settings.leaseSeconds)}}
        ).matched_count > 0

    def cancel(self, uid: str) -> bool:
        """Cancel an item nobody is working. False if it is being worked, or already finished."""

        return self.collection.update_one({"uid": uid, **notStarted()}, cancelled()).matched_count > 0

    def cancelMany(self, where: Mapping[str, Any] | None = None) -> int:
        """Cancel every item matching `where` that nobody is working. Returns how many were."""

        query = {"$and": [dict(where), notStarted()]} if where else notStarted()

        return self.collection.update_many(query, cancelled()).matched_count

    # --- the work() block ---

    @contextmanager
    def work(self, where: Mapping[str, Any] | None = None) -> Iterator[Work | None]:
        """Claim one item and hold it for as long as the block runs.

        The block ends in exactly one outcome: reaching its end marks the item done; raising marks it
        failed and re-raises; and `w.done()`, `w.fail()` or `w.release()` record that outcome and end
        the block there. The lease is renewed throughout, and every write checks the claim still holds
        the item — if it does not, nothing is written and it is logged. Yields None on an empty pile.
        """

        item = self.claim(where)

        if item is None:
            yield None
            return

        working = Work(self, item)

        with self.leases.holding(item.uid, item.claimId):
            try:
                yield working
            except _Ended as ended:
                if ended.claimId != item.claimId:
                    # another block's early end, on its way out through this one. Nobody said this item's
                    # work did not happen — the block may have done part of it — so it goes straight back
                    # to be claimed with its try spent, as a lapsed lease would leave it
                    if working.ended is None and not self._handBack(item, returnTry=False):
                        self._lostClaim(item)

                    raise
            except (Exception, SystemExit):     # sys.exit() in a block is a failure, not a dead worker
                self._close(working, "fail", traceback.format_exc())

                raise
            else:
                self._close(working, "done")

    def _close(self, working: Work, outcome: str, error: str | None = None):
        """End a block that reached its end or raised — unless it already wrote an outcome and carried on."""

        if working.ended is not None:
            # a bare `except:`, or a return in a finally, swallowed the end of the block; the outcome
            # stands (or, after w.confirm(), belongs to another claim), and what ran after it is past undoing
            logger.warning(
                "%r kept running after w.%s(); its outcome stands and nothing more was written",
                working.item, working.ended
            )

            return

        written = self.fail(working.item, error) if outcome == "fail" else self.done(working.item)

        if not written:
            self._lostClaim(working.item)

    def _lostClaim(self, item: Item):
        logger.warning(
            "%r finished after its claim was lost — cancelled, given up on, or taken over when its "
            "lease lapsed; this outcome was not recorded", item
        )

    # --- looking at the pile ---

    def count(self, where: Mapping[str, Any] | None = None, status: WorkStatus | None = None) -> int:
        query = dict(where) if where else {}

        if status:
            query["status"] = status

        return super().count(query)

    def counts(self) -> dict[str, int]:
        return {status: self.count(status=status) for status in ("pending", "running", "done", "failed", "canceled")}

    def purge(self, status: WorkStatus = "done") -> int:
        """Delete every item of one status."""

        return self.deleteMany({"status": status})
