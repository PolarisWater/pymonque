"""Piles: items waiting to be claimed, drained by tasks."""

from __future__ import annotations

from typing import Annotated, Any, Generic, Self, TypeVar

from pydantic import Field, model_validator

from .documents import Document, UtcDatetime, WorkStatus, utc_now


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
