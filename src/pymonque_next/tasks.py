"""Tasks: a call due at a time, run at most once."""

from __future__ import annotations

from typing import Any, Literal, Self

from pydantic import Field, model_validator

from .calls import CallSpec
from .documents import Document, Duration, UtcDatetime, WorkStatus, utc_now


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
    by, such as an account. The fields are data only.
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

    def __repr__(self) -> str:
        return f"Task {self.work!r} from {self.factory!r}"
