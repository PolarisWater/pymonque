"""Schedulers: a recurring schedule that emits a task on every beat."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import model_validator

from .calls import CallSpec
from .documents import UtcDatetime
from .tasks import TaskFactory


# being worked is a lease, not a status: `status` says whether the scheduler emits, nothing else
SchedulerStatus = Literal["enabled", "disabled"]


class Scheduler(TaskFactory):
    """A call emitted as a task on a rhythm drawn from a distribution.

    Schedulers are assumed never to fail: emitting is a database write. Subclass it to store fields of
    your own on every scheduler of an engine.
    """

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

    def __repr__(self) -> str:
        return f"Scheduler {self.name}: {self.work!r}"
