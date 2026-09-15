"""What each shared setting may be, the defaults an app declares, and the marker for a setting left out.

Every declaration and the app's defaults are checked against the same types, so they all refuse the
same values. These settings are shared behaviour — every process running an app must agree on them —
while pacing (poll intervals, worker counts) belongs to each process.
"""

from __future__ import annotations

from typing import Annotated, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field


Seconds     = Annotated[float, Field(gt=0)]
Timeout     = Annotated[float, Field(gt=0)] | None      # seconds a call may run; None: no limit
SkipAfter   = Annotated[float, Field(ge=0)] | None      # seconds past its deadline still worth running; None: however late
MaxAttempts = Annotated[int, Field(ge=1)]               # claims of an item whose outcome never came back

# what a scheduler owes for beats that went by unworked: none of them, one, or every one
Missed = Literal["skip", "once", "replay"]

LEASE_SECONDS = 300.0   # how long a claim is held before anyone may take the work over


class Unset:
    """A setting left out, as opposed to one given as None.

    Leaving a setting out is the only way to take the app's default. None is a value in its own
    right, and only means something where there is a limit to lift: a timeout or a skipAfter.
    """

    def __repr__(self) -> str:
        return "<the app's default>"


UNSET: Any = Unset()


def orDefault(value: Any, default: Any) -> Any:
    """A declared setting, or the default when it was left out."""

    return default if value is UNSET else value


class Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class TaskLimits(Frozen):
    """What a task may do: declared on @task, filled in from the app's defaults.

    Limits describe the function — how long it may run, how late it is still worth running — not one
    call of it, so they live in code and are never stored on a task.
    """

    timeout:        Timeout         = None
    skipAfter:      SkipAfter       = None


class TaskEngineSettings(Frozen):
    """A task engine's shared settings, resolved from its declaration and the app's defaults."""

    leaseSeconds:   Seconds


class SchedulerEngineSettings(Frozen):
    """A scheduler engine's shared settings, resolved from its declaration and the app's defaults."""

    missed:         Missed
    leaseSeconds:   Seconds


class PileSettings(Frozen):
    """A pile's shared settings, resolved from its declaration and the app's defaults."""

    maxAttempts:    MaxAttempts
    leaseSeconds:   Seconds


class AppDefaults(Frozen):
    """The defaults an app class declares, for every declaration that leaves the setting out.

    Named `<kind><Setting>`, after the declaration they fill in.
    """

    taskTimeout:            Timeout     = None
    taskSkipAfter:          SkipAfter   = None
    taskLeaseSeconds:       Seconds     = LEASE_SECONDS

    schedulerMissed:        Missed      = "once"
    schedulerLeaseSeconds:  Seconds     = LEASE_SECONDS

    # one try by default: an item whose holder died is given up rather than handed to the next
    # worker, since nobody knows how far it got
    pileMaxAttempts:        MaxAttempts = 1
    pileLeaseSeconds:       Seconds     = LEASE_SECONDS

    # names an app may still carry from 2.0, refused rather than silently ignored
    retired: ClassVar[dict[str, str]] = {
        "taskMaxAttempts":  "tasks are not retried; a task that must succeed retries inside its own code",
        "taskRetryDelay":   "tasks are not retried, so there is no retry delay",
        "itemMaxAttempts":  "renamed pileMaxAttempts",
        "itemRetryDelay":   "items have no retry delay: fail() is final, and release() hands an item back at once",
    }

    @classmethod
    def of(cls, app: type) -> AppDefaults:
        """The defaults a class declares, checked: a bad one raises ValidationError naming it."""

        for name, reason in cls.retired.items():
            if hasattr(app, name):
                raise TypeError(f"{app.__name__}.{name} is no longer a default: {reason}")

        return cls.model_validate({name: getattr(app, name) for name in cls.model_fields if hasattr(app, name)})
