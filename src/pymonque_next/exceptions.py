"""What pymonque raises. A bad declaration raises pydantic's ValidationError, naming the setting."""


class TaskNotFound(Exception):
    """The app has no task of that name."""


class TaskValidationError(Exception):
    """A call's arguments do not fit its task's signature."""


class DistributionNotFound(Exception):
    """The registry has no distribution of that name."""


class DistributionValidationError(Exception):
    """A call's arguments do not fit its distribution, or it did not give a positive interval."""


class TaskTimeout(Exception):
    """A task outlived its timeout."""


class VersionMismatch(Exception):
    """A live worker is running a different version of the app."""


class UnboundDocument(Exception):
    """A document asked to save, delete or reload itself has never been stored or fetched."""


class TaskStopped(BaseException):
    """Raised inside a task's call when its timeout ran out, to stop it.

    A BaseException, so `except Exception:` in the task does not swallow it. It stops Python code only:
    a call blocked in C, I/O or a sleep goes on until it returns, and is then abandoned.
    """
