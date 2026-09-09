class TaskValidationError(Exception):
    def __init__(self, message: str | None = None):
        super().__init__(message)

class TaskNotFound(Exception):
    def __init__(self, message: str | None = None):
        super().__init__(message)


class DistributionValidationError(Exception):
    def __init__(self, message: str | None = None):
        super().__init__(message)

class DistributionNotFound(Exception):
    def __init__(self, message: str | None = None):
        super().__init__(message)

class VersionMismatch(Exception):
    def __init__(self, message: str | None = None):
        super().__init__(message)


class UnboundDocument(Exception):
    def __init__(self, message: str | None = None):
        super().__init__(message)

class TaskTimeout(Exception):
    def __init__(self, message: str | None = None):
        super().__init__(message)
