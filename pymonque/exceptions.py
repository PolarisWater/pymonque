class TaskValidationError(Exception):
    def __init__(self, message):
        super().__init__(message)

class TaskNotFound(Exception):
    def __init__(self, message):
        super().__init__(message)


class DistributionValidationError(Exception):
    def __init__(self, message):
        super().__init__(message)

class DistributionNotFound(Exception):
    def __init__(self, message):
        super().__init__(message)