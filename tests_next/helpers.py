"""Helpers the rebuilt package's tests share."""

import time


def waitFor(predicate, timeout: float = 3.0, interval: float = 0.01) -> bool:
    """Poll until `predicate` holds, instead of sleeping a fixed amount."""

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if predicate():
            return True

        time.sleep(interval)

    return bool(predicate())
