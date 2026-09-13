"""Shared fixtures.

Every test gets a fresh in-memory MongoDB, so nothing leaks between tests.
"""

import time

import pytest
from mongomock import MongoClient
from pydantic import BaseModel

from pymonque import BaseApp, task, pile


WORKER_POLL_INTERVAL = 0.02


def wait_for(predicate, timeout: float = 3.0, interval: float = 0.01) -> bool:
    """Poll until `predicate` holds, instead of sleeping a fixed amount."""

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)

    return bool(predicate())


def appWith(base, db, **policies):
    """Policies live on the class, so varying one for a test means a subclass."""

    return type("Configured", (base,), policies)(db, **policies.pop("_kwargs", {}))


class Email(BaseModel):
    to:         str
    subject:    str = "(no subject)"


class ExampleApp(BaseApp):
    outbox = pile(Email)
    scraps = pile()

    @task
    @staticmethod
    def greet(name: str, greeting: str = "Hello") -> str:
        return f"{greeting}, {name}!"

    @task
    @staticmethod
    def boom():
        raise ValueError("nope")

    @task
    @staticmethod
    def unserializable():
        return object()  # not encodable by the driver

    @task
    def whoami(self) -> str:
        return type(self).__name__

    @task
    def send_one(self) -> str:
        with self.outbox.work() as item:
            if item is None:
                return "empty"

            return f"sent to {item.data.to}"


@pytest.fixture
def db():
    return MongoClient()["pymonque_test"]


@pytest.fixture
def app(db):
    return ExampleApp(db)


@pytest.fixture
def tasks(app):
    return app.task.tasksCollection


@pytest.fixture
def schedulers(app):
    return app.scheduler.schedulersCollection
