"""Shared fixtures.

Every test gets a fresh in-memory MongoDB, so nothing leaks between tests.
"""

import time

import pytest
from mongomock import MongoClient
from pymongo import UpdateOne
from pydantic import BaseModel

from pymonque import BaseApp, task, pile


WORKER_POOL_INTERVAL = 0.02


def wait_for(predicate, timeout: float = 3.0, interval: float = 0.01) -> bool:
    """Poll until `predicate` holds, instead of sleeping a fixed amount."""

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)

    return bool(predicate())


def _bulkWriteSupported() -> bool:
    collection = MongoClient()["probe"]["probe"]
    collection.insert_one({"uid": "probe"})

    try:
        collection.bulk_write([UpdateOne({"uid": "probe"}, {"$set": {"n": 1}})])
    except Exception:
        return False

    return True


# mongomock 4.3 cannot consume a pymongo 4.17 UpdateOne, so the one branch that
# uses bulk_write (SchedulerEngine.init under the "skip" policy) is unrunnable
# here. The test is written anyway and runs as soon as the stack allows it.
requiresBulkWrite = pytest.mark.skipif(
    not _bulkWriteSupported(),
    reason="mongomock cannot run bulk_write with this pymongo version",
)


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
