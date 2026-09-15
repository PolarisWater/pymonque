"""Shared fixtures for the rebuilt package.

Every test gets a fresh in-memory MongoDB, so nothing leaks between tests.
"""

import threading

import pytest
from mongomock import MongoClient
from mongomock.collection import Collection

from pymonque_next import Task, TaskEngine, TaskLimits
from pymonque_next.settings import TaskEngineSettings
from pymonque_next.tasks import taskFunctions


_findAndModify = threading.Lock()


@pytest.fixture(autouse=True)
def atomicFindAndModify(monkeypatch):
    """Make mongomock's find_one_and_update atomic, as MongoDB's is.

    mongomock finds the document, then updates it by _id alone, so two threads can both take the same
    one. A claim is built on MongoDB's guarantee; without this, a test of claims across threads would
    test mongomock instead.
    """

    original = Collection._find_and_modify

    def atomic(self, *args, **kwargs):
        with _findAndModify:
            return original(self, *args, **kwargs)

    monkeypatch.setattr(Collection, "_find_and_modify", atomic)


@pytest.fixture
def db():
    return MongoClient()["pymonque_test"]


@pytest.fixture
def taskEngine(db):
    """Build a task engine the way an app will, from its tasks, every task's limits — none, unless given
    — and its lease."""

    def build(functions=None, model=Task, *, name="task", collection=None, leaseSeconds=300, limits=None, **kwargs):
        functions = functions or {}

        return TaskEngine(
            db[collection or f"pymonque_task_{name}"],
            model,
            name=name,
            functions=taskFunctions(functions),
            settings=TaskEngineSettings(leaseSeconds=leaseSeconds),
            limits=limits if limits is not None else {task: TaskLimits() for task in functions},
            **kwargs,
        )

    return build
