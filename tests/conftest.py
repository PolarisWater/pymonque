"""Shared fixtures.

Every test gets a fresh in-memory MongoDB, so nothing leaks between tests. The engine fixtures build
each engine the way an app will, from what the app resolves for it.
"""

import threading

import pytest
from mongomock import MongoClient
from mongomock.collection import Collection

from pymonque import PileEngine, Scheduler, SchedulerEngine, Task, TaskEngine, TaskLimits
from pymonque.settings import PileSettings, SchedulerEngineSettings, TaskEngineSettings
from pymonque.tasks import taskFunctions


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
    """Build a task engine from its tasks, every task's limits — none, unless given — and its lease."""

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


@pytest.fixture
def pileEngine(db):
    """Build a pile engine from its payload model — none takes any dict — and its settings."""

    def build(model=None, *, name="jobs", collection=None, maxAttempts=1, leaseSeconds=300, **kwargs):
        return PileEngine(
            db[collection or f"pymonque_pile_{name}"],
            model,
            name=name,
            settings=PileSettings(maxAttempts=maxAttempts, leaseSeconds=leaseSeconds),
            **kwargs,
        )

    return build


@pytest.fixture
def schedulerEngine(db):
    """Build a scheduler engine from its model and settings, emitting into the task engine it is given."""

    def build(model=Scheduler, *, tasks, name="scheduler", collection=None, missed="once", leaseSeconds=300, **kwargs):
        return SchedulerEngine(
            db[collection or f"pymonque_scheduler_{name}"],
            model,
            name=name,
            tasks=tasks,
            settings=SchedulerEngineSettings(missed=missed, leaseSeconds=leaseSeconds),
            **kwargs,
        )

    return build


@pytest.fixture
def stopAfter():
    """Stop every app a test started, so its threads do not go on logging into later tests."""

    started = []

    def register(app):
        started.append(app)

        return app

    yield register

    for app in started:
        app.stopWorkers(timeout=5)
        app.restoreSignals()
