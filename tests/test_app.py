"""BaseApp: wiring the engines together, and the worker threads."""

import time
from datetime import timedelta

import pytest

from pymonque import (
    BaseApp, BaseDistributions, DistributionEngine, TaskEngine, SchedulerEngine,
    PileEngine, Task, task, pile, utc_now,
)

from conftest import ExampleApp, WORKER_POLL_INTERVAL, wait_for


@pytest.fixture
def fast(db):
    """An app whose workers poll fast enough to test, stopped afterwards so its
    threads don't go on logging into later tests."""

    app = ExampleApp(
        db,
        taskPollInterval=WORKER_POLL_INTERVAL,
        schedulerPollInterval=WORKER_POLL_INTERVAL,
    )
    yield app
    app.stopWorkers(timeout=5)


# --- wiring ---

def test_the_engines_are_built(app):
    assert isinstance(app.task, TaskEngine)
    assert isinstance(app.scheduler, SchedulerEngine)
    assert isinstance(app.distribution, DistributionEngine)
    assert all(isinstance(p, PileEngine) for p in app.piles.values())


def test_the_default_collection_names(app):
    assert app.tasksCollection.name == "pymonque_tasks"
    assert app.schedulersCollection.name == "pymonque_schedulers"


def test_the_engines_share_the_app_collections(app):
    assert app.task.tasksCollection is app.tasksCollection
    assert app.scheduler.schedulersCollection is app.schedulersCollection
    assert app.scheduler.taskEngine is app.task


def test_the_task_registry_holds_callables(app):
    assert set(app.task.functions) == {"greet", "boom", "unserializable", "whoami", "send_one"}
    assert app.task.functions["greet"](name="Ada") == "Hello, Ada!"
    assert app.task.functions["whoami"]() == "ExampleApp"  # already bound


def test_the_default_factory(app):
    assert app.defaultFactory.name == "default"
    assert app.task.defaultFactory is app.defaultFactory


def test_two_apps_on_one_database_share_state(db):
    producer = ExampleApp(db)
    consumer = ExampleApp(db)

    producer.task.schedule(ExampleApp.greet(name="Ada"))
    consumer.task._work()

    assert Task.model_validate(db["pymonque_tasks"].find_one()).status == "success"


def test_separate_databases_stay_separate(db):
    other = db.client["another"]
    ExampleApp(db).task.schedule(ExampleApp.greet(name="Ada"))

    assert ExampleApp(other).task.tasksCollection.count_documents({}) == 0


def test_a_app_with_no_tasks_or_piles_still_starts(db):
    class Empty(BaseApp):
        pass

    app = Empty(db)

    assert app.task.functions == {}
    assert app.piles == {}


# --- workers ---

def test_task_workers_drain_the_tasks(fast, db):
    for n in range(5):
        fast.task.schedule(fast.task("greet", name=str(n)))

    fast.startWorkers(taskWorkers=3, schedulerWorkers=0)

    assert wait_for(lambda: db["pymonque_tasks"].count_documents({"status": "success"}) == 5)


def test_workers_never_run_a_task_twice(fast, db):
    fast.task.schedule(ExampleApp.greet(name="Ada"))
    fast.startWorkers(taskWorkers=4, schedulerWorkers=0)
    wait_for(lambda: db["pymonque_tasks"].count_documents({"status": "success"}) == 1)
    time.sleep(WORKER_POLL_INTERVAL * 3)

    assert db["pymonque_tasks"].count_documents({}) == 1
    assert Task.model_validate(db["pymonque_tasks"].find_one()).executionTime is not None


def test_a_scheduler_worker_keeps_emitting(fast, db):
    fast.scheduler.add(
        ExampleApp.greet(name="Ada"),
        fast.distribution("constant", dailyFrequency=86400 / 0.05),  # every 50ms
    )
    fast.schedulersCollection.update_many({}, {"$set": {"deadline": utc_now()}})
    fast.startWorkers(taskWorkers=0, schedulerWorkers=1)

    assert wait_for(lambda: db["pymonque_tasks"].count_documents({}) >= 3)


def test_a_scheduler_feeds_a_pile_draining_task(fast, db):
    """The whole loop: scheduler fires a task, the task claims one pile item."""

    fast.outbox.addMany([{"to": f"user{n}@x.y"} for n in range(4)])
    fast.scheduler.add(
        ExampleApp.send_one(),
        fast.distribution("constant", dailyFrequency=86400 / 0.05),
    )
    fast.schedulersCollection.update_many({}, {"$set": {"deadline": utc_now()}})
    fast.startWorkers(taskWorkers=2, schedulerWorkers=1)

    assert wait_for(lambda: fast.outbox.count(status="done") == 4)
    assert fast.outbox.counts() == {"pending": 0, "claimed": 0, "done": 4, "failed": 0}
    assert db["pymonque_tasks"].count_documents({"status": "failed"}) == 0


def test_startWorkers_defaults_to_no_workers(app):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    app.startWorkers()
    time.sleep(WORKER_POLL_INTERVAL * 2)

    assert app.task.tasksCollection.count_documents({"status": "pending"}) == 1


@pytest.mark.parametrize("engine", ["task", "scheduler"])
def test_a_worker_survives_a_failing_iteration(app, engine, monkeypatch):
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("bang")
        if len(calls) > 3:
            time.sleep(60)  # park the thread; the loop has no stop signal

    target = getattr(app, engine)
    target.pollInterval = WORKER_POLL_INTERVAL
    monkeypatch.setattr(target, "_work", flaky)
    target.startWorkers(1)

    assert wait_for(lambda: len(calls) >= 3)  # kept going after the exception


@pytest.mark.parametrize("engine", ["task", "scheduler"])
def test_a_failing_iteration_is_logged(app, engine, monkeypatch, caplog):
    import logging

    target = getattr(app, engine)
    target.pollInterval = 60  # one iteration is enough
    monkeypatch.setattr(target, "_work", lambda: (_ for _ in ()).throw(RuntimeError("bang")))

    with caplog.at_level(logging.ERROR, logger="pymonque"):
        target.startWorkers(1)
        wait_for(lambda: "bang" in caplog.text)

    assert "bang" in caplog.text


# --- construction options ---

def test_poll_intervals_reach_the_engines(db):
    app = ExampleApp(db, taskPollInterval=2, schedulerPollInterval=3)

    assert app.task.pollInterval == 2
    assert app.scheduler.pollInterval == 3


def test_policies_reach_the_engines(db):
    class App(ExampleApp):
        overdueSchedulersPolicy = "execute once"
        staleItemsPolicy        = "fail"

    app = App(db)

    assert app.scheduler.policy == "execute once"
    assert app.outbox.policy == "fail"


def test_a_custom_distribution_registry_reaches_the_engines(db):
    class Custom(BaseDistributions):
        @staticmethod
        def fixed(dailyFrequency: float) -> timedelta:
            return timedelta(seconds=1)

    app = ExampleApp(db, distributionsRegistry=Custom)

    assert app.task.distributionEngine is app.distribution
    assert "fixed" in app.distribution.functions


def test_an_app_exposes_its_database(db):
    app = ExampleApp(db)

    assert app.db is db
