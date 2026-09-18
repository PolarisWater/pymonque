"""Worker threads: how many run on which engine, draining tasks without running one twice, schedulers
that keep emitting into their engine, the whole loop from a scheduler to a pile, pacing — a backlog
drains without waiting, an idle worker waits — and a failing iteration that is logged and survived."""

import logging
import threading
import time
from datetime import timedelta

import pytest
from pydantic import BaseModel

from pymonque import BaseApp, pile, schedulers, task, tasks, utc_now
from pymonque.claims import WorkerLoop

from tests.helpers import waitFor


POLL = 0.02


class Email(BaseModel):
    to: str


class Busy(BaseApp):
    outbox = pile(Email)
    heavy = tasks()
    nightly = schedulers(emitsInto=heavy)

    @task
    @staticmethod
    def greet(name: str) -> str:
        return f"Hello, {name}!"

    @task
    def sendOne(self) -> str:
        with self.outbox.work() as w:
            if w is None:
                return "empty"

            return f"sent to {w.data.to}"


@pytest.fixture
def busy(db, stopAfter):
    return stopAfter(Busy(db, taskPollInterval=POLL, schedulerPollInterval=POLL, backlogWarnAfter=None))


def everyFiftyMs(app):
    return app.distribution("constant", dailyFrequency=86400 / 0.05)


def dueNow(engine, scheduler):
    engine.update(scheduler.uid, deadline=utc_now())


# --- how many, where ---

def test_start_workers_defaults_to_none(busy):
    busy.task.schedule(Busy.greet(name="Ada"))
    busy.startWorkers()
    time.sleep(POLL * 5)

    assert not busy.running
    assert busy.task.count({"status": "pending"}) == 1


def test_an_int_is_that_many_threads_on_every_engine_of_the_kind(busy):
    busy.startWorkers(taskWorkers=2, schedulerWorkers=1)

    assert {name: e.workers.count for name, e in busy.taskEngines.items()} == {"task": 2, "heavy": 2}
    assert {name: e.workers.count for name, e in busy.schedulerEngines.items()} == {"scheduler": 1, "nightly": 1}


def test_a_dict_sets_engines_by_name_and_leaves_the_rest_without(busy):
    busy.startWorkers(taskWorkers={"heavy": 3}, schedulerWorkers={"nightly": 1})

    assert (busy.task.workers.count, busy.heavy.workers.count) == (0, 3)
    assert (busy.scheduler.workers.count, busy.nightly.workers.count) == (0, 1)


def test_a_dict_naming_an_engine_the_app_does_not_have_is_refused(busy):
    with pytest.raises(ValueError, match="taskWorkers names light, which this app has no engine of"):
        busy.startWorkers(taskWorkers={"light": 1})


@pytest.mark.parametrize("given", [-1, 1.5, "2", True])
def test_a_worker_count_that_is_not_a_number_of_threads_is_refused(busy, given):
    with pytest.raises((TypeError, ValueError)):
        busy.startWorkers(taskWorkers=given)


def test_worker_threads_are_named_after_their_engine(busy):
    busy.startWorkers(taskWorkers={"heavy": 1})

    assert [t.name for t in busy.heavy.workers.threads] == ["pymonque-task-heavy-0"]


# --- draining ---

def test_task_workers_drain_every_task_engine(busy):
    for n in range(5):
        busy.task.schedule(Busy.greet(name=str(n)))
        busy.heavy.schedule(Busy.greet(name=str(n)))

    busy.startWorkers(taskWorkers=3)

    assert waitFor(lambda: busy.task.count({"status": "done"}) == 5 and busy.heavy.count({"status": "done"}) == 5)


def test_workers_never_run_a_task_twice(busy):
    runs = []

    class Counted(Busy):
        @task
        @staticmethod
        def once() -> None:
            runs.append(1)

    app = Counted(busy.db, taskPollInterval=POLL, backlogWarnAfter=None)

    try:
        app.task.schedule(Counted.once())
        app.startWorkers(taskWorkers=4)

        assert waitFor(lambda: app.task.count({"status": "done"}) == 1)
        time.sleep(POLL * 5)
    finally:
        app.stopWorkers(timeout=5)

    assert len(runs) == 1


def test_a_scheduler_worker_keeps_emitting_into_its_engine(busy):
    dueNow(busy.nightly, busy.nightly.add(Busy.greet(name="Ada"), everyFiftyMs(busy)))

    busy.startWorkers(schedulerWorkers={"nightly": 1})

    assert waitFor(lambda: busy.heavy.count() >= 3)
    assert busy.task.count() == 0


def test_a_scheduler_feeds_a_task_that_drains_a_pile(busy):
    busy.outbox.addMany([{"to": f"user{n}@x.y"} for n in range(4)])
    dueNow(busy.scheduler, busy.scheduler.add(Busy.sendOne(), everyFiftyMs(busy)))

    busy.startWorkers(taskWorkers=2, schedulerWorkers=1)

    assert waitFor(lambda: busy.outbox.count(status="done") == 4)
    assert busy.task.count({"status": "failed"}) == 0


# --- pacing ---

def test_a_backlog_drains_without_waiting_between_tasks(db, stopAfter):
    app = stopAfter(Busy(db, taskPollInterval=5, backlogWarnAfter=None))    # a sleep this long would show

    for n in range(20):
        app.task.schedule(Busy.greet(name=str(n)))

    app.startWorkers(taskWorkers={"task": 1})

    assert waitFor(lambda: app.task.count({"status": "done"}) == 20, timeout=4)


def test_an_idle_worker_waits_then_takes_new_work(busy):
    busy.startWorkers(taskWorkers={"task": 1})
    time.sleep(POLL * 3)

    busy.task.schedule(Busy.greet(name="late"))

    assert waitFor(lambda: busy.task.count({"status": "done"}) == 1)


def test_work_reports_whether_it_did_anything(busy):
    assert busy.task.work() is None
    assert busy.scheduler.work() is None

    busy.task.schedule(Busy.greet(name="Ada"))
    dueNow(busy.scheduler, busy.scheduler.add(Busy.greet(name="Ada"), busy.distribution("constant", dailyFrequency=24)))

    assert busy.task.work() is not None
    assert busy.scheduler.work() is not None


# --- a failing iteration ---

def flakyWork(calls, park):
    def work():
        calls.append(1)

        if len(calls) == 1:
            raise RuntimeError("bang")

        if len(calls) > 3:
            park.wait(5)

    return work


def test_a_worker_survives_a_failing_iteration():
    calls, park = [], threading.Event()
    loop = WorkerLoop(flakyWork(calls, park), name="flaky", pollInterval=POLL)

    loop.start(1)

    try:
        assert waitFor(lambda: len(calls) >= 3)     # it went on after the exception
    finally:
        park.set()
        loop.stop(timeout=5)


def test_a_failing_iteration_is_logged_naming_the_engine(caplog):
    park = threading.Event()

    def work():
        park.wait(5) if caplog.records else None
        raise RuntimeError("bang")

    loop = WorkerLoop(work, name="task-heavy", pollInterval=POLL)

    with caplog.at_level(logging.ERROR, logger="pymonque"):
        loop.start(1)

        try:
            assert waitFor(lambda: "bang" in caplog.text)
        finally:
            park.set()
            loop.stop(timeout=5)

    assert "task-heavy worker iteration failed" in caplog.text


def test_a_stored_scheduler_that_no_longer_fits_its_model_is_logged_and_the_worker_goes_on(busy, caplog):
    scheduler = busy.scheduler.add(Busy.greet(name="Ada"), busy.distribution("constant", dailyFrequency=24))
    busy.scheduler.collection.update_one({"uid": scheduler.uid}, {"$set": {"deadline": "not a time", "leaseUntil": utc_now()}})
    busy.task.schedule(Busy.greet(name="Bob"))

    with caplog.at_level(logging.ERROR, logger="pymonque"):
        busy.startWorkers(taskWorkers={"task": 1}, schedulerWorkers={"scheduler": 1})

        assert waitFor(lambda: "scheduler-scheduler worker iteration failed" in caplog.text)

    assert waitFor(lambda: busy.task.count({"status": "done"}) == 1)
    assert busy.scheduler.workers.running


def test_workers_can_be_started_again_after_a_stop(busy):
    busy.startWorkers(taskWorkers={"task": 1})
    busy.stopWorkers(timeout=5)

    assert busy.task.workers.count == 0

    busy.task.schedule(Busy.greet(name="again"))
    busy.startWorkers(taskWorkers={"task": 1})

    assert waitFor(lambda: busy.task.count({"status": "done"}) == 1)


def test_an_engine_reports_its_own_state(busy):
    assert not busy.task.workers.running and not busy.task.workers.stopping

    busy.startWorkers(taskWorkers={"task": 1})
    assert busy.task.workers.running

    busy.task.workers.stop(timeout=3)
    assert busy.task.workers.stopping and not busy.task.workers.running
