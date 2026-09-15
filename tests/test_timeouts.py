"""Task timeouts: declared on @task, with the app's taskTimeout for the tasks that don't."""

import logging
import time
from datetime import timedelta

import pytest

from pymonque import BaseApp, task, utc_now


class Slow(BaseApp):
    @task
    @staticmethod
    def sleep(seconds: float = 0.05) -> str:
        time.sleep(seconds)
        return "finished"

    @task(timeout=0.05)
    @staticmethod
    def limited(seconds: float = 0.5) -> str:
        time.sleep(seconds)
        return "finished"

    @task(timeout=5)
    @staticmethod
    def boom() -> None:
        raise ValueError("nope")


@pytest.fixture
def slow(db):
    return Slow(db, enforceVersion=False, backlogWarnAfter=None)


def run(app, spec):
    stored = app.task.schedule(spec)
    app.task._work()
    return app.task.get(stored.uid)


# --- where a limit comes from ---

def test_a_task_has_no_time_limit_by_default(slow):
    assert slow.task.limits["sleep"].timeout is None
    assert run(slow, Slow.sleep(seconds=0.2)).status == "success"


def test_a_task_declares_its_own(slow):
    assert run(slow, Slow.limited()).status == "timeout"


def test_the_app_default_applies_to_a_bare_task(db):
    class Limited(Slow):
        taskTimeout = 0.05

    app = Limited(db, enforceVersion=False, backlogWarnAfter=None)

    assert run(app, Slow.sleep(seconds=0.5)).status == "timeout"


def test_a_task_declares_itself_looser_than_the_default(db):
    class Limited(BaseApp):
        taskTimeout = 0.05

        @task(timeout=5)
        @staticmethod
        def generous() -> str:
            time.sleep(0.2)
            return "finished"

        @task(timeout=None)
        @staticmethod
        def unlimited() -> str:
            time.sleep(0.2)
            return "finished"

    app = Limited(db, enforceVersion=False, backlogWarnAfter=None)

    assert run(app, Limited.generous()).status == "success"
    assert run(app, Limited.unlimited()).status == "success"


def test_an_emitted_task_times_out_by_its_tasks_limit(slow):
    scheduler = slow.scheduler.add(Slow.limited(), slow.distribution("constant", dailyFrequency=1))
    now = utc_now()
    slow.scheduler.collection.update_one({"uid": scheduler.uid}, {"$set": {"deadline": now, "leaseUntil": now}})

    slow.scheduler._work()
    slow.task._work()

    assert slow.task.find()[0].status == "timeout"


# --- what a timeout records ---

def test_a_timed_out_task_records_why(slow):
    finished = run(slow, Slow.limited())

    assert finished.status == "timeout"
    assert "did not finish within" in finished.error
    assert finished.result is None
    assert finished.executionTime >= timedelta(seconds=0.05)


def test_a_timeout_warns(slow, caplog):
    with caplog.at_level(logging.WARNING, logger="pymonque"):
        run(slow, Slow.limited())

    assert "timed out" in caplog.text


def test_a_raise_inside_a_timed_task_is_still_a_failure(slow):
    finished = run(slow, Slow.boom())

    assert finished.status == "failed"
    assert "ValueError" in finished.error


def test_a_timeout_frees_the_worker_for_the_next_task(slow):
    slow.task.schedule(Slow.limited(seconds=5))
    quick = slow.task.schedule(Slow.sleep(seconds=0.01))

    slow.task._work()
    slow.task._work()

    assert slow.task.get(quick.uid).status == "success"
