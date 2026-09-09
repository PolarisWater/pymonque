"""Task timeouts: app, engine, and per-task, with the nearest one winning."""

import logging
import time
from datetime import timedelta

import pytest

from pymonque import BaseApp, task, utc_now

from conftest import ExampleApp


class Slow(BaseApp):
    @task
    @staticmethod
    def sleep(seconds: float = 0.05) -> str:
        time.sleep(seconds)
        return "finished"

    @task
    @staticmethod
    def boom() -> None:
        raise ValueError("nope")


@pytest.fixture
def slow(db):
    return Slow(db, enforceVersion=False)


def run(app, spec, **kwargs):
    stored = app.task.schedule(spec, **kwargs)
    app.task._work()
    return app.task.get(stored.uid)


# --- no timeout is still the default ---

def test_a_task_has_no_time_limit_by_default(slow):
    assert slow.task.timeout is None
    assert run(slow, Slow.sleep(seconds=0.2)).status == "success"


# --- each level ---

def test_the_app_sets_the_outermost_limit(db):
    class Limited(Slow):
        taskTimeout = 0.05

    app = Limited(db, enforceVersion=False)

    assert app.task.timeout == 0.05
    assert run(app, Slow.sleep(seconds=0.5)).status == "timeout"


def test_the_engine_overrides_the_app(db):
    class Limited(Slow):
        taskTimeout = 5.0

    app = Limited(db, enforceVersion=False)
    app.task.timeout = 0.05

    assert run(app, Slow.sleep(seconds=0.5)).status == "timeout"


def test_a_task_overrides_the_engine_in_both_directions(db):
    class Limited(Slow):
        taskTimeout = 0.05

    app = Limited(db, enforceVersion=False)

    assert run(app, Slow.sleep(seconds=0.3), timeout=5).status == "success"   # looser
    assert run(app, Slow.sleep(seconds=0.3), timeout=0.01).status == "timeout"  # nearer


def test_a_scheduler_stamps_its_timeout_onto_what_it_emits(slow):
    stored = slow.scheduler.add(
        Slow.sleep(), slow.distribution("constant", dailyFrequency=1), timeout=0.05
    )
    slow.scheduler.collection.update_one(          # bring it due
        {"uid": stored.uid},
        {"$set": {"deadline": utc_now(), "leaseUntil": utc_now()}},
    )
    slow.scheduler._work()

    assert slow.task.find()[0].timeout == 0.05


# --- what a timeout records ---

def test_a_timed_out_task_records_why(slow):
    finished = run(slow, Slow.sleep(seconds=0.5), timeout=0.05)

    assert finished.status == "timeout"
    assert "did not finish within" in finished.error
    assert finished.result is None
    assert finished.executionTime >= timedelta(seconds=0.05)


def test_a_timeout_warns(slow, caplog):
    with caplog.at_level(logging.WARNING, logger="pymonque"):
        run(slow, Slow.sleep(seconds=0.5), timeout=0.05)

    assert "timed out" in caplog.text


def test_a_raise_inside_a_timed_task_is_still_a_failure(slow):
    finished = run(slow, Slow.boom(), timeout=5)

    assert finished.status == "failed"
    assert "ValueError" in finished.error


def test_a_timeout_frees_the_worker_for_the_next_task(slow):
    slow.task.schedule(Slow.sleep(seconds=5), timeout=0.05)
    quick = slow.task.schedule(Slow.sleep(seconds=0.01))

    slow.task._work()
    slow.task._work()

    assert slow.task.get(quick.uid).status == "success"


# --- it is shared behaviour ---

def test_a_timeout_change_is_a_different_fingerprint(db):
    class Loose(Slow):
        taskTimeout = None

    class Tight(Slow):
        taskTimeout = 1.0

    assert Loose(db).fingerprint != Tight(db).fingerprint
