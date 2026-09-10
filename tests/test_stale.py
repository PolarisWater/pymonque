"""Two ways work goes stale: a cold start after downtime, and a queue nobody keeps up with."""

import logging
import time
from datetime import timedelta

import pytest

from pymonque import BaseApp, task, utc_now


class App(BaseApp):
    @task
    @staticmethod
    def ping() -> str:
        return "pong"


class Skipping(App):
    overdueTaskPolicy = "skip"


def overdue(app, seconds=300, count=1):
    for _ in range(count):
        app.task.schedule(App.ping(), deadline=utc_now() - timedelta(seconds=seconds))


def counts(app):
    return {s: app.task.count({"status": s}) for s in ("pending", "success", "outdated")}


# --- coming back from downtime: overdueTaskPolicy, at init() ---

def test_a_cold_start_skips_what_piled_up_while_it_was_down(db):
    app = Skipping(db, backlogWarnAfter=None)
    overdue(app, count=3)

    app.init()

    assert counts(app) == {"pending": 0, "success": 0, "outdated": 3}


def test_execute_now_runs_the_backlog_instead(db):
    app = App(db, backlogWarnAfter=None)      # the default policy
    overdue(app, count=3)

    app.init()

    assert counts(app) == {"pending": 3, "success": 0, "outdated": 0}


def test_a_worker_joining_a_running_cluster_is_not_a_cold_start(db):
    """It would drop work its colleagues were about to run."""

    running = Skipping(db, taskPoolInterval=99, backlogWarnAfter=None)
    running.startWorkers(taskWorkers=1, schedulerWorkers=0)

    try:
        overdue(running, count=3)
        joining = Skipping(db, taskPoolInterval=99, backlogWarnAfter=None)
        joining.init()

        assert counts(running) == {"pending": 3, "success": 0, "outdated": 0}
    finally:
        running.stopWorkers(timeout=5)


def test_a_start_after_everyone_stopped_is_a_cold_start_again(db):
    first = Skipping(db, taskPoolInterval=99, backlogWarnAfter=None)
    first.startWorkers(taskWorkers=1, schedulerWorkers=0)
    first.stopWorkers(timeout=5)

    overdue(first, count=3)
    Skipping(db, backlogWarnAfter=None).init()

    assert counts(first)["outdated"] == 3


def test_cold_start_is_about_other_processes_not_this_one(db):
    running = Skipping(db, taskPoolInterval=99, backlogWarnAfter=None)
    running.startWorkers(taskWorkers=1, schedulerWorkers=0)

    try:
        assert running.coldStart() is True           # it is the only one
        assert running.otherLiveWorkers() == []

        joining = Skipping(db, backlogWarnAfter=None)
        joining.startWorkers(taskWorkers=1, schedulerWorkers=0)

        try:
            assert joining.coldStart() is False
            assert len(joining.otherLiveWorkers()) == 1
        finally:
            joining.stopWorkers(timeout=5)
    finally:
        running.stopWorkers(timeout=5)


def test_a_cold_start_says_what_it_dropped(db, caplog):
    app = Skipping(db, backlogWarnAfter=None)
    overdue(app, count=3)

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        app.init()

    assert "3 task(s) were due before this cold start" in caplog.text


# --- nobody keeping up: skipAfter, at the claim ---

@pytest.fixture
def app(db):
    return App(db, enforceVersion=False, backlogWarnAfter=None)


def run(app, **kwargs):
    seconds = kwargs.pop("late", 300)
    stored = app.task.schedule(App.ping(), deadline=utc_now() - timedelta(seconds=seconds), **kwargs)
    app.task._work()
    return app.task.get(stored.uid)


def test_without_a_limit_a_task_runs_however_late(app):
    assert app.task.skipAfter is None
    assert run(app, late=86400).status == "success"


def test_the_app_sets_the_outermost_limit(db):
    class Limited(App):
        taskSkipAfter = 60

    limited = Limited(db, enforceVersion=False, backlogWarnAfter=None)

    assert limited.task.skipAfter == 60
    assert run(limited, late=300).status == "outdated"
    assert run(limited, late=10).status == "success"


def test_the_engine_overrides_the_app(db):
    class Limited(App):
        taskSkipAfter = 86400

    limited = Limited(db, enforceVersion=False, backlogWarnAfter=None)
    limited.task.skipAfter = 60

    assert run(limited, late=300).status == "outdated"


def test_a_task_overrides_the_engine_in_both_directions(db):
    class Limited(App):
        taskSkipAfter = 60

    limited = Limited(db, enforceVersion=False, backlogWarnAfter=None)

    assert run(limited, late=300, skipAfter=86400).status == "success"
    assert run(limited, late=10, skipAfter=1).status == "outdated"


def test_an_outdated_task_records_how_late_it_was(app):
    app.task.skipAfter = 60
    finished = run(app, late=300)

    assert finished.status == "outdated"
    assert "300s after its deadline" in finished.error
    assert finished.result is None


def test_an_outdated_task_is_not_run(db):
    ran = []

    class Counting(BaseApp):
        taskSkipAfter = 60

        @task
        @staticmethod
        def note() -> str:
            ran.append(1)
            return "ran"

    app = Counting(db, enforceVersion=False, backlogWarnAfter=None)
    app.task.schedule(Counting.note(), deadline=utc_now() - timedelta(seconds=300))
    app.task._work()

    assert ran == []
    assert app.task.count({"status": "outdated"}) == 1


def test_a_scheduler_stamps_its_limit_onto_what_it_emits(app):
    stored = app.scheduler.add(
        App.ping(), app.distribution("constant", dailyFrequency=1), skipAfter=90
    )
    app.scheduler.collection.update_one(
        {"uid": stored.uid}, {"$set": {"deadline": utc_now(), "leaseUntil": utc_now()}}
    )
    app.scheduler._work()

    assert app.task.find()[0].skipAfter == 90


def test_the_two_are_independent(db):
    """A queue that goes stale while the app is up is not a cold start."""

    class Both(App):
        overdueTaskPolicy = "skip"
        taskSkipAfter = 60

    app = Both(db, enforceVersion=False, backlogWarnAfter=None)

    assert app.task.policy == "skip"
    assert app.task.skipAfter == 60


def test_a_limit_change_is_a_different_fingerprint(db):
    class Loose(App):
        taskSkipAfter = None

    class Tight(App):
        taskSkipAfter = 60

    assert Loose(db).fingerprint != Tight(db).fingerprint
