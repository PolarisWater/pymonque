"""Tasks and pile items record the same three moments: created, claimed and finished."""

import time
from datetime import timedelta

import pytest

from pymonque import BaseApp, Item, Task, pile, task, utc_now


class App(BaseApp):
    jobs = pile()

    @task
    @staticmethod
    def ok() -> str:
        return "ok"

    @task
    @staticmethod
    def broken() -> None:
        raise RuntimeError("nope")

    @task(timeout=0.05)
    @staticmethod
    def slow() -> None:
        time.sleep(0.3)

    @task(skipAfter=1)
    @staticmethod
    def stale() -> str:
        return "late"

    @task(maxAttempts=2, retryDelay=60)
    @staticmethod
    def retried() -> None:
        raise RuntimeError("nope")


@pytest.fixture
def app(db):
    return App(db, enforceVersion=False, backlogWarnAfter=None)


def test_tasks_and_items_have_the_same_three_moments():
    moments = {"createdAt", "claimedAt", "finishedAt"}

    assert moments <= set(Task.model_fields)
    assert moments <= set(Item.model_fields)


def test_a_new_task_has_only_been_created(app):
    before = utc_now()
    stored = app.task.get(app.task.schedule(App.ok()).uid)

    assert abs(stored.createdAt - before) < timedelta(seconds=1)
    assert stored.claimedAt is None
    assert stored.finishedAt is None


@pytest.mark.parametrize("name, status", [("ok", "success"), ("broken", "failed"), ("slow", "timeout")])
def test_a_run_task_records_its_claim_and_its_finish(app, name, status):
    stored = app.task.schedule(app.task(name))
    app.task._work()
    finished = app.task.get(stored.uid)

    assert finished.status == status
    assert finished.createdAt <= finished.claimedAt <= finished.finishedAt


def test_an_outdated_task_was_claimed_and_finished(app):
    stored = app.task.schedule(App.stale(), deadline=utc_now() - timedelta(seconds=300))
    app.task._work()
    finished = app.task.get(stored.uid)

    assert finished.status == "outdated"
    assert finished.claimedAt is not None and finished.finishedAt is not None


def test_a_task_waiting_for_a_retry_is_neither_claimed_nor_finished(app):
    stored = app.task.schedule(App.retried())
    app.task._work()
    waiting = app.task.get(stored.uid)

    assert waiting.status == "pending"
    assert waiting.claimedAt is None
    assert waiting.finishedAt is None


def test_a_canceled_task_finished_without_a_claim(app):
    stored = app.task.schedule(App.ok(), deadline=utc_now() + timedelta(hours=1))
    app.task.cancel(stored.uid)
    canceled = app.task.get(stored.uid)

    assert canceled.claimedAt is None
    assert canceled.finishedAt is not None


def test_a_task_given_up_on_after_a_crash_is_finished(app):
    stored = app.task.schedule(App.ok())
    app.task.collection.update_one({"uid": stored.uid}, {"$set": {
        "status": "processing", "attempts": 1, "leaseUntil": utc_now() - timedelta(seconds=1),
    }})
    app.task._work()

    assert app.task.get(stored.uid).finishedAt is not None


def test_an_incompatible_task_is_finished(app, db):
    stored = app.task.schedule(App.ok())

    class Smaller(BaseApp):
        @task
        @staticmethod
        def other() -> None:
            return None

    Smaller(db, enforceVersion=False).init()

    assert app.task.get(stored.uid).finishedAt is not None


def test_an_item_records_the_same_moments(app):
    added = app.jobs.add({"n": 1})
    claimed = app.jobs.claim()
    app.jobs.done(claimed)
    finished = app.jobs.get(added.uid)

    assert finished.createdAt <= finished.claimedAt <= finished.finishedAt
