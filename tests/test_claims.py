"""A pile item can only be finished by the claim that currently holds it."""

import logging
from datetime import timedelta

import pytest

from pymonque import BaseApp, pile, task, utc_now


class App(BaseApp):
    jobs = pile(maxAttempts=3)      # so a second worker may take over a lapsed claim


@pytest.fixture
def app(db):
    return App(db, enforceVersion=False, backlogWarnAfter=None)


def lapse(app, item):
    app.jobs.collection.update_one(
        {"uid": item.uid}, {"$set": {"leaseUntil": utc_now() - timedelta(seconds=1)}}
    )


def takenOver(app):
    """The first claim's lease lapses and a second worker claims the same item."""

    app.jobs.add({"n": 1})
    first = app.jobs.claim()
    lapse(app, first)
    second = app.jobs.claim()

    assert second.uid == first.uid and second.claimId != first.claimId
    return first, second


def test_every_claim_gets_its_own_id(app):
    app.jobs.add({"n": 1})
    item = app.jobs.claim()

    assert item.claimId is not None


def test_a_stale_holder_cannot_finish_the_item(app):
    first, second = takenOver(app)

    assert app.jobs.done(first, result="stale") is False
    assert app.jobs.fail(first, error="stale") is False
    assert app.jobs.get(first.uid).status == "claimed"      # still the second worker's


def test_the_current_holder_can(app):
    first, second = takenOver(app)

    assert app.jobs.done(second, result="fresh") is True
    assert app.jobs.get(first.uid).result == "fresh"


def test_a_stale_holder_cannot_release_or_renew(app):
    first, second = takenOver(app)

    assert app.jobs.release(first) is False
    assert app.jobs.renewLease(first) is False
    assert app.jobs.get(first.uid).claimId == second.claimId


def test_a_uid_acts_whatever_the_claim(app):
    """An operator marking an item by hand is not a worker racing for it."""

    first, second = takenOver(app)

    assert app.jobs.done(first.uid) is True
    assert app.jobs.get(first.uid).status == "done"


def test_a_released_item_is_claimed_afresh(app):
    app.jobs.add({"n": 1})
    first = app.jobs.claim()
    app.jobs.release(first)

    assert app.jobs.get(first.uid).claimId is None
    assert app.jobs.claim().claimId != first.claimId


def test_a_given_up_item_cannot_be_finished_by_its_old_holder(db):
    class Once(BaseApp):
        jobs = pile()

    app = Once(db, enforceVersion=False, backlogWarnAfter=None)
    app.jobs.add({"n": 1})
    item = app.jobs.claim()
    lapse(app, item)

    assert app.jobs.claim() is None     # out of attempts, so given up on

    assert app.jobs.done(item) is False
    assert app.jobs.get(item.uid).status == "failed"


def test_renewing_skips_an_item_another_worker_took(app):
    first, second = takenOver(app)
    app.jobs._hold(first)

    assert app.jobs.renewLeases() == 0


def test_work_says_when_its_outcome_was_not_recorded(app, caplog):
    app.jobs.add({"n": 1})

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        with app.jobs.work() as item:
            app.jobs.collection.update_one({"uid": item.uid}, {"$set": {"claimId": "someone-else"}})

    assert "was not recorded" in caplog.text
    assert app.jobs.get(item.uid).status == "claimed"


# --- tasks: the claim is the attempt number ---

class Tasks(BaseApp):
    @task
    @staticmethod
    def ping():
        return "pong"


def test_a_task_claim_that_was_lost_does_not_overwrite_a_cancel(db, caplog):
    app = Tasks(db, enforceVersion=False, backlogWarnAfter=None)
    stored = app.task.schedule(Tasks.ping())
    original = app.task.execute

    def cancelMidRun(claimed):
        # the lease lapsed while it ran, and someone canceled it in that window
        app.task.collection.update_one({"uid": claimed.uid}, {"$set": {"leaseUntil": utc_now()}})
        assert app.task.cancel(claimed.uid)
        return original(claimed)

    app.task.execute = cancelMidRun

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        app.task._work()

    assert app.task.get(stored.uid).status == "canceled"
    assert "not recorded" in caplog.text


def test_a_task_taken_over_keeps_the_new_claims_outcome(db):
    app = Tasks(db, enforceVersion=False, backlogWarnAfter=None)
    stored = app.task.schedule(Tasks.ping())
    original = app.task.execute

    def takenOverMidRun(claimed):
        # a second claim bumps attempts; this worker's write must not land
        app.task.collection.update_one({"uid": claimed.uid}, {"$inc": {"attempts": 1}})
        return original(claimed)

    app.task.execute = takenOverMidRun
    app.task._work()

    assert app.task.get(stored.uid).status == "processing"
