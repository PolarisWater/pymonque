"""A failing task is retried up to maxAttempts, and a task that kills its worker is given up on."""

import time
from datetime import timedelta

import pytest
from pydantic import ValidationError

from pymonque import BaseApp, task, utc_now


calls: list[str] = []


class App(BaseApp):
    taskMaxAttempts = 3
    taskRetryDelay = 0          # retries claimable at once, so a test can drain them

    @task
    @staticmethod
    def broken() -> None:
        calls.append("broken")
        raise RuntimeError("nope")

    @task
    @staticmethod
    def flaky() -> str:
        calls.append("flaky")
        if calls.count("flaky") == 1:
            raise RuntimeError("first time only")
        return "ok"

    @task
    @staticmethod
    def slow() -> None:
        calls.append("slow")
        time.sleep(0.3)


@pytest.fixture(autouse=True)
def fresh():
    calls.clear()


@pytest.fixture
def app(db):
    return App(db, enforceVersion=False, backlogWarnAfter=None)


def drain(app):
    while app.task._work():
        pass


def test_a_failure_is_retried_until_the_last_attempt(app):
    stored = app.task.schedule(App.broken())

    app.task._work()
    retrying = app.task.get(stored.uid)

    assert retrying.status == "pending"
    assert retrying.attempts == 1
    assert "nope" in retrying.error

    drain(app)
    final = app.task.get(stored.uid)

    assert final.status == "failed"
    assert final.attempts == 3
    assert calls == ["broken"] * 3


def test_a_retry_that_succeeds_clears_the_old_error(app):
    stored = app.task.schedule(App.flaky())
    drain(app)
    final = app.task.get(stored.uid)

    assert final.status == "success"
    assert final.attempts == 2
    assert final.error is None


def test_a_task_overrides_the_app(app):
    stored = app.task.schedule(App.broken(), maxAttempts=1)
    drain(app)

    assert app.task.get(stored.uid).status == "failed"
    assert calls == ["broken"]


class Default(BaseApp):
    @task
    @staticmethod
    def broken() -> None:
        calls.append("broken")
        raise RuntimeError("nope")


def test_by_default_a_failure_is_final(db):
    app = Default(db, enforceVersion=False, backlogWarnAfter=None)
    stored = app.task.schedule(Default.broken())
    app.task._work()

    assert app.task.maxAttempts == 1
    assert app.task.get(stored.uid).status == "failed"
    assert app.task._work() is None


def test_by_default_a_crashed_task_is_not_rerun(db):
    """Under the default, a task whose worker died is failed: nobody knows how far it got."""

    app = Default(db, enforceVersion=False, backlogWarnAfter=None)
    stored = app.task.schedule(Default.broken())
    app.task.collection.update_one({"uid": stored.uid}, {"$set": {
        "status": "processing", "attempts": 1, "leaseUntil": utc_now() - timedelta(seconds=1),
    }})
    app.task._work()

    assert calls == []
    assert app.task.get(stored.uid).status == "failed"


def test_a_retry_waits_retryDelay(db):
    class Delayed(App):
        taskRetryDelay = 60

    app = Delayed(db, enforceVersion=False, backlogWarnAfter=None)
    stored = app.task.schedule(App.broken())
    app.task._work()
    retrying = app.task.get(stored.uid)

    assert retrying.status == "pending"
    assert retrying.leaseUntil > utc_now() + timedelta(seconds=50)
    assert app.task._work() is None             # not yet
    assert calls == ["broken"]


def test_a_task_overrides_the_retry_delay(db):
    class Delayed(App):
        taskRetryDelay = 60

    app = Delayed(db, enforceVersion=False, backlogWarnAfter=None)
    stored = app.task.schedule(App.broken(), retryDelay=0)
    drain(app)

    assert app.task.get(stored.uid).status == "failed"
    assert calls == ["broken"] * 3


def crashed(app, attempts):
    """A task whose worker died mid-run: still processing, lease lapsed, no outcome written."""

    stored = app.task.schedule(App.flaky())
    app.task.collection.update_one({"uid": stored.uid}, {"$set": {
        "status": "processing", "attempts": attempts, "leaseUntil": utc_now() - timedelta(seconds=1),
    }})
    return stored


def test_a_crash_below_the_limit_is_recovered(app):
    stored = crashed(app, attempts=1)
    calls.append("flaky")           # so this run is the one that succeeds
    app.task._work()

    assert app.task.get(stored.uid).status == "success"
    assert app.task.get(stored.uid).attempts == 2


def test_a_task_that_keeps_killing_its_worker_is_given_up_on(app):
    stored = crashed(app, attempts=3)
    app.task._work()
    final = app.task.get(stored.uid)

    assert calls == []              # not run a fourth time
    assert final.status == "failed"
    assert "gave up after 3 attempt(s)" in final.error


def test_a_timeout_is_not_retried(app):
    """The call may still be running, so another attempt would run it twice at once."""

    stored = app.task.schedule(App.slow(), timeout=0.05)
    drain(app)

    assert app.task.get(stored.uid).status == "timeout"
    assert calls == ["slow"]


def test_an_outdated_retry_is_not_run(app):
    stored = app.task.schedule(App.broken(), skipAfter=60)
    app.task._work()
    app.task.collection.update_one(
        {"uid": stored.uid}, {"$set": {"deadline": utc_now() - timedelta(seconds=300)}}
    )
    app.task._work()

    assert app.task.get(stored.uid).status == "outdated"
    assert calls == ["broken"]


def test_a_scheduler_stamps_max_attempts_onto_what_it_emits(app):
    stored = app.scheduler.add(
        App.broken(), app.distribution("constant", dailyFrequency=1), maxAttempts=5
    )
    app.scheduler.collection.update_one(
        {"uid": stored.uid}, {"$set": {"deadline": utc_now(), "leaseUntil": utc_now()}}
    )
    app.scheduler._work()

    assert app.task.find()[0].maxAttempts == 5


def test_fewer_than_one_attempt_is_a_mistake(app, db):
    with pytest.raises(ValidationError):
        app.task.schedule(App.broken(), maxAttempts=0)

    class Never(App):
        taskMaxAttempts = 0

    with pytest.raises(ValueError):
        Never(db)


def test_max_attempts_is_part_of_the_fingerprint(db):
    class More(App):
        taskMaxAttempts = 5

    assert More(db).fingerprint != App(db).fingerprint


def test_retry_delay_is_part_of_the_fingerprint(db):
    class Slower(App):
        taskRetryDelay = 300

    assert Slower(db).fingerprint != App(db).fingerprint


def test_a_scheduler_stamps_retry_delay_onto_what_it_emits(app):
    stored = app.scheduler.add(
        App.broken(), app.distribution("constant", dailyFrequency=1), retryDelay=5
    )
    app.scheduler.collection.update_one(
        {"uid": stored.uid}, {"$set": {"deadline": utc_now(), "leaseUntil": utc_now()}}
    )
    app.scheduler._work()

    assert app.task.find()[0].retryDelay == 5


def test_a_negative_retry_delay_is_a_mistake(app, db):
    with pytest.raises(ValidationError):
        app.task.schedule(App.broken(), retryDelay=-1)

    class Backwards(App):
        taskRetryDelay = -1

    with pytest.raises(ValueError):
        Backwards(db)
