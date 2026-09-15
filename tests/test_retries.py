"""A failing task is retried up to its maxAttempts, and a task that kills its worker is given up on."""

import time
from datetime import timedelta

import pytest

from pymonque import BaseApp, task, utc_now


calls: list[str] = []


class App(BaseApp):
    @task(maxAttempts=3, retryDelay=0)      # retries claimable at once, so a test can drain them
    @staticmethod
    def broken() -> None:
        calls.append("broken")
        raise RuntimeError("nope")

    @task(maxAttempts=3, retryDelay=0)
    @staticmethod
    def flaky() -> str:
        calls.append("flaky")
        if calls.count("flaky") == 1:
            raise RuntimeError("first time only")
        return "ok"

    @task
    @staticmethod
    def once() -> None:
        calls.append("once")
        raise RuntimeError("nope")

    @task(maxAttempts=2, retryDelay=60)
    @staticmethod
    def delayed() -> None:
        calls.append("delayed")
        raise RuntimeError("nope")

    @task(maxAttempts=3, retryDelay=0, timeout=0.05)
    @staticmethod
    def slow() -> None:
        calls.append("slow")
        time.sleep(0.3)

    @task(maxAttempts=3, retryDelay=0, skipAfter=60)
    @staticmethod
    def stale() -> None:
        calls.append("stale")
        raise RuntimeError("nope")


@pytest.fixture(autouse=True)
def fresh():
    calls.clear()


@pytest.fixture
def app(db):
    return App(db, enforceVersion=False, backlogWarnAfter=None)


def drain(app):
    while app.task._work():
        pass


# --- retrying ---

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


def test_a_retry_waits_its_delay(app):
    stored = app.task.schedule(App.delayed())
    app.task._work()
    retrying = app.task.get(stored.uid)

    assert retrying.status == "pending"
    assert retrying.leaseUntil > utc_now() + timedelta(seconds=50)
    assert app.task._work() is None             # not yet
    assert calls == ["delayed"]


def test_the_app_default_applies_to_a_bare_task(db):
    class Retrying(BaseApp):
        taskMaxAttempts = 2
        taskRetryDelay = 0

        @task
        @staticmethod
        def broken() -> None:
            calls.append("broken")
            raise RuntimeError("nope")

    app = Retrying(db, enforceVersion=False, backlogWarnAfter=None)
    stored = app.task.schedule(Retrying.broken())
    drain(app)

    assert app.task.get(stored.uid).status == "failed"
    assert calls == ["broken"] * 2


def test_an_emitted_task_retries_under_its_tasks_limits(app):
    scheduler = app.scheduler.add(App.broken(), app.distribution("constant", dailyFrequency=1))
    now = utc_now()
    app.scheduler.collection.update_one({"uid": scheduler.uid}, {"$set": {"deadline": now, "leaseUntil": now}})

    app.scheduler._work()
    drain(app)

    assert calls == ["broken"] * 3


# --- the default is one attempt ---

def test_by_default_a_failure_is_final(app):
    stored = app.task.schedule(App.once())
    app.task._work()

    assert app.task.limits["once"].maxAttempts == 1
    assert app.task.get(stored.uid).status == "failed"
    assert app.task._work() is None


def crashed(app, spec, attempts):
    """A task whose worker died mid-run: still processing, lease lapsed, no outcome written."""

    stored = app.task.schedule(spec)
    app.task.collection.update_one({"uid": stored.uid}, {"$set": {
        "status": "processing", "attempts": attempts, "leaseUntil": utc_now() - timedelta(seconds=1),
    }})
    return stored


def test_by_default_a_crashed_task_is_not_rerun(app):
    """Under the default, a task whose worker died is failed: nobody knows how far it got."""

    stored = crashed(app, App.once(), attempts=1)
    app.task._work()

    assert calls == []
    assert app.task.get(stored.uid).status == "failed"


# --- a crash counts ---

def test_a_crash_below_the_limit_is_recovered(app):
    stored = crashed(app, App.flaky(), attempts=1)
    calls.append("flaky")           # so this run is the one that succeeds
    app.task._work()

    assert app.task.get(stored.uid).status == "success"
    assert app.task.get(stored.uid).attempts == 2


def test_a_task_that_keeps_killing_its_worker_is_given_up_on(app):
    stored = crashed(app, App.flaky(), attempts=3)
    app.task._work()
    final = app.task.get(stored.uid)

    assert calls == []              # not run a fourth time
    assert final.status == "failed"
    assert "gave up after 3 attempt(s)" in final.error


# --- what is never retried ---

def test_a_timeout_is_not_retried(app):
    """The call may still be running, so another attempt would run it twice at once."""

    stored = app.task.schedule(App.slow())
    drain(app)

    assert app.task.get(stored.uid).status == "timeout"
    assert calls == ["slow"]


def test_an_outdated_retry_is_not_run(app):
    stored = app.task.schedule(App.stale())
    app.task._work()
    app.task.collection.update_one(
        {"uid": stored.uid}, {"$set": {"deadline": utc_now() - timedelta(seconds=300)}}
    )
    app.task._work()

    assert app.task.get(stored.uid).status == "outdated"
    assert calls == ["stale"]
