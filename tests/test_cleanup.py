"""Keeping the history in bounds: finished tasks and items stay, as the record of what ran, until a
cleanup deletes those that ended long enough ago — `purge()` on an engine or pile, or the task every
app has, `cleanupFinished(days=…)`, put on a scheduler."""

from datetime import timedelta

import pytest

from pymonque import BaseApp, pile, task, tasks, utc_now
from pymonque.exceptions import TaskValidationError


class App(BaseApp):
    taskTimeout = 1
    heavy = tasks()
    outbox = pile()

    @task
    @staticmethod
    def noop() -> None: ...


@pytest.fixture
def app(db):
    return App(db, enforceVersion=False)


def ended(engine, uid, status, daysAgo):
    engine.collection.update_one({"uid": uid}, {"$set": {"status": status, "finishedAt": utc_now() - timedelta(days=daysAgo)}})


# --- purge() on a task engine ---

def test_purge_deletes_only_finished_tasks_that_ended_long_enough_ago(app):
    old = [app.task.schedule(App.noop()) for _ in range(3)]
    for each, status in zip(old, ["done", "failed", "canceled"]):
        ended(app.task, each.uid, status, 40)

    recent = app.task.schedule(App.noop())
    ended(app.task, recent.uid, "done", 1)
    waiting = app.task.schedule(App.noop())
    running = app.task.schedule(App.noop())
    app.task.collection.update_one({"uid": running.uid}, {"$set": {"status": "running", "createdAt": utc_now() - timedelta(days=90)}})

    assert app.task.purge(timedelta(days=30)) == 3
    assert {t.uid for t in app.task.find()} == {recent.uid, waiting.uid, running.uid}


def test_purge_can_narrow_the_statuses_but_never_reach_unfinished_work(app):
    kept, gone = app.task.schedule(App.noop()), app.task.schedule(App.noop())
    ended(app.task, kept.uid, "failed", 40)
    ended(app.task, gone.uid, "done", 40)

    assert app.task.purge(timedelta(days=30), statuses={"done"}) == 1
    assert app.task.get(kept.uid) is not None

    with pytest.raises(ValueError, match="only finished tasks"):
        app.task.purge(0, statuses={"done", "pending"})


def test_a_task_finished_before_its_end_was_recorded_counts_by_when_it_was_created(app):
    old = app.task.schedule(App.noop())
    app.task.collection.update_one(
        {"uid": old.uid}, {"$set": {"status": "done", "createdAt": utc_now() - timedelta(days=60)}, "$unset": {"finishedAt": ""}}
    )

    assert app.task.purge(timedelta(days=30)) == 1


# --- purge() on a pile ---

def test_a_pile_purges_by_age_or_all_of_a_status(app):
    old, recent, waiting = (app.outbox.add({"n": n}) for n in range(3))
    ended(app.outbox, old.uid, "done", 40)
    ended(app.outbox, recent.uid, "done", 1)

    assert app.outbox.purge("done", olderThan=timedelta(days=30)) == 1
    assert app.outbox.purge("done") == 1                    # without an age: every one, as before
    assert [i.uid for i in app.outbox.find()] == [waiting.uid]

    with pytest.raises(ValueError, match="only finished items"):
        app.outbox.purge("pending")


# --- the task every app has ---

def test_cleanup_finished_clears_every_task_engine_and_pile_and_gone_workers(app):
    for engine in (app.task, app.heavy):
        ended(engine, engine.schedule(App.noop()).uid, "done", 40)
        engine.schedule(App.noop())                         # waiting: stays

    ended(app.outbox, app.outbox.add({"n": 1}).uid, "failed", 40)
    app.registry.collection.insert_one({"uid": "killed", "lastSeen": utc_now() - timedelta(days=40)})
    app.registry.collection.insert_one({"uid": "alive", "lastSeen": utc_now()})

    assert app.cleanupFinished(days=30) == {"tasks": 2, "items": 1, "workers": 1}
    assert app.task.count() == app.heavy.count() == 1
    assert [w["uid"] for w in app.registry.collection.find()] == ["alive"]


def test_cleanup_finished_runs_on_a_scheduler_like_any_task(app):
    ended(app.task, app.task.schedule(App.noop()).uid, "done", 40)
    app.scheduler.ensure("cleanup", App.cleanupFinished(days=30), app.distribution("constant", dailyFrequency=1))
    app.scheduler.collection.update_many({}, {"$set": {"deadline": utc_now() - timedelta(minutes=1), "leaseUntil": utc_now() - timedelta(minutes=1)}})

    app.scheduler.work()                                    # emits the cleanup
    ran = app.task.work()

    assert ran.work.functionName == "cleanupFinished" and ran.result["tasks"] == 1


def test_cleanup_finished_has_its_own_limits_whatever_the_apps_defaults(app):
    assert app.limits["cleanupFinished"].timeout is None    # App.taskTimeout = 1 does not cut it short
    assert app.limits["noop"].timeout == 1


def test_cleanup_finished_refuses_a_negative_age(app):
    with pytest.raises(TaskValidationError):
        app.task.schedule(App.cleanupFinished(days=-1))


def test_an_app_cannot_declare_a_task_of_its_name(db):
    with pytest.raises(TypeError, match="would replace BaseApp.cleanupFinished"):
        class Clash(BaseApp):
            @task
            @staticmethod
            def cleanupFinished() -> None: ...
