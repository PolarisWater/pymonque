"""skipAfter: a task claimed too long after its deadline is outdated instead of run.

It is the one rule for work going stale, whether the app was down or nobody kept up.
"""

from datetime import timedelta

import pytest

from pymonque import BaseApp, task, utc_now


ran: list[str] = []


class App(BaseApp):
    taskSkipAfter = 60

    @task
    @staticmethod
    def default() -> str:
        ran.append("default")
        return "ran"

    @task(skipAfter=3600)
    @staticmethod
    def patient() -> str:
        return "ran"

    @task(skipAfter=1)
    @staticmethod
    def eager() -> str:
        return "ran"

    @task(skipAfter=None)
    @staticmethod
    def unskippable() -> str:
        return "ran"


@pytest.fixture(autouse=True)
def fresh():
    ran.clear()


@pytest.fixture
def app(db):
    return App(db, enforceVersion=False, backlogWarnAfter=None)


def late(app, spec, seconds):
    stored = app.task.schedule(spec, deadline=utc_now() - timedelta(seconds=seconds))
    app.task._work()
    return app.task.get(stored.uid)


def test_without_a_limit_a_task_runs_however_late(db):
    class Unlimited(BaseApp):
        @task
        @staticmethod
        def ping() -> str:
            return "pong"

    app = Unlimited(db, enforceVersion=False, backlogWarnAfter=None)

    assert late(app, Unlimited.ping(), 86400).status == "success"


def test_the_app_default_applies_to_a_bare_task(app):
    assert late(app, App.default(), 300).status == "outdated"
    assert late(app, App.default(), 10).status == "success"


def test_a_task_declares_its_own_in_either_direction(app):
    assert late(app, App.patient(), 300).status == "success"     # looser than the default
    assert late(app, App.eager(), 10).status == "outdated"       # tighter


def test_none_makes_a_task_unskippable(app):
    assert late(app, App.unskippable(), 86400).status == "success"


def test_an_outdated_task_is_not_run_and_says_how_late(app):
    finished = late(app, App.default(), 300)

    assert ran == []
    assert finished.status == "outdated"
    assert "300s after its deadline" in finished.error
    assert finished.result is None


def test_an_emitted_task_goes_stale_by_its_tasks_limit(app):
    scheduler = app.scheduler.add(App.default(), app.distribution("constant", dailyFrequency=1))
    due = utc_now() - timedelta(seconds=300)
    app.scheduler.collection.update_one({"uid": scheduler.uid}, {"$set": {"deadline": due, "leaseUntil": due}})

    app.scheduler._work()
    app.task._work()

    assert app.task.find()[0].status == "outdated"
    assert ran == []
