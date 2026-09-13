"""skipAfter: a task claimed too long after its deadline is outdated instead of run.

It is the one rule for work going stale, whether the app was down or nobody kept up.
"""

from datetime import timedelta

import pytest
from pydantic import ValidationError

from pymonque import BaseApp, task, utc_now


class App(BaseApp):
    @task
    @staticmethod
    def ping() -> str:
        return "pong"


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


def test_minus_one_makes_a_task_unskippable(db):
    class Limited(App):
        taskSkipAfter = 60

    limited = Limited(db, enforceVersion=False, backlogWarnAfter=None)

    assert run(limited, late=86400, skipAfter=-1).status == "success"


def test_minus_one_on_the_app_is_the_same_as_no_limit(db):
    class Unlimited(App):
        taskSkipAfter = -1

    assert run(Unlimited(db, enforceVersion=False, backlogWarnAfter=None), late=86400).status == "success"


def test_minus_one_lifts_a_timeout_too(db):
    class Limited(App):
        taskTimeout = 0.01

    limited = Limited(db, enforceVersion=False, backlogWarnAfter=None)
    stored = limited.task.schedule(App.ping(), timeout=-1)

    assert limited.task.timeoutFor(stored) is None


@pytest.mark.parametrize("field", ["skipAfter", "timeout"])
def test_any_other_negative_is_a_mistake(app, field):
    with pytest.raises(ValidationError):
        app.task.schedule(App.ping(), **{field: -5})


def test_a_negative_app_limit_is_a_mistake_too(db):
    class Broken(App):
        taskSkipAfter = -5

    with pytest.raises(ValueError):
        Broken(db)


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
        App.ping(), app.distribution("constant", dailyFrequency=1), skipAfter=-1
    )
    app.scheduler.collection.update_one(
        {"uid": stored.uid}, {"$set": {"deadline": utc_now(), "leaseUntil": utc_now()}}
    )
    app.scheduler._work()

    assert app.task.find()[0].skipAfter == -1


def test_a_limit_change_is_a_different_fingerprint(db):
    class Loose(App):
        taskSkipAfter = None

    class Tight(App):
        taskSkipAfter = 60

    assert Loose(db).fingerprint != Tight(db).fingerprint
