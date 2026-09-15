"""Limits are declared on @task, filled in from the app's defaults, and set nowhere else."""

import pytest
from pydantic import ValidationError

from pymonque import BaseApp, TaskLimits, task, utc_now


class App(BaseApp):
    taskTimeout = 300
    taskMaxAttempts = 2

    @task
    @staticmethod
    def plain() -> None:
        return None

    @task(timeout=5, retryDelay=1)
    @staticmethod
    def declared() -> None:
        return None

    @task(timeout=None, skipAfter=None)
    def unlimited(self) -> None:
        return None


@pytest.fixture
def app(db):
    return App(db, enforceVersion=False, backlogWarnAfter=None)


# --- resolving ---

def test_a_bare_task_takes_the_app_defaults(app):
    assert app.task.limits["plain"] == TaskLimits(timeout=300, skipAfter=None, maxAttempts=2, retryDelay=60)


def test_a_declared_limit_wins_and_the_rest_are_the_defaults(app):
    assert app.task.limits["declared"] == TaskLimits(timeout=5, skipAfter=None, maxAttempts=2, retryDelay=1)


def test_none_means_no_limit_whatever_the_default(app):
    assert app.task.limits["unlimited"].timeout is None


def test_both_forms_declare_a_runnable_task(app):
    assert set(app.task.functions) == {"plain", "declared", "unlimited"}

    stored = app.task.schedule(App.declared())
    app.task._work()

    assert app.task.get(stored.uid).status == "success"


def test_a_task_runs_under_its_functions_limits(app):
    assert app.task.limitsFor(app.task.schedule(App.declared())).timeout == 5


def test_a_task_is_declared_once():
    def job():
        return None

    declaration = task(timeout=1)
    declaration(job)

    with pytest.raises(TypeError):
        declaration(job)


# --- checked where they are written ---

@pytest.mark.parametrize("limits", [
    {"timeout": 0}, {"skipAfter": -1}, {"maxAttempts": 0}, {"maxAttempts": None}, {"retryDelay": -1},
])
def test_a_bad_limit_fails_where_it_is_written(limits):
    with pytest.raises(ValidationError):
        class Broken(BaseApp):
            @task(**limits)
            @staticmethod
            def job() -> None:
                return None


@pytest.mark.parametrize("default", [
    {"taskTimeout": 0}, {"taskSkipAfter": -1}, {"taskMaxAttempts": 0}, {"taskRetryDelay": -1},
])
def test_a_bad_default_fails_when_the_app_is_built(db, default):
    Broken = type("Broken", (BaseApp,), default)    # no tasks, and it is still checked

    with pytest.raises(ValidationError):
        Broken(db)


# --- nowhere else ---

@pytest.mark.parametrize("limit", ["timeout", "skipAfter", "maxAttempts", "retryDelay"])
def test_a_call_carries_no_limits(app, limit):
    with pytest.raises(TypeError):
        app.task.schedule(App.plain(), **{limit: 1})


def test_stored_tasks_and_schedulers_carry_no_limits(app):
    stored = app.task.schedule(App.plain())
    scheduler = app.scheduler.add(App.plain(), app.distribution("constant", dailyFrequency=1))

    for raw in (
        app.task.collection.find_one({"uid": stored.uid}),
        app.scheduler.collection.find_one({"uid": scheduler.uid}),
    ):
        assert not {"timeout", "skipAfter", "maxAttempts", "retryDelay"} & set(raw)


def test_an_emitted_task_runs_under_its_tasks_limits(app):
    scheduler = app.scheduler.add(App.declared(), app.distribution("constant", dailyFrequency=1))
    now = utc_now()
    app.scheduler.collection.update_one({"uid": scheduler.uid}, {"$set": {"deadline": now, "leaseUntil": now}})

    app.scheduler._work()

    assert app.task.limitsFor(app.task.find()[0]).timeout == 5


# --- shared behaviour, so part of the fingerprint ---

def test_a_declared_limit_changes_the_fingerprint(db):
    class Loose(BaseApp):
        @task
        @staticmethod
        def job() -> None:
            return None

    class Tight(BaseApp):
        @task(timeout=1)
        @staticmethod
        def job() -> None:
            return None

    assert Loose(db).fingerprint != Tight(db).fingerprint


@pytest.mark.parametrize("default", [
    {"taskTimeout": 1}, {"taskSkipAfter": 60}, {"taskMaxAttempts": 3}, {"taskRetryDelay": 5},
])
def test_a_default_change_changes_the_fingerprint(db, default):
    assert type("Changed", (App,), default)(db).fingerprint != App(db).fingerprint


def test_a_default_a_task_overrides_does_not_matter_to_it(db):
    class Everywhere(BaseApp):
        taskTimeout = 1

        @task(timeout=10)
        @staticmethod
        def job() -> None:
            return None

    class Nowhere(BaseApp):
        taskTimeout = 2

        @task(timeout=10)
        @staticmethod
        def job() -> None:
            return None

    assert Everywhere(db).fingerprint == Nowhere(db).fingerprint
