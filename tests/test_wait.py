"""task.wait(): block until a task has finished, from any process."""

import time

import pytest

from pymonque import BaseApp, task
from pymonque.exceptions import TaskNotFound


class App(BaseApp):
    @task
    @staticmethod
    def add(a: int, b: int) -> int:
        return a + b

    @task
    @staticmethod
    def broken() -> None:
        raise RuntimeError("nope")

    @task
    @staticmethod
    def slow() -> str:
        time.sleep(0.2)
        return "done"


@pytest.fixture
def app(db):
    return App(db, taskPollInterval=0.02, enforceVersion=False, backlogWarnAfter=None)


def test_it_returns_the_finished_task(app):
    stored = app.task.schedule(App.add(a=2, b=3))
    app.startWorkers(taskWorkers=1)

    try:
        finished = app.task.wait(stored, timeout=3)
    finally:
        app.stopWorkers(timeout=3)

    assert finished.status == "success"
    assert finished.result == 5


def test_it_takes_a_uid_too(app):
    stored = app.task.schedule(App.add(a=1, b=1))
    app.task._work()

    assert app.task.wait(stored.uid, timeout=1).result == 2


def test_another_process_can_do_the_waiting(app, db):
    stored = app.task.schedule(App.slow())
    app.startWorkers(taskWorkers=1)

    try:
        finished = App(db, enforceVersion=False).task.wait(stored.uid, timeout=3)
    finally:
        app.stopWorkers(timeout=3)

    assert finished.result == "done"


def test_a_failure_is_finished(app):
    stored = app.task.schedule(App.broken())
    app.task._work()

    assert app.task.wait(stored, timeout=1).status == "failed"


def test_a_task_waiting_for_a_retry_is_not_finished(app):
    stored = app.task.schedule(App.broken(), maxAttempts=2, retryDelay=60)
    app.task._work()

    with pytest.raises(TimeoutError):
        app.task.wait(stored, timeout=0.1)


def test_it_gives_up_after_the_timeout(app):
    stored = app.task.schedule(App.add(a=1, b=1))      # nobody is working

    started = time.monotonic()
    with pytest.raises(TimeoutError):
        app.task.wait(stored, timeout=0.15, interval=0.05)

    assert time.monotonic() - started < 1


def test_an_unknown_task_is_an_error(app):
    with pytest.raises(TaskNotFound):
        app.task.wait("no-such-uid", timeout=0.1)
