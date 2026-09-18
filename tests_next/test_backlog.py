"""Saying so when work is due and nobody is free to take it: the backlog each task engine measures, and
the warning, engine by engine, that names the engine and counts its workers across processes."""

import logging
import threading
from datetime import timedelta

import pytest

from pymonque_next import BaseApp, task, tasks, utc_now

from tests_next.helpers import waitFor


gate = threading.Event()


class Queue(BaseApp):
    heavy = tasks()

    @task
    @staticmethod
    def quick() -> str:
        return "done"

    @task
    @staticmethod
    def stuck() -> str:
        gate.wait(5)
        return "done"


@pytest.fixture(autouse=True)
def openGate():
    gate.clear()
    yield
    gate.set()


@pytest.fixture
def queue(db, stopAfter):
    return stopAfter(Queue(db, enforceVersion=False, backlogWarnAfter=1, backlogInterval=60, taskPollInterval=0.02))


def overdue(engine, call, count=1, seconds=5):
    for _ in range(count):
        engine.schedule(call, deadline=utc_now() - timedelta(seconds=seconds))


# --- measuring it ---

def test_an_empty_queue_has_no_backlog(queue):
    assert queue.backlog() == {"task": (0, 0.0), "heavy": (0, 0.0)}


def test_the_backlog_is_what_is_due_and_unclaimed_on_each_engine(queue):
    overdue(queue.heavy, Queue.quick(), count=3, seconds=5)

    due, waiting = queue.backlog()["heavy"]

    assert due == 3
    assert 4 < waiting < 7
    assert queue.backlog()["task"] == (0, 0.0)


def test_a_finished_or_held_task_is_not_backlog(queue):
    overdue(queue.task, Queue.quick())
    queue.task.work()
    overdue(queue.task, Queue.quick())
    queue.task.collection.update_one({"status": "pending"}, {"$set": {"status": "running", "leaseUntil": utc_now() + timedelta(minutes=5)}})

    assert queue.task.backlog() == (0, 0.0)


def test_a_task_whose_worker_died_is_backlog(queue):
    overdue(queue.task, Queue.quick())
    queue.task.collection.update_one({}, {"$set": {"status": "running", "leaseUntil": utc_now() - timedelta(seconds=5)}})

    assert queue.task.backlog()[0] == 1


def test_work_due_in_the_future_is_not_backlog(queue):
    queue.task.schedule(Queue.quick(), deadline=utc_now() + timedelta(hours=1))

    assert queue.task.backlog() == (0, 0.0)


# --- warning about it ---

def test_no_warning_while_the_backlog_is_young(queue, caplog):
    overdue(queue.task, Queue.quick(), count=3, seconds=0)

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        queue._checkBacklog()

    assert caplog.text == ""


def test_it_names_the_engine_nobody_runs_workers_on(queue, caplog):
    overdue(queue.heavy, Queue.quick(), count=3)

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        queue._checkBacklog()

    assert "heavy: 3 task(s) due" in caplog.text
    assert "no process runs workers on it" in caplog.text
    assert "task:" not in caplog.text


def test_it_names_the_engine_whose_workers_are_all_busy(queue, caplog):
    overdue(queue.heavy, Queue.stuck(), count=6)
    queue.startWorkers(taskWorkers={"heavy": 2})

    assert waitFor(lambda: queue.heavy.count({"status": "running"}) == 2)

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        queue._checkBacklog()

    assert "heavy: 4 task(s) due" in caplog.text
    assert "2 worker(s) on it across processes" in caplog.text
    assert "not enough workers" in caplog.text


def test_workers_on_another_engine_do_not_count(queue, caplog):
    overdue(queue.heavy, Queue.quick(), count=3)
    queue.startWorkers(taskWorkers={"task": 2})

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        queue._checkBacklog()

    assert "heavy: 3 task(s) due" in caplog.text
    assert "no process runs workers on it" in caplog.text


def test_a_drained_queue_says_nothing(queue, caplog):
    overdue(queue.task, Queue.quick(), count=6)

    while queue.task.work():
        pass

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        queue._checkBacklog()

    assert caplog.text == ""


def test_starting_workers_warns_at_once_about_an_old_backlog(queue, caplog):
    overdue(queue.heavy, Queue.quick(), count=2)

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        queue.startWorkers(taskWorkers={"task": 1})

    assert "heavy: 2 task(s) due" in caplog.text


def test_the_worker_count_spans_processes(db, stopAfter):
    """A process running no task workers must not report that nobody is working."""

    worker = stopAfter(Queue(db, backlogWarnAfter=None, taskPollInterval=0.02))
    worker.startWorkers(taskWorkers={"heavy": 3})

    elsewhere = Queue(db, backlogWarnAfter=None)

    assert elsewhere.heavy.workers.count == 0                   # none of its own
    assert elsewhere.taskWorkers() == {"task": 0, "heavy": 3}   # but it sees them


def test_warnings_can_be_turned_off(db, stopAfter, caplog):
    app = stopAfter(Queue(db, backlogWarnAfter=None))
    overdue(app.task, Queue.quick(), count=3)

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        app.startWorkers()

    assert caplog.text == ""
    assert "backlog" not in app._monitors
