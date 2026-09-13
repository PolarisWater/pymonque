"""Saying so when work is due and nobody is free to take it."""

import logging
import time
from datetime import timedelta

import pytest

from pymonque import BaseApp, task, utc_now


class Slow(BaseApp):
    @task
    @staticmethod
    def slow() -> str:
        time.sleep(0.4)
        return "done"

    @task
    @staticmethod
    def quick() -> str:
        return "done"


def app(db, **kwargs):
    return Slow(db, **{"enforceVersion": False, "backlogWarnAfter": 1,
                       "backlogInterval": 60, "taskPollInterval": 0.02, **kwargs})


def overdue(a, spec, count=1, seconds=5):
    for _ in range(count):
        a.task.schedule(spec, deadline=utc_now() - timedelta(seconds=seconds))


# --- measuring it ---

def test_an_empty_queue_has_no_backlog(db):
    assert app(db).backlog() == (0, 0.0)


def test_the_backlog_is_what_is_due_and_unclaimed(db):
    a = app(db)
    overdue(a, Slow.quick(), count=3, seconds=5)

    due, waiting = a.backlog()

    assert due == 3
    assert 4 < waiting < 7


def test_a_task_a_worker_is_holding_is_not_backlog(db):
    a = app(db)
    overdue(a, Slow.quick())
    a.task._work()                          # claimed and finished

    assert a.backlog() == (0, 0.0)


def test_work_due_in_the_future_is_not_backlog(db):
    a = app(db)
    a.task.schedule(Slow.quick(), deadline=utc_now() + timedelta(hours=1))

    assert a.backlog() == (0, 0.0)


# --- warning about it ---

def test_no_warning_while_the_backlog_is_young(db, caplog):
    a = app(db)
    overdue(a, Slow.quick(), count=3, seconds=0)

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        a._checkWorkers()

    assert caplog.text == ""


def test_it_says_so_when_nobody_runs_task_workers(db, caplog):
    a = app(db)
    overdue(a, Slow.quick(), count=3)

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        a._checkWorkers()

    assert "no process is running task workers" in caplog.text


def test_it_says_so_when_the_workers_are_all_busy(db, caplog):
    a = app(db)
    overdue(a, Slow.slow(), count=6)
    a.startWorkers(taskWorkers=2, schedulerWorkers=0)

    try:
        time.sleep(0.2)
        with caplog.at_level(logging.WARNING, logger="pymonque"):
            a._checkWorkers()
    finally:
        a.stopWorkers(timeout=5)

    assert "2 task worker(s) running" in caplog.text
    assert "not enough workers" in caplog.text


def test_a_drained_queue_says_nothing(db, caplog):
    a = app(db)
    overdue(a, Slow.quick(), count=6)

    while a.task._work():                       # drained, however long that took
        pass

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        a._checkWorkers()

    assert a.task.count({"status": "success"}) == 6
    assert caplog.text == ""


def test_the_count_spans_processes(db):
    """A scheduler-only process must not report that nobody is working."""

    worker = Slow(db, backlogWarnAfter=None, taskPollInterval=0.02)
    worker.startWorkers(taskWorkers=3, schedulerWorkers=0)

    try:
        schedulerOnly = Slow(db, backlogWarnAfter=None)

        assert schedulerOnly.task.workerCount == 0      # none of its own
        assert schedulerOnly.taskWorkers() == 3         # but it can see them
    finally:
        worker.stopWorkers(timeout=5)


def test_warnings_can_be_turned_off(db, caplog):
    a = app(db, backlogWarnAfter=None)
    overdue(a, Slow.quick(), count=3)

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        a.startWorkers(taskWorkers=0, schedulerWorkers=0)

    assert caplog.text == ""
