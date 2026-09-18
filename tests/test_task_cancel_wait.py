"""Cancelling tasks that have not started, and waiting on a task from any process."""

import threading
import time
from datetime import timedelta

import pytest

from pymonque import CallSpec, utc_now
from pymonque.exceptions import TaskNotFound


def greet(name: str) -> str:
    return f"Hello, {name}!"


def broken():
    raise RuntimeError("nope")


def slow() -> str:
    time.sleep(0.2)

    return "slept"


FUNCTIONS = {"greet": greet, "broken": broken, "slow": slow}


@pytest.fixture
def engine(taskEngine):
    return taskEngine(FUNCTIONS)


def held(engine, task, leaseUntil):
    engine.collection.update_one({"uid": task.uid}, {"$set": {"status": "running", "claimId": "someone", "leaseUntil": leaseUntil}})


# --- cancel ---

def test_a_waiting_task_can_be_cancelled(engine):
    task = engine.schedule(CallSpec.new("greet", name="Ada"), deadline=utc_now() + timedelta(hours=1))

    assert engine.cancel(task.uid) is True
    assert engine.get(task.uid).status == "canceled"


def test_a_cancelled_task_is_never_run(engine):
    task = engine.schedule(CallSpec.new("greet", name="Ada"))
    engine.cancel(task.uid)

    assert engine.work() is None
    assert engine.get(task.uid).status == "canceled"


def test_a_running_task_cannot_be_cancelled(engine):
    task = engine.schedule(CallSpec.new("greet", name="Ada"))
    held(engine, task, utc_now() + timedelta(minutes=5))

    assert engine.cancel(task.uid) is False
    assert engine.get(task.uid).status == "running"


def test_a_task_whose_worker_died_can_be_cancelled(engine):
    task = engine.schedule(CallSpec.new("greet", name="Ada"))
    held(engine, task, utc_now() - timedelta(hours=1))

    assert engine.cancel(task.uid) is True

    cancelled = engine.get(task.uid)

    assert (cancelled.status, cancelled.claimId) == ("canceled", None)


def test_a_finished_task_cannot_be_cancelled(engine):
    task = engine.schedule(CallSpec.new("greet", name="Ada"))
    engine.work()

    assert engine.cancel(task.uid) is False
    assert engine.get(task.uid).status == "done"


def test_cancelling_an_unknown_uid_reports_nothing_happened(engine):
    assert engine.cancel("no-such-uid") is False


def test_cancelMany_takes_a_filter(engine):
    for name in ("a", "a", "b"):
        engine.schedule(CallSpec.new("greet", name=name))

    assert engine.cancelMany({"work.kwargs.name": "a"}) == 2
    assert engine.count({"status": "canceled"}) == 2
    assert engine.findOne({"work.kwargs.name": "b"}).status == "pending"


def test_cancelMany_leaves_running_work_alone(engine):
    engine.schedule(CallSpec.new("greet", name="waiting"))
    running = engine.schedule(CallSpec.new("greet", name="running"))
    held(engine, running, utc_now() + timedelta(minutes=5))

    assert engine.cancelMany() == 1
    assert engine.get(running.uid).status == "running"


# --- wait ---

def worker(engine):
    thread = threading.Thread(target=lambda: [engine.work() for _ in range(3)])
    thread.start()

    return thread


def test_wait_returns_the_task_as_it_ended(engine):
    task = engine.schedule(CallSpec.new("slow"))
    thread = worker(engine)

    try:
        ended = engine.wait(task, timeout=3, interval=0.02)
    finally:
        thread.join(3)

    assert (ended.status, ended.result) == ("done", "slept")


def test_wait_takes_a_uid(engine):
    task = engine.schedule(CallSpec.new("greet", name="Ada"))
    engine.work()

    assert engine.wait(task.uid, timeout=1).result == "Hello, Ada!"


def test_another_process_can_do_the_waiting(engine, taskEngine):
    task = engine.schedule(CallSpec.new("slow"))
    thread = worker(engine)

    try:
        ended = taskEngine(FUNCTIONS).wait(task.uid, timeout=3, interval=0.02)
    finally:
        thread.join(3)

    assert ended.result == "slept"


@pytest.mark.parametrize("end", ["broken", "cancel"])
def test_a_failed_or_cancelled_task_has_ended(engine, end):
    task = engine.schedule(CallSpec.new("broken"))

    if end == "cancel":
        engine.cancel(task.uid)
    else:
        engine.work()

    assert engine.wait(task, timeout=1).status == ("canceled" if end == "cancel" else "failed")


def test_wait_gives_up_after_its_timeout(engine):
    task = engine.schedule(CallSpec.new("greet", name="Ada"))     # nobody is working

    started = time.monotonic()

    with pytest.raises(TimeoutError, match="still pending"):
        engine.wait(task, timeout=0.15, interval=0.05)

    assert time.monotonic() - started < 1


def test_waiting_on_an_unknown_task_is_an_error(engine):
    with pytest.raises(TaskNotFound):
        engine.wait("no-such-uid", timeout=0.1)
