"""Graceful shutdown: stop claiming, let in-flight work finish, then exit."""

import os
import signal
import threading
import time

import pytest

from pymonque import BaseApp, Task, task, utc_now
from pymonque.exceptions import VersionMismatch

from conftest import ExampleApp, wait_for


started = threading.Event()
release = threading.Event()


class SlowApp(BaseApp):
    @task
    @staticmethod
    def slow() -> str:
        started.set()
        release.wait(5)
        return "finished"

    @task
    @staticmethod
    def quick() -> str:
        return "ok"


@pytest.fixture(autouse=True)
def resetGates():
    started.clear()
    release.clear()
    yield
    release.set()


@pytest.fixture
def slow(db):
    return SlowApp(db, taskPollInterval=0.02, enforceVersion=False)


# --- stopping ---

def test_stopWorkers_stops_the_loops(slow):
    slow.startWorkers(taskWorkers=2, schedulerWorkers=1)
    assert slow.running

    assert slow.stopWorkers(timeout=5) is True
    assert not slow.running
    assert slow.stopping


def test_stopping_is_not_delayed_by_the_poll_interval(db):
    app = ExampleApp(db, taskPollInterval=5, enforceVersion=False)   # a full sleep would be obvious
    app.startWorkers(taskWorkers=2, schedulerWorkers=1)
    time.sleep(0.05)   # let them reach the idle wait

    started_at = time.monotonic()
    assert app.stopWorkers(timeout=3) is True
    assert time.monotonic() - started_at < 1, "shutdown sat through the poll interval"


def test_work_in_flight_is_finished_not_abandoned(slow, db):
    slow.task.schedule(SlowApp.slow())
    slow.startWorkers(taskWorkers=1)

    assert started.wait(2), "task never started"
    release.set()
    assert slow.stopWorkers(timeout=5) is True

    entry = Task.model_validate(db["pymonque_tasks"].find_one({"work.functionName": "slow"}))
    assert entry.status == "success"
    assert entry.result == "finished"


def test_no_new_work_is_claimed_once_stopping(slow, db):
    slow.task.schedule(SlowApp.slow())
    slow.startWorkers(taskWorkers=1)
    assert started.wait(2)

    slow.task.schedule(SlowApp.quick())    # queued after the stop request
    slow.requestStop()
    release.set()
    slow.stopWorkers(timeout=5)

    assert db["pymonque_tasks"].find_one({"work.functionName": "quick"})["status"] == "pending"


def test_a_timeout_reports_the_work_it_could_not_wait_for(slow):
    slow.task.schedule(SlowApp.slow())
    slow.startWorkers(taskWorkers=1)
    assert started.wait(2)

    assert slow.stopWorkers(timeout=0.2) is False    # still inside slow()

    release.set()


def test_requestStop_does_not_block(slow):
    slow.task.schedule(SlowApp.slow())
    slow.startWorkers(taskWorkers=1)
    assert started.wait(2)

    started_at = time.monotonic()
    slow.requestStop()

    assert time.monotonic() - started_at < 0.5
    assert slow.stopping
    release.set()


def test_workers_can_be_started_again_after_a_stop(db):
    app = ExampleApp(db, taskPollInterval=0.02, enforceVersion=False)
    app.startWorkers(taskWorkers=1)
    app.stopWorkers(timeout=3)

    app.startWorkers(taskWorkers=1)
    app.task.schedule(app.task("greet", name="again"))

    assert wait_for(lambda: app.task.tasksCollection.count_documents({"status": "success"}) == 1)
    app.stopWorkers(timeout=3)


# --- the version slot ---

def test_a_clean_stop_frees_the_version_slot(db):
    class V1(BaseApp):
        @task
        @staticmethod
        def a() -> str: return "a"

    class V2(BaseApp):
        @task
        @staticmethod
        def a(extra: int = 0) -> str: return "a"

    old = V1(db)
    old.startWorkers(taskWorkers=1)

    with pytest.raises(VersionMismatch):
        V2(db).startWorkers(taskWorkers=1)

    old.stopWorkers(timeout=3)                 # deregisters immediately
    V2(db).startWorkers(taskWorkers=1)         # the new version may start at once

    assert len(V2(db).liveWorkers()) == 1


# --- signals ---

def test_sigterm_starts_a_graceful_shutdown(slow, db):
    slow.task.schedule(SlowApp.slow())
    slow.handleSignals(timeout=5, signals=(signal.SIGTERM,))
    slow.startWorkers(taskWorkers=1)

    try:
        assert started.wait(2)
        os.kill(os.getpid(), signal.SIGTERM)

        assert wait_for(lambda: slow.stopping, timeout=2), "the signal was not handled"
        release.set()
        assert slow.stopWorkers(timeout=5) is True
    finally:
        slow.restoreSignals()

    entry = Task.model_validate(db["pymonque_tasks"].find_one({"work.functionName": "slow"}))
    assert entry.status == "success"    # the claimed task still completed


def test_a_second_signal_exits_immediately(slow, monkeypatch):
    exits = []
    monkeypatch.setattr(os, "_exit", lambda code: exits.append(code))

    slow.handleSignals(signals=(signal.SIGTERM,))
    handler = signal.getsignal(signal.SIGTERM)

    try:
        handler(signal.SIGTERM, None)          # first: graceful
        assert slow.stopping and exits == []

        handler(signal.SIGTERM, None)          # second: now
        assert exits == [128 + signal.SIGTERM]
    finally:
        slow.restoreSignals()


def test_signal_handling_is_opt_in(slow):
    before = signal.getsignal(signal.SIGTERM)
    slow.startWorkers(taskWorkers=1)

    assert signal.getsignal(signal.SIGTERM) is before    # startWorkers never installs one
    slow.stopWorkers(timeout=3)


def test_previous_handlers_are_restored(slow):
    def mine(signum, frame): ...

    signal.signal(signal.SIGTERM, mine)
    slow.handleSignals(signals=(signal.SIGTERM,))
    assert signal.getsignal(signal.SIGTERM) is not mine

    slow.restoreSignals()
    assert signal.getsignal(signal.SIGTERM) is mine
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


# --- run() ---

def test_run_blocks_until_a_signal_then_drains(slow, db):
    slow.task.schedule(SlowApp.quick())

    def interrupt():
        assert wait_for(lambda: slow.running, timeout=2)
        time.sleep(0.1)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=interrupt, daemon=True).start()

    try:
        drained = slow.run(taskWorkers=1, timeout=5)     # blocks here
    finally:
        slow.restoreSignals()

    assert drained is True
    assert not slow.running
    assert db["pymonque_tasks"].find_one({"work.functionName": "quick"})["status"] == "success"


def test_an_engine_reports_its_own_state(slow):
    assert not slow.task.running and not slow.task.stopping

    slow.startWorkers(taskWorkers=1)
    assert slow.task.running

    slow.task.stopWorkers(timeout=3)
    assert slow.task.stopping and not slow.task.running
