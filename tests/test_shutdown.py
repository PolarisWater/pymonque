"""Graceful shutdown: stop claiming, let the work in flight finish — its lease renewed until it does —
then free the version slot; the monitor threads a process runs; signals; and run()."""

import os
import signal
import threading
import time

import pytest

from pymonque import BaseApp, task, tasks
from pymonque.exceptions import VersionMismatch

from tests.helpers import waitFor


started = threading.Event()
release = threading.Event()


class Slow(BaseApp):
    heavy = tasks(leaseSeconds=0.3)

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
def gates():
    started.clear()
    release.clear()
    yield
    release.set()


@pytest.fixture
def slow(db, stopAfter):
    return stopAfter(Slow(db, taskPollInterval=0.02, enforceVersion=False, backlogWarnAfter=None))


# --- stopping ---

def test_stop_workers_stops_every_loop(slow):
    slow.startWorkers(taskWorkers=2, schedulerWorkers=1)
    assert slow.running

    assert slow.stopWorkers(timeout=5) is True
    assert not slow.running
    assert slow.stopping


def test_stopping_is_not_delayed_by_the_poll_interval(db, stopAfter):
    app = stopAfter(Slow(db, taskPollInterval=5, schedulerPollInterval=5, backlogWarnAfter=None))
    app.startWorkers(taskWorkers=2, schedulerWorkers=1)
    time.sleep(0.05)    # let them reach the idle wait

    began = time.monotonic()
    assert app.stopWorkers(timeout=3) is True
    assert time.monotonic() - began < 1, "shutdown sat through the poll interval"


def test_work_in_flight_is_finished_not_abandoned(slow):
    task = slow.task.schedule(Slow.slow())
    slow.startWorkers(taskWorkers={"task": 1})

    assert started.wait(2)
    release.set()
    assert slow.stopWorkers(timeout=5) is True

    finished = slow.task.get(task.uid)
    assert (finished.status, finished.result) == ("done", "finished")


def test_leases_keep_renewing_while_a_shutdown_drains(slow):
    task = slow.heavy.schedule(Slow.slow())
    slow.startWorkers(taskWorkers={"heavy": 1})
    assert started.wait(2)

    slow.requestStop()
    time.sleep(1)       # three leases' worth: without renewal, anyone could take the task over

    assert slow.heavy.work() is None
    assert slow.heavy.get(task.uid).status == "running"

    release.set()
    assert slow.stopWorkers(timeout=5) is True
    assert slow.heavy.get(task.uid).status == "done"


def test_nothing_new_is_claimed_once_stopping(slow):
    slow.task.schedule(Slow.slow())
    slow.startWorkers(taskWorkers={"task": 1})
    assert started.wait(2)

    quick = slow.task.schedule(Slow.quick())    # queued after the stop request
    slow.requestStop()
    release.set()
    slow.stopWorkers(timeout=5)

    assert slow.task.get(quick.uid).status == "pending"


def test_a_timeout_reports_the_work_it_could_not_wait_for(slow, caplog):
    slow.task.schedule(Slow.slow())
    slow.startWorkers(taskWorkers={"task": 1})
    assert started.wait(2)

    assert slow.stopWorkers(timeout=0.2) is False       # still inside slow()
    assert "shutdown timed out with work still in flight" in caplog.text


def test_request_stop_does_not_block(slow):
    slow.task.schedule(Slow.slow())
    slow.startWorkers(taskWorkers={"task": 1})
    assert started.wait(2)

    began = time.monotonic()
    slow.requestStop()

    assert time.monotonic() - began < 0.5
    assert slow.stopping


def test_join_workers_blocks_until_the_workers_stop(slow):
    slow.startWorkers(taskWorkers=1)

    assert slow.joinWorkers(timeout=0.1) is False

    slow.requestStop()
    assert slow.joinWorkers(timeout=5) is True


# --- the version slot, and the threads a process runs ---

class V1(BaseApp):
    @task
    @staticmethod
    def a() -> str:
        return "a"


class V2(BaseApp):
    @task
    @staticmethod
    def a(extra: int = 0) -> str:
        return "a"


def test_a_clean_stop_frees_the_version_slot_at_once(db, stopAfter):
    old = stopAfter(V1(db, backlogWarnAfter=None))
    old.startWorkers(taskWorkers=1)

    with pytest.raises(VersionMismatch):
        stopAfter(V2(db, backlogWarnAfter=None)).startWorkers(taskWorkers=1)

    old.stopWorkers(timeout=3)      # deregisters now, without waiting to go stale

    new = stopAfter(V2(db, backlogWarnAfter=None))
    new.startWorkers(taskWorkers=1)
    assert len(new.liveWorkers()) == 1


def test_the_heartbeat_goes_on_while_work_drains(db, stopAfter):
    app = stopAfter(Slow(db, heartbeatInterval=0.05, backlogWarnAfter=None))
    app.task.schedule(Slow.slow())
    app.startWorkers(taskWorkers={"task": 1})
    assert started.wait(2)

    app.requestStop()
    seen = db["pymonque_workers"].find_one({"uid": app.workerUid})["lastSeen"]

    assert waitFor(lambda: db["pymonque_workers"].find_one({"uid": app.workerUid})["lastSeen"] > seen, timeout=2)

    release.set()
    app.stopWorkers(timeout=5)
    assert db["pymonque_workers"].find_one({"uid": app.workerUid}) is None


def test_starting_twice_leaves_one_heartbeat(db, stopAfter):
    app = stopAfter(Slow(db, heartbeatInterval=30, backlogInterval=30, taskPollInterval=0.02))
    app.startWorkers(taskWorkers={"task": 1})
    first = dict(app._monitors)

    app.startWorkers(taskWorkers={"task": 1})

    assert sorted(first) == ["backlog", "heartbeat"]
    assert app._monitors == first       # the same two threads, not four


def test_stopping_takes_the_monitor_threads_with_it(db, stopAfter):
    app = stopAfter(Slow(db, heartbeatInterval=30, backlogInterval=30, taskPollInterval=0.02))
    app.startWorkers(taskWorkers={"task": 1})
    monitors = list(app._monitors.values())

    assert all(t.is_alive() for t in monitors)

    app.stopWorkers(timeout=5)

    assert not any(t.is_alive() for t in monitors)


# --- signals ---

def test_sigterm_starts_a_graceful_shutdown(slow):
    task = slow.task.schedule(Slow.slow())
    slow.handleSignals(signals=(signal.SIGTERM,))
    slow.startWorkers(taskWorkers={"task": 1})

    assert started.wait(2)
    os.kill(os.getpid(), signal.SIGTERM)

    assert waitFor(lambda: slow.stopping, timeout=2), "the signal was not handled"
    release.set()
    assert slow.stopWorkers(timeout=5) is True
    assert slow.task.get(task.uid).status == "done"     # the claimed task still finished


def test_a_second_signal_exits_at_once(slow, monkeypatch):
    exits = []
    monkeypatch.setattr(os, "_exit", lambda code: exits.append(code))

    slow.handleSignals(signals=(signal.SIGTERM,))
    handler = signal.getsignal(signal.SIGTERM)

    handler(signal.SIGTERM, None)       # first: graceful
    assert slow.stopping and exits == []

    handler(signal.SIGTERM, None)       # second: now
    assert exits == [128 + signal.SIGTERM]


def test_signal_handling_is_opt_in(slow):
    before = signal.getsignal(signal.SIGTERM)
    slow.startWorkers(taskWorkers=1)

    assert signal.getsignal(signal.SIGTERM) is before


def test_previous_handlers_are_restored(slow):
    def mine(signum, frame): ...

    signal.signal(signal.SIGTERM, mine)

    try:
        slow.handleSignals(signals=(signal.SIGTERM,))
        slow.handleSignals(signals=(signal.SIGTERM,))       # twice: ours is not the one to restore
        assert signal.getsignal(signal.SIGTERM) is not mine

        slow.restoreSignals()
        assert signal.getsignal(signal.SIGTERM) is mine
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_DFL)


# --- run() ---

def test_run_blocks_until_a_signal_then_drains(slow):
    quick = slow.task.schedule(Slow.quick())
    before = signal.getsignal(signal.SIGTERM)

    def interrupt():
        assert waitFor(lambda: slow.running, timeout=2)
        time.sleep(0.1)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=interrupt, daemon=True).start()

    assert slow.run(taskWorkers=1, timeout=5) is True      # blocks here
    assert not slow.running and not slow.retired
    assert slow.task.get(quick.uid).status == "done"
    assert signal.getsignal(signal.SIGTERM) is before      # handled only while it ran
