"""Task timeouts: where a limit comes from; what a timeout records and logs; the call stopped, or — blocked
outside Python — abandoned and counted; a pile item it holds left to lapse; and retiring a process
that has abandoned too many threads."""

import logging
import threading
import time
from datetime import timedelta

import pytest

from pymonque_next import BaseApp, pile, task, utc_now

from tests_next.helpers import waitFor


class Timed(BaseApp):
    outbox = pile(maxAttempts=2, leaseSeconds=0.3)

    @task
    @staticmethod
    def sleep(seconds: float = 0.05) -> str:
        time.sleep(seconds)
        return "finished"

    @task(timeout=0.1)
    @staticmethod
    def blocked(seconds: float = 2) -> str:
        time.sleep(seconds)     # a call blocked outside Python: an injected exception waits for it
        return "finished"

    @task(timeout=0.1)
    @staticmethod
    def spinning() -> None:
        while True:             # pure Python: an injected exception stops it
            pass

    @task(timeout=5)
    @staticmethod
    def boom() -> None:
        raise ValueError("nope")

    @task(timeout=5)
    @staticmethod
    def exits() -> None:
        raise SystemExit(3)

    @task(timeout=0.1)
    def holding(self, seconds: float = 1.5) -> str:
        with self.outbox.work() as w:
            time.sleep(seconds)

        return "finished"


@pytest.fixture
def timed(db, stopAfter):
    app = stopAfter(Timed(db, enforceVersion=False, backlogWarnAfter=None, taskPollInterval=0.02))
    app.task.stopGrace = 0.2

    return app


def run(app, call):
    stored = app.task.schedule(call)
    app.task.work()

    return app.task.get(stored.uid)


# --- where a limit comes from ---

def test_a_task_has_no_time_limit_by_default(timed):
    assert timed.task.limits["sleep"].timeout is None
    assert run(timed, Timed.sleep(seconds=0.2)).status == "done"


def test_a_task_declares_its_own(timed):
    assert run(timed, Timed.blocked()).status == "timeout"


def test_the_app_default_applies_to_a_bare_task(db, stopAfter):
    app = stopAfter(type("Limited", (Timed,), {"taskTimeout": 0.1})(db, backlogWarnAfter=None))

    assert run(app, Timed.sleep(seconds=0.5)).status == "timeout"


def test_a_task_may_declare_itself_looser_than_the_default_or_unlimited(db):
    class Limited(BaseApp):
        taskTimeout = 0.05

        @task(timeout=5)
        @staticmethod
        def generous() -> str:
            time.sleep(0.2)
            return "finished"

        @task(timeout=None)
        @staticmethod
        def unlimited() -> str:
            time.sleep(0.2)
            return "finished"

    app = Limited(db)

    assert run(app, Limited.generous()).status == "done"
    assert run(app, Limited.unlimited()).status == "done"


def test_an_emitted_task_times_out_by_its_tasks_limit(timed):
    scheduler = timed.scheduler.add(Timed.blocked(), timed.distribution("constant", dailyFrequency=1))
    timed.scheduler.update(scheduler.uid, deadline=utc_now())

    timed.scheduler.work()
    timed.task.work()

    assert timed.task.find()[0].status == "timeout"


# --- what a timeout records ---

def test_a_timed_out_task_records_why_and_where_it_was(timed):
    finished = run(timed, Timed.blocked())

    assert finished.status == "timeout"
    assert "did not finish within 0.1s" in finished.error
    assert "in blocked" in finished.error and "time.sleep(seconds)" in finished.error
    assert finished.result is None
    assert finished.claimId is None
    assert finished.executionTime >= timedelta(seconds=0.1)


def test_a_timeout_warns_naming_the_task_and_where_it_was(timed, caplog):
    with caplog.at_level(logging.WARNING, logger="pymonque"):
        finished = run(timed, Timed.blocked())

    assert f"({finished.uid}) timed out after 0.1s" in caplog.text
    assert "time.sleep(seconds)" in caplog.text


def test_a_raise_inside_a_timed_task_is_still_a_failure(timed):
    finished = run(timed, Timed.boom())

    assert finished.status == "failed"
    assert "ValueError: nope" in finished.error


def test_sys_exit_inside_a_timed_task_is_a_failure(timed):
    assert run(timed, Timed.exits()).status == "failed"


def test_a_timeout_frees_the_worker_for_the_next_task(timed):
    slow = timed.task.schedule(Timed.blocked(seconds=5))
    quick = timed.task.schedule(Timed.sleep(seconds=0.01))

    timed.startWorkers(taskWorkers=1)

    assert waitFor(lambda: timed.task.get(quick.uid).status == "done", timeout=2)
    assert timed.task.get(slow.uid).status == "timeout"


def test_a_timed_out_task_is_never_run_again(timed):
    runs = []

    class Counted(Timed):
        @task(timeout=0.05)
        @staticmethod
        def once() -> None:
            runs.append(1)
            time.sleep(0.5)

    app = Counted(timed.db, backlogWarnAfter=None)
    stored = app.task.schedule(Counted.once())

    app.task.work()
    time.sleep(0.6)

    assert app.task.work() is None
    assert app.task.get(stored.uid).status == "timeout"
    assert len(runs) == 1


# --- stopping the call ---

def test_a_pure_python_call_is_stopped(timed, caplog):
    with caplog.at_level(logging.WARNING, logger="pymonque"):
        finished = run(timed, Timed.spinning())

        assert waitFor(lambda: "stopped after its timeout" in caplog.text, timeout=2)

    assert finished.status == "timeout"
    assert timed.abandoned() == []


def test_a_call_blocked_outside_python_is_abandoned_and_logged_with_its_stack(timed, caplog):
    with caplog.at_level(logging.WARNING, logger="pymonque"):
        finished = run(timed, Timed.blocked())

        assert waitFor(lambda: len(timed.abandoned()) == 1, timeout=2)

    [abandoned] = timed.abandoned()

    assert abandoned.task.uid == finished.uid
    assert "still running after being stopped — blocked outside Python" in caplog.text
    assert caplog.text.count("time.sleep(seconds)") >= 2        # at the timeout, and again when abandoned
    assert "time.sleep(seconds)" in abandoned.where()


def test_an_abandoned_thread_that_ends_is_no_longer_counted(timed):
    run(timed, Timed.blocked(seconds=0.6))

    assert waitFor(lambda: len(timed.abandoned()) == 1, timeout=2)
    assert waitFor(lambda: timed.abandoned() == [], timeout=2)


def test_the_abandoned_count_is_published_on_the_heartbeat(timed):
    timed.startWorkers()
    timed.task.schedule(Timed.blocked())
    timed.task.work()

    assert waitFor(
        lambda: timed.db["pymonque_workers"].find_one({"uid": timed.workerUid})["abandonedThreads"] == 1,
        timeout=2,
    )


# --- a pile item the call holds ---

def test_a_pile_item_held_by_a_timed_out_call_stops_being_renewed(timed, caplog):
    item = timed.outbox.add({"to": "a@b.c"})

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        run(timed, Timed.holding())

    assert "held by a call abandoned at its timeout" in caplog.text

    # its lease lapses, a spent try, and another worker takes it while the call still sleeps
    assert waitFor(lambda: timed.outbox.claim() is not None, timeout=2)

    taken = timed.outbox.get(item.uid)
    assert (taken.status, taken.attempts) == ("running", 2)


def test_a_pile_item_is_renewed_while_a_timed_call_is_within_its_limit(db, stopAfter):
    class Patient(Timed):
        @task(timeout=5)
        def holding(self, seconds: float = 0.8) -> str:
            with self.outbox.work() as w:
                time.sleep(seconds)

            return "finished"

    app = stopAfter(Patient(db, backlogWarnAfter=None))
    item = app.outbox.add({"to": "a@b.c"})

    worker = threading.Thread(target=lambda: run(app, Patient.holding()))
    worker.start()
    time.sleep(0.6)     # twice the pile's lease

    assert app.outbox.claim() is None
    worker.join(5)
    assert app.outbox.get(item.uid).status == "done"


# --- retiring ---

def test_retire_after_must_be_a_number_of_threads(db):
    with pytest.raises(ValueError, match="retireAfter"):
        Timed(db, retireAfter=0)


def test_a_process_retires_once_it_has_abandoned_enough_threads(db, stopAfter, caplog):
    app = stopAfter(Timed(db, backlogWarnAfter=None, taskPollInterval=0.02, retireAfter=1))
    app.task.stopGrace = 0.1
    app.task.schedule(Timed.blocked())

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        app.startWorkers(taskWorkers=1)

        assert waitFor(lambda: app.retired, timeout=3)
        assert app.joinWorkers(timeout=3)

    assert app.stopping
    assert "at the limit of 1, so this process retires" in caplog.text
    assert "stuck at:" in caplog.text and "time.sleep(seconds)" in caplog.text
    assert "no longer claiming, draining" in caplog.text


def test_a_retired_process_claims_nothing_more(db, stopAfter):
    app = stopAfter(Timed(db, backlogWarnAfter=None, taskPollInterval=0.02, retireAfter=1))
    app.task.stopGrace = 0.1
    app.task.schedule(Timed.blocked())
    app.startWorkers(taskWorkers=1)

    assert waitFor(lambda: app.retired, timeout=3)
    app.joinWorkers(timeout=3)

    later = app.task.schedule(Timed.sleep())
    time.sleep(0.1)

    assert app.task.get(later.uid).status == "pending"


def test_run_returns_once_retired_and_says_so(db, stopAfter, caplog):
    app = stopAfter(Timed(db, backlogWarnAfter=None, taskPollInterval=0.02, retireAfter=1))
    app.task.stopGrace = 0.1
    app.task.schedule(Timed.blocked())

    with caplog.at_level(logging.ERROR, logger="pymonque"):
        assert app.run(taskWorkers=1, timeout=5) is True

    assert app.retired
    assert "exiting, for the supervisor to restart this process" in caplog.text


def test_without_retire_after_a_process_keeps_working(timed):
    timed.task.schedule(Timed.blocked())
    timed.startWorkers(taskWorkers=1)

    assert waitFor(lambda: len(timed.abandoned()) == 1, timeout=3)

    later = timed.task.schedule(Timed.sleep())

    assert waitFor(lambda: timed.task.get(later.uid).status == "done")
    assert not timed.retired
