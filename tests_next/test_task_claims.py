"""Claiming tasks: the lease a task carries, the claim that runs it, a task whose worker died, why
only the claim holding a task writes its outcome, and renewal while a long task runs."""

import logging
import threading
import time
from collections import Counter
from datetime import timedelta

import pytest

from pymonque_next import CallSpec, utc_now

from tests_next.helpers import waitFor


def ping() -> str:
    return "pong"


@pytest.fixture
def engine(taskEngine):
    return taskEngine({"ping": ping})


def abandoned(engine, task, **fields):
    """Leave a task as a worker that claimed it and died leaves it: running, its lease run out, no outcome."""

    engine.collection.update_one({"uid": task.uid}, {"$set": {
        "status": "running",
        "claimId": "a-dead-worker",
        "claimedAt": utc_now() - timedelta(hours=1),
        "leaseUntil": utc_now() - timedelta(seconds=1),
        **fields,
    }})


# --- the lease ---

def test_a_waiting_task_is_claimable_at_its_deadline(engine):
    task = engine.get(engine.schedule(CallSpec.new("ping"), deadline=utc_now() + timedelta(hours=1)).uid)

    assert task.leaseUntil == task.deadline


def test_a_claim_marks_the_task_running_under_a_lease_of_its_own(taskEngine):
    seen = {}

    def look():
        seen.update(engine.collection.find_one({"status": "running"}))

    engine = taskEngine({"look": look}, leaseSeconds=60)
    engine.schedule(CallSpec.new("look"))
    engine.work()

    assert seen["claimId"] is not None
    assert seen["leaseUntil"] - seen["claimedAt"] == timedelta(seconds=60)


def test_every_claim_of_a_task_is_its_own(taskEngine):
    claims = []

    def look():
        claims.append(engine.collection.find_one({"status": "running"})["claimId"])

    engine = taskEngine({"look": look})
    engine.schedule(CallSpec.new("look"))
    engine.schedule(CallSpec.new("look"))

    while engine.work():
        pass

    assert len(set(claims)) == 2


def test_an_outcome_lets_go_of_the_claim(engine):
    task = engine.schedule(CallSpec.new("ping"))
    engine.work()

    assert engine.get(task.uid).claimId is None
    assert len(engine.leases) == 0


def test_a_task_held_under_a_live_lease_is_left_alone(engine):
    task = engine.schedule(CallSpec.new("ping"))
    engine.collection.update_one({"uid": task.uid}, {"$set": {
        "status": "running", "claimId": "someone", "leaseUntil": utc_now() + timedelta(minutes=5),
    }})

    assert engine.work() is None
    assert engine.get(task.uid).status == "running"


# --- a worker that died ---

def test_a_task_whose_worker_died_fails_and_is_not_run_again(taskEngine):
    runs = []
    engine = taskEngine({"ping": lambda: runs.append("ping")})
    task = engine.schedule(CallSpec.new("ping"))
    abandoned(engine, task)

    assert engine.work().status == "failed"

    ended = engine.get(task.uid)

    assert runs == []
    assert ended.status == "failed"
    assert "stopped renewing the lease" in ended.error and "not run again" in ended.error
    assert ended.finishedAt is not None and ended.claimId is None
    assert engine.work() is None


def test_a_written_off_task_keeps_the_time_it_was_started(engine):
    task = engine.schedule(CallSpec.new("ping"))
    started = (utc_now() - timedelta(hours=1)).replace(microsecond=0)
    abandoned(engine, task, claimedAt=started)
    engine.work()

    assert engine.get(task.uid).claimedAt == started


def test_writing_off_a_task_is_logged(engine, caplog):
    abandoned(engine, engine.schedule(CallSpec.new("ping")))

    with caplog.at_level(logging.ERROR, logger="pymonque"):
        engine.work()

    assert "stopped renewing the lease" in caplog.text


# --- only the claim holding a task writes its outcome ---

def test_a_lost_claim_does_not_overwrite_a_cancel(taskEngine, caplog):
    def cancelledMidRun():
        # the lease lapsed while it ran, and someone cancelled it in that window
        uid = engine.collection.find_one({"status": "running"})["uid"]
        engine.collection.update_one({"uid": uid}, {"$set": {"leaseUntil": utc_now() - timedelta(seconds=1)}})

        assert engine.cancel(uid)

        return "ran anyway"

    engine = taskEngine({"cancelledMidRun": cancelledMidRun})
    task = engine.schedule(CallSpec.new("cancelledMidRun"))

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        engine.work()

    ended = engine.get(task.uid)

    assert (ended.status, ended.result) == ("canceled", None)
    assert "not recorded" in caplog.text


def test_a_task_taken_over_keeps_the_new_claims_outcome(taskEngine):
    def takenOverMidRun():
        engine.collection.update_one({"status": "running"}, {"$set": {"claimId": "the-new-claim"}})

        return "stale"

    engine = taskEngine({"takenOverMidRun": takenOverMidRun})
    task = engine.schedule(CallSpec.new("takenOverMidRun"))
    engine.work()
    ended = engine.get(task.uid)

    assert (ended.status, ended.claimId, ended.result) == ("running", "the-new-claim", None)


def test_renewal_skips_a_task_another_worker_took(taskEngine):
    renewed = []

    def takenOverMidRun():
        engine.collection.update_one({"status": "running"}, {"$set": {"claimId": "the-new-claim"}})
        renewed.append(engine.leases.renew())

    engine = taskEngine({"takenOverMidRun": takenOverMidRun})
    engine.schedule(CallSpec.new("takenOverMidRun"))
    engine.work()

    assert renewed == [0]


def test_renewal_never_overwrites_a_tasks_outcome(taskEngine):
    claim = {}

    def look():
        claim.update(engine.collection.find_one({"status": "running"}))

    engine = taskEngine({"look": look})
    task = engine.schedule(CallSpec.new("look"))
    engine.work()
    ended = engine.collection.find_one({"uid": task.uid})

    with engine.leases.holding(task.uid, claim["claimId"]):     # a renewal that read the claim before the outcome landed
        assert engine.leases.renew() == 0

    assert engine.collection.find_one({"uid": task.uid}) == ended


# --- renewal while a task runs ---

def test_a_long_task_is_not_taken_over_while_its_lease_is_renewed(taskEngine):
    started, finish = threading.Event(), threading.Event()

    def long() -> str:
        started.set()
        finish.wait(5)

        return "finished"

    engine = taskEngine({"long": long}, leaseSeconds=0.3)
    rival = taskEngine({"long": long}, leaseSeconds=0.3)       # another process, on the same collection
    task = engine.schedule(CallSpec.new("long"))
    worker = threading.Thread(target=engine.work)
    worker.start()

    try:
        assert started.wait(3), "the task never started"

        until = time.monotonic() + 1        # several leases long

        while time.monotonic() < until:
            assert rival.work() is None, "another worker took a task that is being worked"
            time.sleep(0.02)
    finally:
        finish.set()
        worker.join(5)

    assert engine.get(task.uid).status == "done"


def test_renewal_stops_when_the_work_does(taskEngine):
    engine = taskEngine({"ping": ping}, leaseSeconds=0.3)
    engine.schedule(CallSpec.new("ping"))
    engine.work()

    assert len(engine.leases) == 0
    assert waitFor(lambda: not engine.leases.renewing, timeout=1)


def test_a_task_is_never_run_twice_across_threads(taskEngine):
    runs = Counter()
    lock = threading.Lock()

    def count(n: int):
        with lock:
            runs[n] += 1

    engine = taskEngine({"count": count})

    for n in range(30):
        engine.schedule(CallSpec.new("count", n=n))

    def drain():
        while engine.work() is not None:
            pass

    threads = [threading.Thread(target=drain) for _ in range(4)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(10)

    assert runs == Counter(range(30))
    assert engine.count({"status": "done"}) == 30
