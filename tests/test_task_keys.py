"""Scheduling under a key: one task per key at a time. While a task of that key is waiting or running,
scheduling it again returns that task — moved up to the sooner deadline if it is still waiting — and
once it has ended, the key is free for the next."""

import threading
from datetime import timedelta

import pytest

from pymonque import utc_now


def sync(accountId: int) -> str:
    return f"synced {accountId}"


@pytest.fixture
def engine(taskEngine):
    return taskEngine({"sync": sync})


def later(minutes):
    return (utc_now() + timedelta(minutes=minutes)).replace(microsecond=0)


def test_a_key_queues_one_task_however_often_it_is_asked_for(engine):
    first = engine.schedule(engine("sync", accountId=42), later(10), key="sync-42")

    again = [engine.schedule(engine("sync", accountId=42), later(10), key="sync-42") for _ in range(5)]

    assert {each.uid for each in again} == {first.uid}
    assert engine.count() == 1
    assert (first.key, engine.get(first.uid).activeKey) == ("sync-42", "sync-42")


def test_asking_sooner_moves_the_waiting_task_up(engine):
    task = engine.schedule(engine("sync", accountId=42), later(60), key="sync-42")

    sooner = engine.schedule(engine("sync", accountId=42), later(5), key="sync-42")
    after = engine.schedule(engine("sync", accountId=42), later(30), key="sync-42")

    assert sooner.uid == after.uid == task.uid
    assert (after.deadline, after.leaseUntil) == (later(5), later(5))      # the soonest asked for, lease with it


def test_a_running_task_is_returned_and_left_as_it_is(engine):
    task = engine.schedule(engine("sync", accountId=42), later(10), key="sync-42")
    engine.collection.update_one({"uid": task.uid}, {"$set": {"status": "running", "claimId": "a-worker"}})

    again = engine.schedule(engine("sync", accountId=42), key="sync-42")        # due now: sooner

    assert again.uid == task.uid
    assert (again.status, again.deadline) == ("running", later(10))
    assert engine.count() == 1


def test_the_first_call_wins_whatever_a_later_one_asks_to_run(engine):
    task = engine.schedule(engine("sync", accountId=1), key="sync")
    again = engine.schedule(engine("sync", accountId=2), key="sync")

    assert again.uid == task.uid and again.work.kwargs == {"accountId": 1}


def test_a_finished_task_frees_its_key_and_keeps_it_on_record(engine):
    task = engine.schedule(engine("sync", accountId=42), key="sync-42")
    engine.work()

    ran = engine.get(task.uid)
    assert (ran.status, ran.key, ran.activeKey) == ("done", "sync-42", None)

    fresh = engine.schedule(engine("sync", accountId=42), key="sync-42")
    assert fresh.uid != task.uid and engine.count() == 2


@pytest.mark.parametrize("end", ["cancel", "fail", "outdated", "worker died", "incompatible", "stuck"])
def test_every_way_a_task_ends_frees_its_key(taskEngine, engine, end):
    from pymonque import TaskLimits

    def boom(accountId: int) -> None:
        raise ValueError("nope")

    if end == "fail":
        engine = taskEngine({"sync": boom})
    elif end == "outdated":
        engine = taskEngine({"sync": sync}, limits={"sync": TaskLimits(skipAfter=1)})

    task = engine.schedule(engine("sync", accountId=42), utc_now() - timedelta(minutes=5), key="sync-42")

    if end == "cancel":
        engine.cancel(task.uid)
    elif end in ("fail", "outdated"):
        engine.work()
    elif end == "worker died":
        engine.collection.update_one({"uid": task.uid}, {"$set": {"status": "running", "claimId": "dead", "leaseUntil": utc_now() - timedelta(seconds=1)}})
        engine.work()
    elif end == "incompatible":
        taskEngine({}, collection=engine.collection.name).flagIncompatible()
    elif end == "stuck":
        engine.collection.update_one({"uid": task.uid}, {"$set": {"status": "running", "claimId": "dead", "leaseUntil": utc_now() - timedelta(seconds=1)}})
        taskEngine({}, collection=engine.collection.name).writeOffStuck()

    ended = engine.get(task.uid)
    assert ended.status in ("canceled", "failed", "outdated", "incompatible") and ended.activeKey is None
    assert engine.schedule(engine("sync", accountId=42), key="sync-42").uid != task.uid


def test_without_a_key_nothing_changes(engine):
    engine.schedule(engine("sync", accountId=42))
    engine.schedule(engine("sync", accountId=42))

    assert engine.count() == 2


def test_keys_are_per_engine(taskEngine):
    one, two = taskEngine({"sync": sync}, name="one"), taskEngine({"sync": sync}, name="two")

    assert one.schedule(one("sync", accountId=1), key="k").uid != two.schedule(two("sync", accountId=1), key="k").uid


def test_a_key_is_a_non_empty_string(engine):
    for bad in ("", 42):
        with pytest.raises(TypeError, match="non-empty string"):
            engine.schedule(engine("sync", accountId=1), key=bad)

    assert engine.count() == 0


def test_a_key_is_the_engines_to_keep(engine):
    task = engine.schedule(engine("sync", accountId=42), key="sync-42")

    with pytest.raises(TypeError, match="the engine keeps it"):
        engine.update(task.uid, key="other")

    with pytest.raises(TypeError, match="the engine keeps it"):
        engine.schedule(engine("sync", accountId=42), activeKey="x")


def test_schedule_from_distribution_takes_a_key(engine):
    daily = engine.distributions("constant", dailyFrequency=1)
    first = engine.scheduleFromDistribution(engine("sync", accountId=42), daily, key="sync-42")

    assert engine.scheduleFromDistribution(engine("sync", accountId=42), daily, key="sync-42").uid == first.uid


def test_processes_racing_on_a_key_queue_one_task(engine):
    uids, start = [], threading.Barrier(8)

    def race():
        start.wait()
        uids.append(engine.schedule(engine("sync", accountId=42), key="sync-42").uid)

    threads = [threading.Thread(target=race) for _ in range(8)]
    for each in threads:
        each.start()
    for each in threads:
        each.join()

    assert len(set(uids)) == 1 and engine.count() == 1
