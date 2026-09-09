"""Leases: a claim is held for a while and renewed while it's worked, so an
abandoned one becomes claimable again on its own — no restart, no sweep."""

import threading
import time
from datetime import timedelta

import pytest

from pymonque import BaseApp, Task, task, pile, utc_now

from conftest import ExampleApp, appWith, wait_for


held = threading.Event()
release = threading.Event()


class LongApp(BaseApp):
    scraps = pile()

    @task
    @staticmethod
    def long() -> str:
        held.set()
        release.wait(10)
        return "finished"

    @task
    def longPile(self) -> str:
        with self.scraps.work() as item:
            if item is None:
                return "empty"
            held.set()
            release.wait(10)
            return "drained"


@pytest.fixture(autouse=True)
def gates():
    held.clear()
    release.clear()
    yield
    release.set()
    time.sleep(0.05)


@pytest.fixture
def long(db):
    # renewal runs every max(1, leaseSeconds/3), so 3s means once a second
    return LongApp(db, taskPoolInterval=0.05, leaseSeconds=3, enforceVersion=False)


# --- a claim is exclusive while it is held ---

def test_a_fresh_task_carries_a_lease(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    entry = Task.model_validate(tasks.find_one())

    assert entry.leaseUntil == entry.deadline   # claimable exactly when it is due


def test_claiming_pushes_the_lease_out(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    app.task._work()

    assert Task.model_validate(tasks.find_one()).leaseUntil > utc_now()


# --- the renewal thread is what makes a long task safe ---

def test_a_long_task_is_not_stolen_while_its_lease_is_renewed(long, db):
    long.task.schedule(LongApp.long())
    long.startWorkers(taskWorkers=1)

    assert held.wait(3), "task never started"

    rival = LongApp(db, leaseSeconds=3, enforceVersion=False)
    deadline = time.monotonic() + 4          # longer than a whole lease period

    while time.monotonic() < deadline:
        assert rival.task._work() is None, "another worker stole a task being worked"
        time.sleep(0.1)

    release.set()
    assert wait_for(lambda: db["pymonque_tasks"].find_one()["status"] == "success", timeout=5)
    long.stopWorkers(timeout=5)


def test_renewal_stops_when_the_work_does(long, db):
    long.task.schedule(LongApp.long())
    long.startWorkers(taskWorkers=1)
    assert held.wait(3)
    release.set()

    assert wait_for(lambda: db["pymonque_tasks"].find_one()["status"] == "success", timeout=5)
    long.stopWorkers(timeout=5)

    assert long.task.renewLeases() == 0       # nothing is held any more


def test_an_abandoned_lease_is_picked_up_by_the_next_worker(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    tasks.update_many({}, {"$set": {
        "status": "processing", "leaseUntil": utc_now() - timedelta(seconds=1)
    }})

    assert app.task._work() is not None
    assert Task.model_validate(tasks.find_one()).status == "success"


# --- piles renew too, while an item is inside work() ---

def test_a_pile_item_is_not_stolen_while_it_is_being_worked(long, db):
    long.scraps.add({"n": 1})
    long.task.schedule(LongApp.longPile())
    long.startWorkers(taskWorkers=1)

    assert held.wait(3), "the item was never claimed"

    rival = LongApp(db, leaseSeconds=3, enforceVersion=False)
    deadline = time.monotonic() + 4

    while time.monotonic() < deadline:
        assert rival.scraps.claim() is None, "another worker stole an item being worked"
        time.sleep(0.1)

    release.set()
    assert wait_for(lambda: long.scraps.count(status="done") == 1, timeout=5)
    long.stopWorkers(timeout=5)


# --- the point of the whole change ---

def test_a_new_instance_does_not_disturb_work_in_flight(long, db):
    long.task.schedule(LongApp.long())
    long.startWorkers(taskWorkers=1)
    assert held.wait(3)

    for _ in range(3):
        LongApp(db, leaseSeconds=3, enforceVersion=False)   # other processes booting

    assert db["pymonque_tasks"].find_one()["status"] == "processing"   # untouched

    release.set()
    assert wait_for(lambda: db["pymonque_tasks"].find_one()["status"] == "success", timeout=5)
    long.stopWorkers(timeout=5)


def test_constructing_a_app_runs_no_housekeeping(db):
    app = appWith(ExampleApp, db, overdueTaskPolicy="skip")
    app.task.schedule(app.task("greet", name="old"), deadline=utc_now() - timedelta(days=1))

    appWith(ExampleApp, db, overdueTaskPolicy="skip")   # would have outdated it before

    assert app.task.tasksCollection.find_one()["status"] == "pending"


def test_starting_workers_does_run_it(db):
    app = appWith(ExampleApp, db, overdueTaskPolicy="skip",
                  _kwargs={"enforceVersion": False})
    app.task.schedule(app.task("greet", name="old"), deadline=utc_now() - timedelta(days=1))

    app.startWorkers(taskWorkers=0, schedulerWorkers=0)

    assert app.task.tasksCollection.find_one()["status"] == "outdated"


# --- documents written before leases existed ---

def test_a_task_without_a_lease_is_backfilled(db, app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    tasks.update_many({}, {"$unset": {"leaseUntil": ""}})

    assert app.task._work() is None          # invisible to the claim until backfilled

    app.init()

    assert app.task._work() is not None


def test_a_scheduler_left_processing_by_an_old_version_is_revived(db, app, schedulers):
    app.scheduler.add(ExampleApp.greet(name="Ada"), app.distribution("constant", dailyFrequency=24))
    schedulers.update_many({}, {"$set": {"status": "processing"}, "$unset": {"leaseUntil": ""}})

    app.init()
    revived = app.scheduler.find()[0]

    assert revived.status == "enabled"
    assert revived.leaseUntil == revived.deadline


def test_an_item_without_a_lease_is_backfilled(db, app):
    app.outbox.add(to="a@b.c")
    app.outbox.itemsCollection.update_many({}, {"$unset": {"leaseUntil": ""}})

    assert app.outbox.claim() is None

    app.init()

    assert app.outbox.claim() is not None


# --- scheduler leases ---

def test_a_scheduler_lease_can_be_renewed(app, schedulers):
    scheduler = app.scheduler.add(
        ExampleApp.greet(name="Ada"), app.distribution("constant", dailyFrequency=24)
    )
    schedulers.update_one({"uid": scheduler.uid}, {"$set": {"leaseUntil": utc_now() - timedelta(hours=1)}})
    app.scheduler._hold(scheduler.uid)

    assert app.scheduler.renewLeases() == 1
    assert app.scheduler.byUid(scheduler.uid).leaseUntil > utc_now()
