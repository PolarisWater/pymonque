"""Multiple scheduler engines feeding one task engine, with typed Scheduler subclasses."""

from datetime import timedelta

import pytest
from pymongo import IndexModel, ASCENDING

from pymonque import (
    BaseApp, CallSpec, Scheduler, SchedulerEngine, Task,
    task, schedulers, utc_now,
)
from pymonque.core import schedulerUid
from pymonque.exceptions import TaskValidationError, TaskNotFound, DistributionNotFound

from conftest import wait_for, WORKER_POLL_INTERVAL


# --- the shape a real app has: context on the scheduler, stamped onto every task ---

class AccountScheduler(Scheduler):
    accountId: int

    def emitWork(self) -> CallSpec:
        return self.work.bind(accountId=self.accountId)


class GroupScheduler(Scheduler):
    groupId: str

    def emitWork(self) -> CallSpec:
        return self.work.bind(groupId=self.groupId)


class OpsApp(BaseApp):
    globalOps  = schedulers()
    accountOps = schedulers(AccountScheduler, extraIndexes=[
        IndexModel([("accountId", ASCENDING)], name="accountId_idx"),
    ])
    groupOps   = schedulers(GroupScheduler, "group_operations")

    @task
    @staticmethod
    def sync(accountId: int, full: bool = False) -> str:
        return f"synced {accountId} full={full}"

    @task
    @staticmethod
    def announce(groupId: str) -> str:
        return f"announced {groupId}"

    @task
    @staticmethod
    def housekeeping() -> str:
        return "swept"


@pytest.fixture
def ops(db):
    return OpsApp(db)


def hourly(app) -> CallSpec:
    return app.distribution("constant", dailyFrequency=24)


def due(engine, scheduler):
    at = utc_now() - timedelta(minutes=1)
    engine.schedulersCollection.update_one(
        {"uid": scheduler.uid}, {"$set": {"deadline": at, "leaseUntil": at}}
    )


# --- CallSpec.bind ---

def test_bind_merges_kwargs():
    spec = CallSpec.new("sync", full=True)

    assert spec.bind(accountId=7).kwargs == {"full": True, "accountId": 7}


def test_bind_overrides_and_leaves_the_original_alone():
    spec = CallSpec.new("sync", accountId=1)

    assert spec.bind(accountId=2).kwargs == {"accountId": 2}
    assert spec.kwargs == {"accountId": 1}


# --- emitWork ---

def test_the_default_scheduler_emits_its_work_unchanged(ops):
    scheduler = ops.globalOps.add(OpsApp.housekeeping(), hourly(ops))

    assert scheduler.emitWork() == scheduler.work


def test_a_subclass_stamps_its_context(ops):
    scheduler = ops.accountOps.add(OpsApp.sync(full=True), hourly(ops), accountId=7)

    assert scheduler.emitWork().kwargs == {"full": True, "accountId": 7}


def test_the_context_reaches_the_emitted_task(ops, tasks):
    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)
    due(ops.accountOps, scheduler)
    ops.accountOps._work()
    emitted = Task.model_validate(tasks.find_one())

    assert emitted.work.kwargs == {"accountId": 7}
    assert emitted.factory.uid == scheduler.uid


def test_the_emitted_task_actually_runs(ops, tasks):
    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)
    due(ops.accountOps, scheduler)
    ops.accountOps._work()
    ops.task._work()

    assert Task.model_validate(tasks.find_one()).result == "synced 7 full=False"


def test_the_stored_work_stays_uncontextualised(ops):
    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)

    assert ops.accountOps.get(scheduler.uid).work.kwargs == {}  # context applied at emit


# --- validation goes through emitWork ---

def test_work_is_validated_as_it_will_be_emitted(ops):
    # `sync` requires accountId, which only emitWork supplies
    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)

    assert scheduler.accountId == 7


def test_the_same_work_is_rejected_on_an_engine_that_cannot_supply_it(ops):
    with pytest.raises(TaskValidationError):
        ops.globalOps.add(OpsApp.sync(), hourly(ops))


def test_a_bad_kwarg_is_still_caught(ops):
    with pytest.raises(TaskValidationError):
        ops.accountOps.add(CallSpec.new("sync", nope=1), hourly(ops), accountId=7)


def test_an_unknown_task_is_rejected(ops):
    with pytest.raises(TaskNotFound):
        ops.accountOps.add(CallSpec.new("does_not_exist"), hourly(ops), accountId=7)


def test_an_unknown_distribution_is_rejected(ops):
    with pytest.raises(DistributionNotFound):
        ops.accountOps.add(OpsApp.sync(), CallSpec.new("does_not_exist"), accountId=7)


def test_a_missing_context_field_is_rejected(ops):
    with pytest.raises(Exception):
        ops.accountOps.add(OpsApp.sync(), hourly(ops))  # no accountId


# --- schedulerModel ---

def test_each_engine_reads_its_own_model(ops):
    ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)
    ops.groupOps.add(OpsApp.announce(), hourly(ops), groupId="g1")

    assert isinstance(ops.accountOps.find()[0], AccountScheduler)
    assert isinstance(ops.groupOps.find()[0], GroupScheduler)
    assert isinstance(ops.globalOps.build(OpsApp.housekeeping(), hourly(ops)), Scheduler)


def test_context_survives_a_fire(ops):
    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)
    due(ops.accountOps, scheduler)
    ops.accountOps._work()

    assert ops.accountOps.get(scheduler.uid).accountId == 7


# --- the registry ---

def test_engines_are_collected(ops):
    assert set(ops.schedulerEngines) == {"globalOps", "accountOps", "groupOps", "scheduler"}
    assert all(isinstance(e, SchedulerEngine) for e in ops.schedulerEngines.values())


def test_each_engine_gets_its_own_collection(ops):
    assert ops.globalOps.schedulersCollection.name == "pymonque_schedulers_globalOps"
    assert ops.accountOps.schedulersCollection.name == "pymonque_schedulers_accountOps"
    assert ops.groupOps.schedulersCollection.name == "group_operations"
    assert ops.scheduler.schedulersCollection.name == "pymonque_schedulers"


def test_they_share_one_task_engine(ops):
    assert all(e.taskEngine is ops.task for e in ops.schedulerEngines.values())


def test_class_access_returns_the_declaration():
    assert isinstance(OpsApp.accountOps, schedulers)
    assert OpsApp.accountOps.name == "accountOps"


def test_a_declaration_named_scheduler_replaces_the_default(db):
    class Q(BaseApp):
        scheduler = schedulers(AccountScheduler)

        @task
        @staticmethod
        def sync(accountId: int): ...

    app = Q(db)

    assert app.scheduler.schedulerModel is AccountScheduler
    assert app.scheduler.schedulersCollection.name == "pymonque_schedulers"
    assert set(app.schedulerEngines) == {"scheduler"}


def test_a_subclass_can_override_an_engine(db):
    class Child(OpsApp):
        groupOps = schedulers(GroupScheduler, "other_groups")

    assert Child(db).groupOps.schedulersCollection.name == "other_groups"


def test_per_engine_policy_and_interval(db):
    class Q(BaseApp):
        fast = schedulers(policy="skip", pollInterval=0.5)

    class App(Q):
        overdueSchedulersPolicy = "execute once"

    app = App(db, schedulerPollInterval=9)

    assert app.fast.policy == "skip"
    assert app.fast.pollInterval == 0.5
    assert app.scheduler.policy == "execute once"
    assert app.scheduler.pollInterval == 9


def test_extra_indexes_are_created(ops):
    names = set(ops.accountOps.schedulersCollection.index_information())

    assert "accountId_idx" in names


def test_init_reaches_every_engine(db, ops):
    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)
    ops.accountOps.schedulersCollection.update_one(
        {"uid": scheduler.uid}, {"$unset": {"leaseUntil": ""}}   # written before leases existed
    )

    OpsApp(db).init()

    assert ops.accountOps.get(scheduler.uid).leaseUntil is not None


def test_startWorkers_covers_every_engine(db):
    app = OpsApp(db, schedulerPollInterval=WORKER_POLL_INTERVAL, taskPollInterval=WORKER_POLL_INTERVAL)
    a = app.accountOps.add(OpsApp.sync(), hourly(app), accountId=7)
    g = app.groupOps.add(OpsApp.announce(), hourly(app), groupId="g1")
    due(app.accountOps, a)
    due(app.groupOps, g)

    app.startWorkers(taskWorkers=2, schedulerWorkers=1)

    assert wait_for(lambda: app.task.tasksCollection.count_documents({"status": "success"}) == 2)
    results = {Task.model_validate(r).result for r in app.task.tasksCollection.find()}
    assert results == {"synced 7 full=False", "announced g1"}


# --- the write API ---

def test_upsert_creates_then_replaces(ops):
    scheduler = ops.accountOps.build(OpsApp.sync(), hourly(ops), accountId=7)
    ops.accountOps.upsert(scheduler)
    ops.accountOps.upsert(scheduler)

    assert ops.accountOps.count() == 1
    assert ops.accountOps.get(scheduler.uid).accountId == 7


def test_upsert_validates(ops):
    bad = ops.accountOps.build(CallSpec.new("sync", nope=1), hourly(ops), accountId=7)

    with pytest.raises(TaskValidationError):
        ops.accountOps.upsert(bad)

    assert ops.accountOps.count() == 0


def test_find_and_count_filter(ops):
    ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)
    ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=8)

    assert ops.accountOps.count() == 2
    assert ops.accountOps.count({"accountId": 7}) == 1
    assert [s.accountId for s in ops.accountOps.find({"accountId": 8})] == [8]


def test_get_misses_cleanly(ops):
    assert ops.accountOps.get("nope") is None


def test_update_changes_the_work(ops):
    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)
    updated = ops.accountOps.update(scheduler.uid, work=CallSpec.new("sync", full=True))

    assert updated.work.kwargs == {"full": True}
    assert updated.emitWork().kwargs == {"full": True, "accountId": 7}


def test_update_keeps_the_deadline_when_the_rhythm_is_unchanged(ops):
    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)
    updated = ops.accountOps.update(scheduler.uid, work=CallSpec.new("sync", full=True))

    assert abs(updated.deadline - scheduler.deadline) < timedelta(milliseconds=100)


def test_a_new_distribution_restarts_the_rhythm(ops):
    scheduler = ops.accountOps.add(OpsApp.sync(), q_daily := ops.distribution("constant", dailyFrequency=1), accountId=7)
    updated = ops.accountOps.update(scheduler.uid, distribution=hourly(ops))

    assert timedelta(minutes=59) < updated.deadline - utc_now() < timedelta(minutes=61)


def test_update_changes_context_fields(ops):
    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)
    updated = ops.accountOps.update(scheduler.uid, accountId=9)

    assert updated.accountId == 9
    assert updated.emitWork().kwargs == {"accountId": 9}


def test_update_validates_the_merged_scheduler(ops):
    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)

    with pytest.raises(TaskValidationError):
        ops.accountOps.update(scheduler.uid, work=CallSpec.new("sync", nope=1))

    assert ops.accountOps.get(scheduler.uid).work.kwargs == {}  # nothing written


def test_update_toggles_enabled(ops):
    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)

    assert ops.accountOps.update(scheduler.uid, enabled=False).status == "disabled"
    assert ops.accountOps.update(scheduler.uid, enabled=True).status == "enabled"


def test_update_of_a_missing_uid_returns_none(ops):
    assert ops.accountOps.update("nope", enabled=False) is None


def test_delete_and_deleteMany(ops):
    a = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)
    ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=8)
    ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=8)

    assert ops.accountOps.delete(a.uid) is True
    assert ops.accountOps.delete(a.uid) is False
    assert ops.accountOps.deleteMany({"accountId": 8}) == 2
    assert ops.accountOps.count() == 0


# --- ensure on a typed engine ---

def test_ensure_takes_context_fields(ops):
    scheduler = ops.accountOps.ensure(
        "sync-7", OpsApp.sync(), hourly(ops), accountId=7
    )

    assert scheduler.accountId == 7
    assert scheduler.uid == schedulerUid("sync-7")
    assert scheduler.emitWork().kwargs == {"accountId": 7}


def test_ensure_is_still_idempotent_with_context(db):
    for _ in range(3):
        app = OpsApp(db)
        app.accountOps.ensure("sync-7", OpsApp.sync(), hourly(app), accountId=7)

    assert app.accountOps.count() == 1


def test_ensure_can_change_the_context(db):
    first = OpsApp(db)
    first.accountOps.ensure("sync-7", OpsApp.sync(), hourly(first), accountId=7)

    second = OpsApp(db)
    updated = second.accountOps.ensure("sync-7", OpsApp.sync(), hourly(second), accountId=9)

    assert updated.accountId == 9
    assert second.accountOps.count() == 1


def test_ensure_validates_through_emitWork(ops):
    with pytest.raises(TaskValidationError):
        ops.globalOps.ensure("bad", OpsApp.sync(), hourly(ops))  # no accountId anywhere


def test_a_declared_scheduler_fires_with_context(ops, tasks):
    scheduler = ops.accountOps.ensure("sync-7", OpsApp.sync(), hourly(ops), accountId=7)
    due(ops.accountOps, scheduler)
    ops.accountOps._work()

    assert Task.model_validate(tasks.find_one()).work.kwargs == {"accountId": 7}


# --- passing collection objects directly, the way an app with its own db module does ---

def test_an_engine_accepts_a_collection_object(db):
    operations = db["my_operations"]

    class Q(BaseApp):
        ops = schedulers(AccountScheduler, operations)

        @task
        @staticmethod
        def sync(accountId: int): ...

    app = Q(db)
    app.ops.add(CallSpec.new("sync"), app.distribution("constant", dailyFrequency=1), accountId=7)

    assert app.ops.schedulersCollection is operations
    assert operations.count_documents({}) == 1


def test_a_pile_accepts_a_collection_object(db):
    from pymonque import pile

    items = db["my_items"]

    class Q(BaseApp):
        things = pile(itemsCollection=items)

    app = Q(db)
    app.things.add({"n": 1})

    assert app.things.itemsCollection is items
    assert items.count_documents({}) == 1


def test_declaration_repr():
    assert repr(OpsApp.accountOps) == "schedulers accountOps (AccountScheduler)"


def test_an_invalid_scheduler_update_writes_nothing(ops):
    from pydantic import ValidationError

    scheduler = ops.accountOps.add(OpsApp.sync(), hourly(ops), accountId=7)

    with pytest.raises(ValidationError):
        ops.accountOps.update(scheduler.uid, accountId="not a number")

    assert ops.accountOps.collection.find_one({"uid": scheduler.uid})["accountId"] == 7
    assert ops.accountOps.get(scheduler.uid).accountId == 7      # still loads
