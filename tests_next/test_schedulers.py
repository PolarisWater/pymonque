"""The scheduler engine: adding schedulers, firing them on their rhythm, missed beats, emitting a beat
exactly once, keeping a declared scheduler in step with ensure(), changing stored schedulers, and
schedulers whose fields reach the tasks they emit."""

import logging
import threading
from datetime import timedelta
from typing import Any

import pytest
from pydantic import ValidationError
from pymongo import IndexModel

from pymonque_next import (
    BaseDistributions, CallSpec, CollectionEngine, DistributionEngine, Scheduler, Task, TaskLimits, utc_now,
)
from pymonque_next.exceptions import DistributionNotFound, DistributionValidationError, TaskNotFound, TaskValidationError
from pymonque_next.schedulers import schedulerUid


def greet(name: str = "Ada") -> str:
    return f"Hello, {name}!"


def sync(accountId: int, full: bool = False) -> str:
    return f"synced {accountId} full={full}"


FUNCTIONS = {"greet": greet, "sync": sync}


@pytest.fixture
def tasks(taskEngine):
    return taskEngine(FUNCTIONS)


@pytest.fixture
def engine(schedulerEngine, tasks):
    return schedulerEngine(tasks=tasks)


def hourly(engine) -> CallSpec:
    return engine.distributions("constant", dailyFrequency=24)


def due(engine, scheduler, at=None):
    """Move a scheduler's beat into the past, as waiting for it would."""

    at = at or utc_now() - timedelta(minutes=1)
    engine.collection.update_one({"uid": scheduler.uid}, {"$set": {"deadline": at, "leaseUntil": at}})

    return engine.get(scheduler.uid)


def added(engine, name="Ada", dailyFrequency=24):
    return engine.add(CallSpec.new("greet", name=name), engine.distributions("constant", dailyFrequency=dailyFrequency))


# --- adding ---

def test_add_stores_an_enabled_scheduler_one_interval_out(engine):
    before = utc_now()
    scheduler = engine.get(added(engine).uid)

    assert scheduler.status == "enabled"
    assert (scheduler.work, scheduler.distribution.kwargs) == (CallSpec.new("greet", name="Ada"), {"dailyFrequency": 24})
    assert timedelta(minutes=59) < scheduler.deadline - before < timedelta(minutes=61)


@pytest.mark.parametrize("work, distribution, error", [
    (CallSpec.new("nope"), CallSpec.new("constant", dailyFrequency=1), TaskNotFound),
    (CallSpec.new("greet", nope=1), CallSpec.new("constant", dailyFrequency=1), TaskValidationError),
    (CallSpec.new("greet"), CallSpec.new("nope"), DistributionNotFound),
])
def test_add_refuses_what_would_not_run_and_stores_nothing(engine, work, distribution, error):
    with pytest.raises(error):
        engine.add(work, distribution)

    assert engine.count() == 0


def test_a_scheduler_cannot_be_built_on_a_dead_interval(taskEngine, schedulerEngine):
    class Stuck(BaseDistributions):
        @staticmethod
        def stuck(dailyFrequency: float) -> timedelta:
            return timedelta(0)

    engine = schedulerEngine(tasks=taskEngine(FUNCTIONS, distributions=DistributionEngine(Stuck)))

    with pytest.raises(DistributionValidationError):
        engine.add(CallSpec.new("greet"), CallSpec.new("stuck", dailyFrequency=1))

    assert engine.count() == 0


def test_the_engine_refuses_a_model_that_is_not_a_scheduler(schedulerEngine, tasks):
    with pytest.raises(TypeError, match="Scheduler"):
        schedulerEngine(Task, tasks=tasks)


def test_the_engine_draws_from_its_task_engines_distributions(engine, tasks):
    assert engine.distributions is tasks.distributions


# --- firing ---

def test_a_due_scheduler_emits_its_work_as_a_task_pointing_back_at_it(engine, tasks):
    scheduler = due(engine, added(engine))

    assert engine.work().uid == scheduler.uid

    emitted = tasks.find()[0]

    assert (emitted.status, emitted.work) == ("pending", scheduler.work)
    assert (emitted.factory.uid, emitted.factory.name) == (scheduler.uid, "Scheduler")
    assert emitted.deadline == scheduler.deadline


def test_an_emitted_task_runs(engine, tasks):
    due(engine, added(engine))
    engine.work()

    assert tasks.work().result == "Hello, Ada!"


def test_a_future_scheduler_stays_put(engine, tasks):
    scheduler = added(engine)

    assert engine.work() is None
    assert tasks.count() == 0
    assert engine.get(scheduler.uid).deadline == engine.get(scheduler.uid).leaseUntil


def test_working_an_empty_engine_does_nothing(engine):
    assert engine.work() is None


def test_the_most_overdue_scheduler_fires_first(engine, tasks):
    now = utc_now()
    due(engine, added(engine, name="second"), now - timedelta(minutes=1))
    due(engine, added(engine, name="first"), now - timedelta(minutes=30))
    engine.work()

    assert tasks.find()[0].work.kwargs == {"name": "first"}


def test_firing_moves_the_deadline_one_interval_on_from_the_beat(engine):
    scheduler = due(engine, added(engine))
    engine.work()

    # measured from the beat, not from now, so the rhythm does not drift
    assert engine.get(scheduler.uid).deadline == scheduler.deadline + timedelta(hours=1)


def test_a_fired_scheduler_stays_enabled_is_let_go_and_is_not_claimed_twice(engine, tasks):
    scheduler = due(engine, added(engine))
    engine.work()
    fired = engine.get(scheduler.uid)

    assert (fired.status, fired.claimId) == ("enabled", None)
    assert fired.leaseUntil == fired.deadline
    assert engine.work() is None
    assert tasks.count() == 1


def test_a_scheduler_is_held_under_a_claim_of_its_own_while_it_emits(engine, tasks, monkeypatch):
    scheduler = due(engine, added(engine))
    seen = []
    insert = tasks.insert

    def emitting(task):
        seen.append(engine.collection.find_one({"uid": scheduler.uid})["claimId"])

        return insert(task)

    monkeypatch.setattr(tasks, "insert", emitting)
    engine.work()

    assert seen and seen[0] is not None
    assert engine.get(scheduler.uid).claimId is None


def test_a_disabled_scheduler_walks_its_deadline_without_emitting_or_warning(engine, tasks, caplog):
    scheduler = added(engine)
    engine.update(scheduler.uid, enabled=False)
    due(engine, scheduler, utc_now() - timedelta(hours=3))

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        engine.work()

    walked = engine.get(scheduler.uid)

    assert tasks.count() == 0
    assert walked.status == "disabled" and walked.deadline > utc_now()
    assert caplog.text == ""


def test_a_scheduler_whose_task_is_gone_skips_the_beat_and_stays_enabled(engine, taskEngine, schedulerEngine, caplog):
    scheduler = due(engine, added(engine))
    smaller = schedulerEngine(tasks=taskEngine({"sync": sync}))     # the same collections, without greet

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        smaller.work()

    skipped = engine.get(scheduler.uid)

    assert skipped.status == "enabled"
    assert skipped.deadline > scheduler.deadline
    assert smaller.tasks.count() == 0
    assert "beat was skipped" in caplog.text


def test_a_scheduler_whose_beat_needs_a_field_it_does_not_give_skips_the_beat_and_walks_on(engine, taskEngine, schedulerEngine, caplog):
    scheduler = due(engine, added(engine))                                          # stored against a plain Task
    redeployed = schedulerEngine(tasks=taskEngine(FUNCTIONS, AccountTask))         # the task model now needs accountId

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        assert redeployed.work() is not None

    after = engine.get(scheduler.uid)

    assert (after.status, after.claimId) == ("enabled", None)
    assert after.deadline > scheduler.deadline
    assert redeployed.tasks.count() == 0
    assert "beat was skipped" in caplog.text and "accountId" in caplog.text


def test_a_scheduler_giving_a_field_the_task_no_longer_has_skips_the_beat_and_walks_on(accountOps, taskEngine, schedulerEngine, caplog):
    scheduler = due(accountOps, accountOps.add(CallSpec.new("sync"), hourly(accountOps), accountId=7))
    redeployed = schedulerEngine(AccountScheduler, name="accountOps", tasks=taskEngine(FUNCTIONS, name="accountTasks"))

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        assert redeployed.work() is not None

    after = accountOps.get(scheduler.uid)

    assert (after.status, after.claimId) == ("enabled", None)
    assert after.deadline > scheduler.deadline
    assert "beat was skipped" in caplog.text and "accountId" in caplog.text


def test_a_lapsed_lease_is_claimable_and_a_live_one_is_left_alone(engine, tasks):
    scheduler = added(engine)
    engine.collection.update_one({"uid": scheduler.uid}, {"$set": {"leaseUntil": utc_now() + timedelta(minutes=5), "deadline": utc_now() - timedelta(hours=2)}})

    assert engine.work() is None        # overdue, but somebody holds it

    engine.collection.update_one({"uid": scheduler.uid}, {"$set": {"leaseUntil": utc_now() - timedelta(seconds=1)}})

    assert engine.work() is not None    # its holder stopped: picked up with no restart
    assert tasks.count() == 1


def test_indexes_back_the_claim_query_beside_extra_ones(schedulerEngine, tasks):
    engine = schedulerEngine(tasks=tasks, extraIndexes=[IndexModel([("name", 1)], name="byName")])

    assert {"leaseUntil_1", "uid_1", "byName"} <= set(engine.collection.index_information())


def test_an_emitted_task_goes_stale_by_its_tasks_limit(schedulerEngine, taskEngine):
    tasks = taskEngine(FUNCTIONS, limits={"greet": TaskLimits(skipAfter=60), "sync": TaskLimits()})
    engine = schedulerEngine(tasks=tasks, missed="once")
    due(engine, added(engine), utc_now() - timedelta(seconds=300))
    engine.work()

    assert tasks.work().status == "outdated"


# --- missed beats ---

def behindByADay(schedulerEngine, tasks, missed):
    """An hourly scheduler nobody has served for a day: 24 beats missed."""

    engine = schedulerEngine(tasks=tasks, missed=missed)

    return engine, due(engine, added(engine), utc_now() - timedelta(days=1))


def test_replay_walks_the_backlog_beat_by_beat(schedulerEngine, tasks):
    engine, scheduler = behindByADay(schedulerEngine, tasks, "replay")
    engine.work()
    after = engine.get(scheduler.uid)

    assert tasks.count() == 1
    assert after.deadline == scheduler.deadline + timedelta(hours=1)
    assert after.deadline < utc_now()       # 23 more still owed


def test_once_emits_one_run_and_resumes_from_now(schedulerEngine, tasks):
    engine, scheduler = behindByADay(schedulerEngine, tasks, "once")
    engine.work()

    assert tasks.count() == 1
    assert timedelta(minutes=59) < engine.get(scheduler.uid).deadline - utc_now() < timedelta(minutes=61)


def test_skip_emits_nothing_and_resumes_from_now(schedulerEngine, tasks):
    engine, scheduler = behindByADay(schedulerEngine, tasks, "skip")
    engine.work()

    assert tasks.count() == 0
    assert timedelta(minutes=59) < engine.get(scheduler.uid).deadline - utc_now() < timedelta(minutes=61)


@pytest.mark.parametrize("missed", ["replay", "once", "skip"])
def test_a_scheduler_merely_due_emits_whatever_missed_says(schedulerEngine, tasks, missed):
    engine = schedulerEngine(tasks=tasks, missed=missed)
    scheduler = due(engine, added(engine), utc_now() - timedelta(seconds=1))
    engine.work()

    assert tasks.count() == 1
    assert engine.get(scheduler.uid).deadline == scheduler.deadline + timedelta(hours=1)


def test_a_missed_beat_warns_and_a_merely_due_one_does_not(engine, caplog):
    behind = due(engine, added(engine, name="behind"), utc_now() - timedelta(days=1))

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        engine.work()

    assert "missed a beat" in caplog.text

    caplog.clear()
    engine.delete(behind.uid)
    due(engine, added(engine, name="due"), utc_now() - timedelta(seconds=1))

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        engine.work()

    assert caplog.text == ""


def test_a_scheduler_faster_than_it_can_be_served_stops_accumulating(schedulerEngine, tasks):
    engine = schedulerEngine(tasks=tasks, missed="once")
    scheduler = due(engine, added(engine, dailyFrequency=86400), utc_now() - timedelta(hours=1))      # a second apart

    for _ in range(20):
        engine.work()

    assert tasks.count() == 1       # not 20, and not 3600
    assert engine.get(scheduler.uid).deadline > utc_now()


# --- a beat, exactly once ---

def test_a_beat_reclaimed_after_a_crash_is_not_emitted_twice(engine, tasks):
    scheduler = due(engine, added(engine))
    engine.work()
    due(engine, scheduler, scheduler.deadline)      # the worker died after emitting, before moving the deadline on
    engine.work()

    assert tasks.count() == 1


def test_a_rhythm_restarted_mid_claim_is_not_overwritten(engine, tasks, monkeypatch):
    declared = engine.ensure("nightly", CallSpec.new("greet"), hourly(engine))
    due(engine, declared)
    insert = tasks.insert

    def redeclaredWhileClaimed(task):
        stored = insert(task)
        engine.ensure("nightly", CallSpec.new("greet"), engine.distributions("constant", dailyFrequency=1))

        return stored

    monkeypatch.setattr(tasks, "insert", redeclaredWhileClaimed)
    engine.work()

    assert engine.get(declared.uid).deadline - utc_now() > timedelta(hours=23)


def test_moving_the_deadline_by_hand_moves_the_lease(engine):
    later = (utc_now() + timedelta(days=30)).replace(microsecond=0)
    saved = engine.get(added(engine, name="saved").uid)
    saved.deadline = later
    saved.save()
    updated = engine.update(added(engine, name="updated").uid, deadline=later)

    assert engine.get(saved.uid).leaseUntil == later
    assert (updated.deadline, updated.leaseUntil) == (later, later)


def test_moving_a_held_schedulers_deadline_by_hand_releases_its_claim(engine):
    def held(scheduler):
        engine.collection.update_one({"uid": scheduler.uid}, {"$set": {"claimId": "a-worker"}})

    updated = added(engine, name="updated")
    held(updated)

    assert engine.update(updated.uid, deadline=utc_now() + timedelta(days=1)).claimId is None

    declared = engine.ensure("nightly", CallSpec.new("greet"), engine.distributions("constant", dailyFrequency=1))
    held(declared)

    assert engine.ensure("nightly", CallSpec.new("greet", name="Grace"), engine.distributions("constant", dailyFrequency=1)).claimId == "a-worker"
    assert engine.ensure("nightly", CallSpec.new("greet"), hourly(engine)).claimId is None       # a new rhythm moved it


# --- ensure ---

def declare(engine, name="nightly", to="Ada", dailyFrequency=1, enabled=None, **fields):
    return engine.ensure(
        name, CallSpec.new("greet", name=to), engine.distributions("constant", dailyFrequency=dailyFrequency),
        enabled=enabled, **fields,
    )


def test_ensure_creates_a_scheduler_under_a_uid_from_its_name(engine):
    scheduler = declare(engine)

    assert engine.count() == 1
    assert (scheduler.uid, scheduler.name, scheduler.status) == (schedulerUid("nightly"), "nightly", "enabled")
    assert scheduler.work.kwargs == {"name": "Ada"}


def test_ensure_is_idempotent_across_restarts(schedulerEngine, taskEngine):
    for _ in range(5):
        declare(schedulerEngine(tasks=taskEngine(FUNCTIONS)))

    assert schedulerEngine(tasks=taskEngine(FUNCTIONS)).count() == 1


def test_the_same_name_gives_the_same_uid_anywhere():
    assert schedulerUid("nightly") == schedulerUid("nightly") != schedulerUid("hourly")


def test_different_names_are_different_schedulers(engine):
    declare(engine, name="nightly")
    declare(engine, name="hourly")

    assert engine.count() == 2


def test_ensure_keeps_the_deadline_and_updates_changed_work(engine):
    first = declare(engine, to="Ada")
    again = declare(engine, to="Grace")

    assert again.work.kwargs == {"name": "Grace"}
    assert again.deadline == first.deadline


def test_a_new_distribution_restarts_the_rhythm(engine):
    declare(engine, dailyFrequency=1)
    updated = declare(engine, dailyFrequency=24)

    assert updated.distribution.kwargs == {"dailyFrequency": 24}
    assert timedelta(minutes=59) < updated.deadline - utc_now() < timedelta(minutes=61)


def test_ensure_leaves_a_disabled_scheduler_disabled_unless_told(engine):
    scheduler = declare(engine)
    engine.update(scheduler.uid, enabled=False)

    assert declare(engine).status == "disabled"
    assert declare(engine, enabled=True).status == "enabled"
    assert declare(engine, enabled=False).status == "disabled"


def test_ensure_can_create_a_disabled_scheduler(engine):
    assert declare(engine, enabled=False).status == "disabled"


def test_ensure_checks_before_storing(engine):
    with pytest.raises(TaskNotFound):
        engine.ensure("bad", CallSpec.new("nope"), hourly(engine))

    assert engine.count() == 0


def test_a_declared_scheduler_fires_like_any_other(engine, tasks):
    due(engine, declare(engine))
    engine.work()

    assert tasks.find()[0].factory.name == "nightly"


def test_concurrent_declarations_create_one_scheduler(schedulerEngine, taskEngine):
    engines = [schedulerEngine(tasks=taskEngine(FUNCTIONS)) for _ in range(8)]
    barrier = threading.Barrier(len(engines))

    def declareFrom(engine):
        barrier.wait()
        declare(engine)

    threads = [threading.Thread(target=declareFrom, args=(engine,)) for engine in engines]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(10)

    assert engines[0].count() == 1


def test_a_declared_scheduler_is_found_and_removed_by_name_and_can_be_declared_again(engine):
    declare(engine)

    assert engine.byName("nightly").name == "nightly"
    assert engine.byName("never-declared") is None
    assert engine.removeNamed("nightly") is True
    assert engine.removeNamed("nightly") is False

    declare(engine)

    assert engine.count() == 1


def test_added_and_declared_schedulers_coexist_and_unnamed_ones_do_not_collide(engine):
    added(engine, name="a")
    added(engine, name="b")
    declare(engine)

    assert engine.count() == 3
    assert engine.count({"name": "Scheduler"}) == 2


@pytest.mark.parametrize("field, message", [
    ("uid", "the engine keeps it"),
    ("claimId", "the engine keeps it"),
    ("leaseUntil", "the engine keeps it"),
    ("status", "enabled="),
])
def test_a_field_the_engine_keeps_is_refused_wherever_fields_are_given(engine, field, message):
    stored = added(engine)
    calls = {
        "build":    lambda: engine.build(CallSpec.new("greet"), hourly(engine), **{field: "x"}),
        "add":      lambda: engine.add(CallSpec.new("greet"), hourly(engine), **{field: "x"}),
        "ensure":   lambda: engine.ensure("nightly", CallSpec.new("greet"), hourly(engine), **{field: "x"}),
        "update":   lambda: engine.update(stored.uid, **{field: "x"}),
    }

    for verb, call in calls.items():
        with pytest.raises(TypeError, match=message):
            call()

    assert engine.count() == 1
    assert engine.get(stored.uid).status == "enabled"


def test_a_field_the_scheduler_does_not_have_is_refused(engine):
    with pytest.raises(TypeError, match="Scheduler has no field accountId"):
        engine.add(CallSpec.new("greet"), hourly(engine), accountId=7)

    assert engine.count() == 0


def test_a_scheduler_is_still_named_and_its_deadline_moved_by_hand(engine):
    later = (utc_now() + timedelta(days=2)).replace(microsecond=0)
    scheduler = engine.add(CallSpec.new("greet"), hourly(engine), name="greeter")
    updated = engine.update(scheduler.uid, name="renamed", deadline=later)

    assert (scheduler.name, updated.name, updated.deadline) == ("greeter", "renamed", later)


# --- changing stored schedulers ---

def test_upsert_creates_then_replaces_and_checks_first(engine):
    scheduler = engine.build(CallSpec.new("greet"), hourly(engine))
    engine.upsert(scheduler)
    engine.upsert(scheduler)

    assert engine.count() == 1

    with pytest.raises(TaskValidationError):
        engine.upsert(engine.build(CallSpec.new("greet", nope=1), hourly(engine)))

    assert engine.count() == 1


def test_update_changes_the_work_and_keeps_the_rhythm(engine):
    scheduler = added(engine)
    updated = engine.update(scheduler.uid, work=CallSpec.new("greet", name="Grace"))

    assert updated.work.kwargs == {"name": "Grace"}
    assert updated.deadline == engine.collection.find_one({"uid": scheduler.uid})["deadline"]
    assert abs(updated.deadline - scheduler.deadline) < timedelta(milliseconds=1)


def test_update_with_a_new_distribution_restarts_the_rhythm(engine):
    scheduler = engine.add(CallSpec.new("greet"), engine.distributions("constant", dailyFrequency=1))
    updated = engine.update(scheduler.uid, distribution=hourly(engine))

    assert timedelta(minutes=59) < updated.deadline - utc_now() < timedelta(minutes=61)


def test_update_toggles_enabled(engine):
    scheduler = added(engine)

    assert engine.update(scheduler.uid, enabled=False).status == "disabled"
    assert engine.update(scheduler.uid, enabled=True).status == "enabled"


def test_update_checks_the_merged_scheduler_and_writes_nothing_if_it_would_not_run(engine):
    scheduler = added(engine)

    with pytest.raises(TaskValidationError):
        engine.update(scheduler.uid, work=CallSpec.new("greet", nope=1))

    assert engine.get(scheduler.uid).work.kwargs == {"name": "Ada"}


def test_update_of_a_missing_uid_gives_none(engine):
    assert engine.update("nope", enabled=False) is None


def test_a_scheduler_is_a_collection_engine_document(engine):
    a = added(engine, name="a")
    added(engine, name="b")

    assert isinstance(engine, CollectionEngine)
    assert engine.count({"work.kwargs.name": "a"}) == 1
    assert engine.get("nope") is None
    assert engine.delete(a.uid) is True and engine.deleteMany({}) == 1


# --- schedulers whose fields reach the tasks they emit ---

class AccountScheduler(Scheduler):
    accountId: int
    note: str | None = None

    def taskFields(self) -> dict[str, Any]:
        return {"accountId": self.accountId}


class AccountTask(Task):
    accountId: int

    def runWork(self) -> CallSpec:
        return self.work.bind(accountId=self.accountId)


@pytest.fixture
def accountTasks(taskEngine):
    return taskEngine(FUNCTIONS, AccountTask, name="accountTasks")


@pytest.fixture
def accountOps(schedulerEngine, accountTasks):
    return schedulerEngine(AccountScheduler, name="accountOps", tasks=accountTasks, extraIndexes=[
        IndexModel([("accountId", 1)], name="byAccount"),
    ])


def test_a_schedulers_fields_reach_the_task_it_emits_and_the_task_runs(accountOps, accountTasks):
    scheduler = due(accountOps, accountOps.add(CallSpec.new("sync"), hourly(accountOps), accountId=7))
    accountOps.work()
    emitted = accountTasks.find()[0]

    assert isinstance(emitted, AccountTask) and emitted.accountId == 7
    assert emitted.work.kwargs == {}                                    # stamped by the task, not stored
    assert accountOps.get(scheduler.uid).work.kwargs == {}              # nor on the scheduler
    assert accountTasks.work().result == "synced 7 full=False"


def test_a_scheduler_is_checked_as_its_task_will_run(accountOps):
    # sync requires accountId, which only the emitted task supplies
    assert accountOps.add(CallSpec.new("sync"), hourly(accountOps), accountId=7).accountId == 7


def test_the_same_work_is_refused_where_nothing_supplies_the_context(engine):
    with pytest.raises(TaskValidationError):
        engine.add(CallSpec.new("sync"), hourly(engine))


def test_a_scheduler_with_context_is_refused_by_a_task_engine_that_cannot_store_it(schedulerEngine, tasks):
    plainTasks = schedulerEngine(AccountScheduler, name="accountsIntoPlain", tasks=tasks)

    with pytest.raises(TypeError, match="Task has no field accountId"):
        plainTasks.add(CallSpec.new("sync"), hourly(plainTasks), accountId=7)


def test_a_task_engine_needing_context_refuses_a_scheduler_that_gives_none(schedulerEngine, accountTasks):
    plainOps = schedulerEngine(name="plainIntoAccounts", tasks=accountTasks)

    with pytest.raises(ValidationError, match="accountId"):
        plainOps.add(CallSpec.new("greet"), hourly(plainOps))


@pytest.mark.parametrize("fields", [{}, {"accountId": "not a number"}])
def test_a_missing_or_invalid_scheduler_field_is_refused(accountOps, fields):
    with pytest.raises(ValidationError):
        accountOps.add(CallSpec.new("sync"), hourly(accountOps), **fields)

    assert accountOps.count() == 0


def test_each_engine_reads_its_own_model_and_the_fields_survive_a_fire(accountOps, engine):
    scheduler = due(accountOps, accountOps.add(CallSpec.new("sync"), hourly(accountOps), accountId=7))
    added(engine)
    accountOps.work()

    assert isinstance(accountOps.get(scheduler.uid), AccountScheduler)
    assert accountOps.get(scheduler.uid).accountId == 7
    assert type(engine.find()[0]) is Scheduler


def test_ensure_takes_fields_and_can_change_them(accountOps):
    first = accountOps.ensure("sync-7", CallSpec.new("sync"), hourly(accountOps), accountId=7)
    again = accountOps.ensure("sync-7", CallSpec.new("sync"), hourly(accountOps), accountId=9)

    assert (first.accountId, again.accountId, again.uid) == (7, 9, schedulerUid("sync-7"))
    assert accountOps.count() == 1


def test_update_changes_fields_and_writes_nothing_invalid(accountOps):
    scheduler = accountOps.add(CallSpec.new("sync"), hourly(accountOps), accountId=7, note="hello")

    assert accountOps.update(scheduler.uid, accountId=9).accountId == 9
    assert accountOps.update(scheduler.uid, note=None).note is None

    with pytest.raises(ValidationError):
        accountOps.update(scheduler.uid, accountId="not a number")

    assert accountOps.collection.find_one({"uid": scheduler.uid})["accountId"] == 9


def test_extra_indexes_are_created(accountOps):
    assert "byAccount" in accountOps.collection.index_information()


def test_a_scheduler_engine_emits_into_the_task_engine_it_is_given(schedulerEngine, taskEngine):
    light, heavy = taskEngine(FUNCTIONS, name="light"), taskEngine(FUNCTIONS, name="heavy")
    nightly = schedulerEngine(name="nightly", tasks=heavy)
    due(nightly, added(nightly))
    nightly.work()

    assert (light.count(), heavy.count()) == (0, 1)


def test_ensure_with_fields_stays_idempotent_and_is_checked_through_the_task_it_emits(accountOps, engine):
    for _ in range(3):
        accountOps.ensure("sync-7", CallSpec.new("sync"), hourly(accountOps), accountId=7)

    assert accountOps.count() == 1

    with pytest.raises(TaskValidationError):
        engine.ensure("sync-nobody", CallSpec.new("sync"), hourly(engine))     # nothing supplies accountId

    assert engine.count() == 0


# --- the whole loop ---

def test_a_scheduler_fires_a_task_that_drains_a_pile(schedulerEngine, taskEngine, pileEngine):
    outbox = pileEngine(name="outbox")

    def sendOne() -> str:
        with outbox.work() as w:
            if w is None:
                return "empty"

            return f"sent to {w.data['to']}"

    tasks = taskEngine({"sendOne": sendOne})
    engine = schedulerEngine(tasks=tasks)
    outbox.addMany([{"to": "a@b.c"}, {"to": "d@e.f"}])
    scheduler = engine.add(CallSpec.new("sendOne"), hourly(engine))

    for beat in range(2):
        due(engine, scheduler, utc_now() - timedelta(minutes=beat + 1))
        engine.work()
        tasks.work()

    assert sorted(task.result for task in tasks.find()) == ["sent to a@b.c", "sent to d@e.f"]
    assert outbox.counts() == {"pending": 0, "running": 0, "done": 2, "failed": 0, "canceled": 0}
