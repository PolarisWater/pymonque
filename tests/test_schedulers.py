"""SchedulerEngine: emitting on a rhythm, advancing deadlines, and falling behind."""

import logging
from datetime import timedelta

import pytest

from pymonque import BaseApp, CallSpec, Scheduler, Task, task, utc_now
from pymonque.exceptions import TaskNotFound, DistributionNotFound

from conftest import ExampleApp, appWith


HOURLY = {"functionName": "constant", "kwargs": {"dailyFrequency": 24}}


def addScheduler(app, *, name="Ada", dailyFrequency=24) -> Scheduler:
    app.scheduler.add(
        ExampleApp.greet(name=name),
        app.distribution("constant", dailyFrequency=dailyFrequency),
    )
    return Scheduler.model_validate(
        app.scheduler.schedulersCollection.find_one({"work.kwargs.name": name})
    )


def reload(app, scheduler: Scheduler) -> Scheduler:
    return Scheduler.model_validate(
        app.scheduler.schedulersCollection.find_one({"uid": scheduler.uid})
    )


def setDeadline(app, scheduler: Scheduler, deadline, engine=None):
    engine = engine or app.scheduler
    # a scheduler that is not being worked is claimable exactly at its deadline
    engine.schedulersCollection.update_one(
        {"uid": scheduler.uid}, {"$set": {"deadline": deadline, "leaseUntil": deadline}}
    )


# --- adding ---

def test_add_stores_an_enabled_scheduler(app):
    scheduler = addScheduler(app)

    assert scheduler.status == "enabled"
    assert scheduler.work.functionName == "greet"
    assert scheduler.distribution.kwargs == {"dailyFrequency": 24}


def test_the_first_deadline_is_one_interval_out(app):
    before = utc_now()
    scheduler = addScheduler(app, dailyFrequency=24)

    assert timedelta(minutes=59) < scheduler.deadline - before < timedelta(minutes=61)


def test_add_rejects_an_unknown_task(app, schedulers):
    with pytest.raises(TaskNotFound):
        app.scheduler.add(CallSpec.new("does_not_exist"), app.distribution("constant", dailyFrequency=1))

    assert schedulers.count_documents({}) == 0


def test_add_rejects_an_unknown_distribution(app, schedulers):
    with pytest.raises(DistributionNotFound):
        app.scheduler.add(ExampleApp.greet(name="Ada"), CallSpec.new("does_not_exist"))

    assert schedulers.count_documents({}) == 0


# --- firing ---

def test_a_due_scheduler_emits_a_task(app, tasks):
    scheduler = addScheduler(app)
    setDeadline(app, scheduler, utc_now() - timedelta(minutes=1))
    app.scheduler._work()
    emitted = Task.model_validate(tasks.find_one())

    assert emitted.work == scheduler.work
    assert emitted.status == "pending"
    assert emitted.factory.uid == scheduler.uid  # the task points back at its scheduler


def test_a_future_scheduler_stays_put(app, tasks):
    scheduler = addScheduler(app)
    app.scheduler._work()

    assert tasks.count_documents({}) == 0
    assert reload(app, scheduler).deadline == scheduler.deadline


def test_firing_advances_the_deadline_by_one_interval(app):
    scheduler = addScheduler(app, dailyFrequency=24)
    due = utc_now() - timedelta(minutes=1)
    setDeadline(app, scheduler, due)
    app.scheduler._work()

    # measured from the old deadline, not from now, so the rhythm does not drift
    assert abs(reload(app, scheduler).deadline - (due + timedelta(hours=1))) < timedelta(milliseconds=100)


def test_the_scheduler_returns_to_enabled(app):
    scheduler = addScheduler(app)
    setDeadline(app, scheduler, utc_now() - timedelta(minutes=1))
    app.scheduler._work()

    assert reload(app, scheduler).status == "enabled"


def test_a_disabled_scheduler_advances_without_emitting(app, tasks):
    scheduler = addScheduler(app)
    due = utc_now() - timedelta(minutes=1)
    app.scheduler.schedulersCollection.update_one(
        {"uid": scheduler.uid},
        {"$set": {"status": "disabled", "deadline": due, "leaseUntil": due}},
    )
    app.scheduler._work()
    after = reload(app, scheduler)

    assert tasks.count_documents({}) == 0
    assert after.status == "disabled"
    assert after.deadline > utc_now()


def test_a_scheduler_is_never_claimed_twice(app, tasks):
    scheduler = addScheduler(app)
    setDeadline(app, scheduler, utc_now() - timedelta(minutes=1))

    app.scheduler._work()
    app.scheduler._work()  # the deadline has moved past now

    assert tasks.count_documents({}) == 1


def test_working_an_empty_collection_is_a_no_op(app):
    assert app.scheduler._work() is None


def test_the_earliest_deadline_fires_first(app, tasks):
    now = utc_now()
    first = addScheduler(app, name="first")
    second = addScheduler(app, name="second")
    setDeadline(app, first, now - timedelta(hours=1))
    setDeadline(app, second, now - timedelta(minutes=1))
    app.scheduler._work()

    assert tasks.find_one()["work"]["kwargs"]["name"] == "first"


def test_a_scheduler_that_missed_a_beat_warns(app, caplog):
    scheduler = addScheduler(app, dailyFrequency=86400)  # one second apart
    setDeadline(app, scheduler, utc_now() - timedelta(days=1))

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        app.scheduler._work()

    assert "missed a beat" in caplog.text


def test_a_disabled_scheduler_walks_its_deadline_without_warning(app, tasks, caplog):
    """It keeps its distribution's shape for when it is enabled again, but owes nothing."""

    scheduler = addScheduler(app)                    # hourly
    setDeadline(app, scheduler, utc_now() - timedelta(hours=3))
    app.scheduler.collection.update_one({"uid": scheduler.uid}, {"$set": {"status": "disabled"}})

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        app.scheduler._work()

    assert caplog.text == ""
    assert tasks.count_documents({}) == 0
    assert app.scheduler.get(scheduler.uid).deadline > utc_now()


def test_a_scheduler_that_is_merely_due_does_not_warn(app, caplog):
    scheduler = addScheduler(app)                    # hourly
    setDeadline(app, scheduler, utc_now() - timedelta(seconds=1))

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        app.scheduler._work()

    assert caplog.text == ""


def test_an_emitted_task_is_runnable(app, tasks):
    scheduler = addScheduler(app)
    setDeadline(app, scheduler, utc_now() - timedelta(minutes=1))
    app.scheduler._work()
    app.task._work()

    assert Task.model_validate(tasks.find_one()).result == "Hello, Ada!"


# --- indexes ---

def test_indexes_back_the_claim_query(app, schedulers):
    keys = [tuple(index["key"]) for index in schedulers.index_information().values()]

    assert (("status", 1), ("leaseUntil", 1)) in keys
    assert (("uid", 1),) in keys


# --- overdue policies, applied every time a scheduler is claimed ---

def test_an_abandoned_scheduler_is_reclaimed_when_its_lease_lapses(db, app, tasks):
    scheduler = addScheduler(app)
    # a worker claimed it and died: its lease is long expired
    app.scheduler.schedulersCollection.update_one(
        {"uid": scheduler.uid},
        {"$set": {"leaseUntil": utc_now() - timedelta(hours=1)}},
    )

    assert app.scheduler._work() is not None      # picked up without any restart
    assert tasks.count_documents({}) == 1


def test_a_scheduler_with_a_live_lease_is_left_alone(db, app):
    scheduler = addScheduler(app)
    held = utc_now() + timedelta(minutes=5)
    app.scheduler.schedulersCollection.update_one(
        {"uid": scheduler.uid},
        {"$set": {"deadline": utc_now() - timedelta(hours=1), "leaseUntil": held}},
    )

    assert app.scheduler._work() is None          # overdue, but somebody else holds it

    ExampleApp(db)                            # and constructing a app does not touch it
    assert abs(reload(app, scheduler).leaseUntil - held) < timedelta(milliseconds=100)


def behindByADay(db, policy):
    """An hourly scheduler that has not been served for a day: 24 beats missed."""

    app = appWith(ExampleApp, db, overdueSchedulersPolicy=policy)
    scheduler = addScheduler(app, dailyFrequency=24)
    setDeadline(app, scheduler, utc_now() - timedelta(days=1))

    return app, scheduler


def test_execute_reconstructed_replays_the_backlog_beat_by_beat(db, tasks):
    app, scheduler = behindByADay(db, "execute reconstructed")
    before = reload(app, scheduler).deadline

    app.scheduler._work()
    after = reload(app, scheduler)

    assert tasks.count_documents({}) == 1                    # one missed run, replayed
    assert abs((after.deadline - before) - timedelta(hours=1)) < timedelta(seconds=1)
    assert after.deadline < utc_now()                        # still owed 23 more


def test_execute_once_emits_one_run_and_resumes_from_now(db, tasks):
    app, scheduler = behindByADay(db, "execute once")

    app.scheduler._work()
    after = reload(app, scheduler)

    assert tasks.count_documents({}) == 1                    # the backlog collapses to one
    assert timedelta(minutes=59) < after.deadline - utc_now() < timedelta(minutes=61)


def test_skip_emits_nothing_and_resumes_from_now(db, tasks):
    app, scheduler = behindByADay(db, "skip")

    app.scheduler._work()
    after = reload(app, scheduler)

    assert tasks.count_documents({}) == 0                    # nothing owed is run
    assert timedelta(minutes=59) < after.deadline - utc_now() < timedelta(minutes=61)


@pytest.mark.parametrize("policy", ["execute reconstructed", "execute once", "skip"])
def test_a_scheduler_that_is_merely_due_emits_under_every_policy(db, tasks, policy):
    app = appWith(ExampleApp, db, overdueSchedulersPolicy=policy)
    scheduler = addScheduler(app, dailyFrequency=24)
    setDeadline(app, scheduler, utc_now() - timedelta(seconds=1))

    app.scheduler._work()

    assert tasks.count_documents({}) == 1
    assert timedelta(minutes=59) < reload(app, scheduler).deadline - utc_now() < timedelta(minutes=61)


def test_a_scheduler_set_faster_than_it_can_be_served_stops_accumulating(db, tasks):
    """The bug this policy exists for: a 1s scheduler down for an hour used to emit
    an unbounded burst trying to catch up."""

    app = appWith(ExampleApp, db, overdueSchedulersPolicy="execute once")
    scheduler = addScheduler(app, dailyFrequency=86400)      # one second apart
    setDeadline(app, scheduler, utc_now() - timedelta(hours=1))

    for _ in range(20):
        app.scheduler._work()

    assert tasks.count_documents({}) == 1                    # not 20, and not 3600
    assert reload(app, scheduler).deadline > utc_now()       # caught up on the first claim


def test_the_policy_still_applies_after_startup(db, tasks):
    """init() no longer owns the policy, so a scheduler that falls behind while
    the app is up is treated the same as one that fell behind while it was down."""

    app = appWith(ExampleApp, db, overdueSchedulersPolicy="skip")
    app.init()

    scheduler = addScheduler(app, dailyFrequency=24)
    setDeadline(app, scheduler, utc_now() - timedelta(days=1))   # fell behind since

    app.scheduler._work()

    assert tasks.count_documents({}) == 0


def test_a_scheduler_whose_task_vanished_skips_its_beats_but_stays_enabled(db, app, tasks, caplog):
    """Disabling is the operator's word: it would outlast the task coming back."""

    scheduler = addScheduler(app)
    due = utc_now() - timedelta(seconds=1)
    setDeadline(app, scheduler, due)

    class Smaller(BaseApp):
        @task
        @staticmethod
        def other():
            return None

    smaller = Smaller(db)
    smaller.init()

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        smaller.scheduler._work()

    after = reload(app, scheduler)
    assert after.status == "enabled"
    assert after.deadline > due             # still keeps its rhythm
    assert tasks.count_documents({}) == 0
    assert "no task for" in caplog.text


# --- ensure(): declaring a scheduler that should always exist ---

def declare(app, *, name="nightly", to="Ada", dailyFrequency=1, enabled=None):
    return app.scheduler.ensure(
        name,
        ExampleApp.greet(name=to),
        app.distribution("constant", dailyFrequency=dailyFrequency),
        enabled=enabled,
    )


def test_ensure_creates_the_scheduler(app, schedulers):
    scheduler = declare(app)

    assert schedulers.count_documents({}) == 1
    assert scheduler.name == "nightly"
    assert scheduler.status == "enabled"
    assert scheduler.work.kwargs == {"name": "Ada"}


def test_ensure_is_idempotent_across_restarts(db, schedulers):
    for _ in range(5):
        declare(ExampleApp(db))

    assert schedulers.count_documents({}) == 1


def test_the_uid_is_derived_from_the_name(db):
    first = declare(ExampleApp(db))
    second = declare(ExampleApp(db.client["elsewhere"]))

    assert first.uid == second.uid  # same name, same scheduler, any database


def test_different_names_are_different_schedulers(app, schedulers):
    declare(app, name="nightly")
    declare(app, name="hourly")

    assert schedulers.count_documents({}) == 2


def test_ensure_does_not_reset_the_deadline(db):
    first = declare(ExampleApp(db))
    again = declare(ExampleApp(db))

    assert abs(again.deadline - first.deadline) < timedelta(milliseconds=100)


def test_ensure_updates_changed_work(db):
    declare(ExampleApp(db), to="Ada")
    updated = declare(ExampleApp(db), to="Grace")

    assert updated.work.kwargs == {"name": "Grace"}


def test_changing_the_work_keeps_the_rhythm(db):
    first = declare(ExampleApp(db), to="Ada")
    updated = declare(ExampleApp(db), to="Grace")

    assert abs(updated.deadline - first.deadline) < timedelta(milliseconds=100)


def test_changing_the_distribution_restarts_the_rhythm(db):
    declare(ExampleApp(db), dailyFrequency=1)          # daily
    updated = declare(ExampleApp(db), dailyFrequency=24)  # hourly

    assert updated.distribution.kwargs == {"dailyFrequency": 24}
    assert timedelta(minutes=59) < updated.deadline - utc_now() < timedelta(minutes=61)


def test_ensure_leaves_a_disabled_scheduler_disabled(db, app):
    scheduler = declare(app)
    app.scheduler.schedulersCollection.update_one(
        {"uid": scheduler.uid}, {"$set": {"status": "disabled"}}
    )

    assert declare(ExampleApp(db)).status == "disabled"


def test_ensure_can_force_the_state(db, app):
    declare(app)

    assert declare(ExampleApp(db), enabled=False).status == "disabled"
    assert declare(ExampleApp(db), enabled=True).status == "enabled"


def test_ensure_can_create_a_disabled_scheduler(app):
    assert declare(app, enabled=False).status == "disabled"


def test_ensure_validates_before_storing(app, schedulers):
    with pytest.raises(TaskNotFound):
        app.scheduler.ensure("bad", CallSpec.new("does_not_exist"), app.distribution("constant", dailyFrequency=1))

    assert schedulers.count_documents({}) == 0


def test_a_declared_scheduler_fires_like_any_other(app, tasks):
    scheduler = declare(app)
    setDeadline(app, scheduler, utc_now() - timedelta(minutes=1))
    app.scheduler._work()

    assert tasks.count_documents({}) == 1
    assert Task.model_validate(tasks.find_one()).factory.name == "nightly"


def test_add_and_ensure_coexist(app, schedulers):
    addScheduler(app)
    declare(app)

    assert schedulers.count_documents({}) == 2


def test_unnamed_schedulers_do_not_collide(app, schedulers):
    addScheduler(app, name="a")
    addScheduler(app, name="b")

    assert schedulers.count_documents({"name": "Scheduler"}) == 2  # the unique uid index allows this


def test_get_finds_a_declared_scheduler(app):
    declare(app)

    assert app.scheduler.byName("nightly").name == "nightly"
    assert app.scheduler.byName("never-declared") is None


def test_remove_deletes_it(app, schedulers):
    declare(app)

    assert app.scheduler.removeNamed("nightly") is True
    assert app.scheduler.removeNamed("nightly") is False
    assert schedulers.count_documents({}) == 0


def test_a_removed_scheduler_can_be_declared_again(db, schedulers):
    declare(ExampleApp(db))
    ExampleApp(db).scheduler.removeNamed("nightly")
    declare(ExampleApp(db))

    assert schedulers.count_documents({}) == 1


def test_concurrent_declarations_create_one_scheduler(db, schedulers):
    import threading

    apps = [ExampleApp(db) for _ in range(8)]
    barrier = threading.Barrier(len(apps))

    def declareFrom(app):
        barrier.wait()
        declare(app)

    threads = [threading.Thread(target=declareFrom, args=(app,)) for app in apps]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert schedulers.count_documents({}) == 1


def test_a_scheduler_field_can_be_cleared(db):
    from pymonque import Scheduler, schedulers

    class Noted(Scheduler):
        note: str | None = None

    class App(BaseApp):
        scheduler = schedulers(Noted)

        @task
        @staticmethod
        def greet(name: str = "x") -> str: return name

    app = App(db)
    stored = app.scheduler.add(
        App.greet(), app.distribution("constant", dailyFrequency=1), note="hello"
    )

    assert app.scheduler.update(stored.uid, note=None).note is None


# --- emitting a beat exactly once ---

def test_a_beat_reclaimed_after_a_crash_is_not_emitted_twice(app):
    scheduler = addScheduler(app)
    due = utc_now() - timedelta(seconds=1)
    setDeadline(app, scheduler, due)

    app.scheduler._work()
    # the worker died after emitting, before moving the deadline on
    setDeadline(app, scheduler, due)
    app.scheduler._work()

    assert app.task.tasksCollection.count_documents({"work.functionName": "greet"}) == 1


def test_a_new_deadline_moves_the_lease_with_it(app):
    scheduler = addScheduler(app)
    later = (utc_now() + timedelta(days=30)).replace(microsecond=0)

    updated = app.scheduler.update(scheduler.uid, deadline=later)

    assert updated.deadline == later
    assert updated.leaseUntil == later
