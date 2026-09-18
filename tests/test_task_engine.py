"""The task engine: building and scheduling calls, running a due task once, what a failure records,
several engines sharing one set of tasks, and tasks of a Task subclass, whose fields reach the call
through runWork()."""

import sys
from datetime import timedelta

import pytest
from pydantic import BaseModel, ValidationError
from pymongo import IndexModel

from pymonque import CallSpec, CollectionEngine, Task, TaskFactory, TaskLimits, utc_now
from pymonque.exceptions import DistributionNotFound, TaskNotFound, TaskValidationError


class Email(BaseModel):
    to: str


received: dict = {}


def greet(name: str, greeting: str = "Hello") -> str:
    return f"{greeting}, {name}!"


def boom():
    raise ValueError("nope")


def unstorable():
    return object()     # the driver cannot encode it


def quits():
    sys.exit(3)


def send(email: Email, count: int):
    received.update(email=email, count=count)


def sync(accountId: int, full: bool = False) -> str:
    return f"synced {accountId} full={full}"


FUNCTIONS = {"greet": greet, "boom": boom, "unstorable": unstorable, "quits": quits, "send": send, "sync": sync}


@pytest.fixture
def engine(taskEngine):
    return taskEngine(FUNCTIONS)


def stored(engine, task) -> Task:
    return engine.get(task.uid)


# --- building a call ---

def test_calling_the_engine_builds_a_checked_call(engine):
    assert engine("greet", name="Ada") == CallSpec.new("greet", name="Ada")


@pytest.mark.parametrize("name, kwargs, error", [
    ("nope", {}, TaskNotFound),
    ("greet", {"name": 42}, TaskValidationError),
    ("greet", {"name": "Ada", "nope": 1}, TaskValidationError),
])
def test_calling_the_engine_refuses_arguments_that_do_not_fit(engine, name, kwargs, error):
    with pytest.raises(error):
        engine(name, **kwargs)


def test_calling_the_engine_leaves_a_missing_argument_to_schedule(engine):
    call = engine("greet")

    assert call == CallSpec.new("greet")

    with pytest.raises(TaskValidationError):
        engine.schedule(call)

    assert engine.count() == 0


# --- scheduling ---

def test_schedule_stores_a_pending_task(engine):
    task = stored(engine, engine.schedule(engine("greet", name="Ada")))

    assert (task.status, task.work.kwargs, task.claimId) == ("pending", {"name": "Ada"}, None)


@pytest.mark.parametrize("work, error", [
    (CallSpec.new("nope"), TaskNotFound),
    (CallSpec.new("greet"), TaskValidationError),
    (CallSpec.new("greet", name=object()), TaskValidationError),
])
def test_nothing_invalid_is_scheduled(engine, work, error):
    with pytest.raises(error):
        engine.schedule(work)

    assert engine.count() == 0


def test_a_task_is_due_now_unless_given_a_deadline(engine):
    before = utc_now()
    now = engine.schedule(engine("greet", name="Ada"))
    later = engine.schedule(engine("greet", name="Ada"), deadline=before + timedelta(hours=1))

    assert abs(stored(engine, now).deadline - before) < timedelta(seconds=1)
    assert abs(stored(engine, later).deadline - (before + timedelta(hours=1))) < timedelta(milliseconds=1)


def test_a_task_is_stamped_with_the_engines_factory_or_the_one_given(engine):
    web = TaskFactory(name="web-api")
    plain = stored(engine, engine.schedule(engine("greet", name="Ada")))
    named = stored(engine, engine.schedule(engine("greet", name="Ada"), factory=web))

    assert plain.factory.name == "default"
    assert (named.factory.name, named.factory.uid) == ("web-api", web.uid)


@pytest.mark.parametrize("limit", ["timeout", "skipAfter"])
def test_a_call_carries_no_limits(engine, limit):
    with pytest.raises(TypeError, match="declared on @task"):
        engine.schedule(engine("greet", name="Ada"), **{limit: 1})

    assert engine.count() == 0


@pytest.mark.parametrize("field", ["uid", "status", "claimId", "result"])
def test_schedule_refuses_a_field_the_engine_keeps(engine, field):
    with pytest.raises(TypeError, match="the engine keeps it"):
        engine.schedule(engine("greet", name="Ada"), **{field: "x"})


def test_schedule_refuses_a_field_a_plain_task_does_not_have(engine):
    with pytest.raises(TypeError, match="Task has no field accountId; Task adds no fields"):
        engine.schedule(engine("greet", name="Ada"), accountId=7)


def test_scheduleFromDistribution_is_due_an_interval_from_now(engine):
    before = utc_now()
    task = engine.scheduleFromDistribution(engine("greet", name="Ada"), engine.distributions("constant", dailyFrequency=24))

    assert timedelta(minutes=59) < stored(engine, task).deadline - before < timedelta(minutes=61)


def test_scheduleFromDistribution_refuses_a_bad_distribution(engine):
    with pytest.raises(DistributionNotFound):
        engine.scheduleFromDistribution(engine("greet", name="Ada"), CallSpec.new("nope"))

    assert engine.count() == 0


# --- running ---

def test_a_due_task_is_claimed_and_run(engine):
    task = engine.schedule(engine("greet", name="Ada"))
    ran = engine.work()
    ended = stored(engine, task)

    assert ran.uid == task.uid
    assert (ended.status, ended.result, ended.error, ended.claimId) == ("done", "Hello, Ada!", None, None)
    assert ended.executionTime is not None


def test_a_future_task_is_left_alone(engine):
    task = engine.schedule(engine("greet", name="Ada"), deadline=utc_now() + timedelta(hours=1))

    assert engine.work() is None
    assert stored(engine, task).status == "pending"


def test_working_an_empty_engine_does_nothing(engine):
    assert engine.work() is None


def test_the_most_overdue_task_goes_first(engine):
    now = utc_now()
    engine.schedule(engine("greet", name="second"), deadline=now - timedelta(minutes=1))
    engine.schedule(engine("greet", name="first"), deadline=now - timedelta(hours=1))

    assert engine.work().result == "Hello, first!"


def test_a_task_runs_once(engine):
    engine.schedule(engine("greet", name="Ada"))

    assert engine.work() is not None
    assert engine.work() is None
    assert engine.count({"status": "done"}) == 1


def test_a_queue_drains_by_working_until_nothing_is_left(engine):
    for name in ("Ada", "Grace", "Alan"):
        engine.schedule(engine("greet", name=name))

    ran = []

    while (task := engine.work()) is not None:
        ran.append(task.result)

    assert sorted(ran) == ["Hello, Ada!", "Hello, Alan!", "Hello, Grace!"]


def test_a_task_the_engine_has_no_function_for_is_left_alone(engine, taskEngine):
    engine.schedule(engine("greet", name="Ada"))
    other = taskEngine({"boom": boom})      # the same collection, other tasks

    assert other.work() is None
    assert engine.count({"status": "pending"}) == 1


def test_a_task_gets_models_and_coerced_values_not_stored_data(engine):
    received.clear()
    engine.schedule(CallSpec.new("send", email=Email(to="a@b.c"), count="3"))
    engine.work()

    assert received == {"email": Email(to="a@b.c"), "count": 3}


# --- failing ---

def test_a_raising_task_fails_with_its_traceback(engine):
    task = engine.schedule(engine("boom"))
    engine.work()
    ended = stored(engine, task)

    assert ended.status == "failed"
    assert "ValueError: nope" in ended.error
    assert ended.result is None and ended.executionTime is not None


def test_sys_exit_in_a_task_is_a_failure_not_a_dead_worker(engine):
    task = engine.schedule(engine("quits"))
    engine.work()       # SystemExit would otherwise end the thread working the queue

    assert stored(engine, task).status == "failed"
    assert "SystemExit" in stored(engine, task).error


def test_a_result_the_driver_cannot_store_is_a_failure(engine):
    task = engine.schedule(engine("unstorable"))
    engine.work()
    ended = stored(engine, task)

    assert ended.status == "failed" and ended.error and ended.result is None
    assert engine.count({"status": "running"}) == 0


def test_a_call_is_checked_again_when_it_runs(engine):
    task = engine.schedule(engine("greet", name="Ada"))
    engine.collection.update_one({"uid": task.uid}, {"$set": {"work.kwargs": {"name": 42}}})     # changed since
    engine.work()
    ended = stored(engine, task)

    assert ended.status == "failed"
    assert "TaskValidationError" in ended.error


# --- limits ---

def test_a_task_runs_under_its_functions_limits(taskEngine):
    engine = taskEngine({"greet": greet, "boom": boom}, limits={"greet": TaskLimits(skipAfter=5), "boom": TaskLimits(timeout=9)})

    assert engine.limitsFor(engine.schedule(engine("greet", name="Ada"))) == TaskLimits(skipAfter=5)
    assert engine.limitsFor(engine.schedule(engine("boom"))) == TaskLimits(timeout=9)


def test_limits_for_a_task_the_engine_does_not_have_are_refused(taskEngine):
    with pytest.raises(TypeError, match="limits given for nope"):
        taskEngine({"greet": greet}, limits={"greet": TaskLimits(), "nope": TaskLimits()})


def test_a_task_without_limits_is_refused_rather_than_run_without_the_defaults(taskEngine):
    with pytest.raises(TypeError, match="no limits given for boom"):
        taskEngine({"greet": greet, "boom": boom}, limits={"greet": TaskLimits()})


# --- the collection ---

def test_a_task_engine_is_a_collection_engine(engine):
    task = engine.schedule(engine("greet", name="Ada"))

    assert isinstance(engine, CollectionEngine)
    assert engine.count() == 1
    assert engine.find()[0].work.functionName == "greet"
    assert engine.get(task.uid).bound


def test_indexes_back_the_claim_query(engine):
    keys = [tuple(index["key"]) for index in engine.collection.index_information().values()]

    assert (("status", 1), ("leaseUntil", 1)) in keys
    assert (("uid", 1),) in keys


def test_extra_indexes_are_created(taskEngine):
    engine = taskEngine(FUNCTIONS, extraIndexes=[IndexModel([("factory.name", 1)], name="byFactory")])

    assert "byFactory" in engine.collection.index_information()


def test_the_engine_refuses_a_model_that_is_not_a_task(taskEngine):
    with pytest.raises(TypeError, match="Task"):
        taskEngine(FUNCTIONS, Email)


def test_update_validates_before_writing(engine):
    task = engine.schedule(engine("greet", name="Ada"))

    with pytest.raises(ValidationError):
        engine.update(task.uid, status="banana")

    assert engine.collection.find_one({"uid": task.uid})["status"] == "pending"


def test_moving_a_waiting_tasks_deadline_moves_its_lease(engine):
    later = (utc_now() + timedelta(days=1)).replace(microsecond=0)
    saved = engine.schedule(engine("greet", name="Ada"))
    saved.deadline = later
    saved.save()
    updated = engine.update(engine.schedule(engine("greet", name="Grace")).uid, deadline=later)

    assert stored(engine, saved).leaseUntil == later
    assert updated.leaseUntil == later
    assert engine.work() is None


def test_moving_a_running_tasks_deadline_keeps_its_lease(engine):
    task = engine.schedule(engine("greet", name="Ada"))
    held = (utc_now() + timedelta(minutes=5)).replace(microsecond=0)
    engine.collection.update_one({"uid": task.uid}, {"$set": {"status": "running", "leaseUntil": held}})

    assert engine.update(task.uid, deadline=utc_now() - timedelta(days=1)).leaseUntil == held


# --- housekeeping ---

def test_a_waiting_task_whose_function_is_gone_is_flagged_incompatible(engine, taskEngine):
    finished = engine.schedule(engine("greet", name="Grace"))
    engine.work()
    waiting = engine.schedule(engine("greet", name="Ada"))

    assert taskEngine({"boom": boom}).flagIncompatible() == 1

    flagged = stored(engine, waiting)

    assert flagged.status == "incompatible" and "no task" in flagged.error
    assert flagged.finishedAt is not None
    assert stored(engine, finished).status == "done"


def test_a_task_a_dead_worker_left_running_whose_function_is_gone_is_written_off(engine, taskEngine):
    def running(name, leaseUntil):
        task = engine.schedule(engine("greet", name=name))
        engine.collection.update_one({"uid": task.uid}, {"$set": {"status": "running", "claimId": "a-worker", "leaseUntil": leaseUntil}})

        return task

    stuck = running("stuck", utc_now() - timedelta(seconds=1))
    live = running("live", utc_now() + timedelta(minutes=5))
    waiting = engine.schedule(engine("greet", name="waiting"))

    assert engine.writeOffStuck() == 0      # an engine that can run it leaves it to a claim
    assert taskEngine({"boom": boom}).writeOffStuck() == 1

    ended = stored(engine, stuck)

    assert (ended.status, ended.claimId) == ("failed", None)
    assert "stopped renewing the lease" in ended.error and ended.finishedAt is not None
    assert stored(engine, live).status == "running"
    assert stored(engine, waiting).status == "pending"


# --- several engines ---

def test_any_task_engine_runs_any_task_from_its_own_collection(taskEngine):
    light = taskEngine(FUNCTIONS, name="light")
    heavy = taskEngine(FUNCTIONS, name="heavy")
    greeting = light.schedule(light("greet", name="Ada"))
    syncing = heavy.schedule(light("sync", accountId=7))

    assert light.collection.name != heavy.collection.name
    assert light.work().uid == greeting.uid and light.work() is None
    assert heavy.work().uid == syncing.uid and heavy.work() is None


# --- tasks of a Task subclass ---

class AccountTask(Task):
    accountId: int

    def runWork(self) -> CallSpec:
        return self.work.bind(accountId=self.accountId)


@pytest.fixture
def accounts(taskEngine):
    return taskEngine(FUNCTIONS, AccountTask, name="accounts")


def test_a_task_subclass_takes_its_fields_where_a_task_is_scheduled(accounts):
    task = accounts.get(accounts.schedule(CallSpec.new("sync"), accountId=42).uid)

    assert isinstance(task, AccountTask)
    assert task.accountId == 42


def test_runWork_is_where_the_fields_reach_the_call(accounts):
    task = accounts.schedule(CallSpec.new("sync", full=True), accountId=42)

    assert accounts.work().result == "synced 42 full=True"
    assert accounts.get(task.uid).work.kwargs == {"full": True}     # stored as given


def test_a_call_is_checked_as_runWork_gives_it(accounts, engine):
    accounts.schedule(CallSpec.new("sync"), accountId=42)           # the task supplies accountId

    with pytest.raises(TaskValidationError):
        engine.schedule(CallSpec.new("sync"))                       # a plain task has none to give


def test_calling_an_engine_leaves_out_what_runWork_supplies(accounts):
    accounts.schedule(accounts("sync", full=True), accountId=42)

    assert accounts.work().result == "synced 42 full=True"


def test_a_plain_task_runs_its_work_unchanged(engine):
    task = stored(engine, engine.schedule(engine("sync", accountId=7)))

    assert task.runWork() == task.work
    assert engine.work().result == "synced 7 full=False"


@pytest.mark.parametrize("fields", [{}, {"accountId": "not a number"}])
def test_a_missing_or_invalid_field_is_refused_before_anything_is_stored(accounts, fields):
    with pytest.raises(ValidationError):
        accounts.schedule(CallSpec.new("sync"), **fields)

    assert accounts.count() == 0


def test_a_field_the_subclass_does_not_add_is_refused_naming_those_it_does(accounts):
    with pytest.raises(TypeError, match="AccountTask has no field tenant; AccountTask adds accountId"):
        accounts.schedule(CallSpec.new("sync"), tenant="acme", accountId=1)
