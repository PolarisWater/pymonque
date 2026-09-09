"""TaskEngine: scheduling, validation, claiming, execution, and startup policies."""

from datetime import timedelta

import pytest

from pymonque import BaseApp, CallSpec, Task, TaskFactory, task, utc_now
from pymonque.exceptions import TaskNotFound, TaskValidationError

from conftest import ExampleApp, appWith


def stored(tasks, name: str) -> Task:
    return Task.model_validate(tasks.find_one({"work.functionName": name}))


# --- building work ---

def test_the_string_form_builds_a_callspec(app):
    spec = app.task("greet", name="Ada")

    assert isinstance(spec, CallSpec)
    assert spec.kwargs == {"name": "Ada"}


def test_the_string_form_and_the_class_form_agree(app):
    assert app.task("greet", name="Ada") == ExampleApp.greet(name="Ada")


def test_an_unknown_task_is_rejected(app):
    with pytest.raises(TaskNotFound):
        app.task("does_not_exist")


def test_a_missing_argument_is_rejected(app):
    with pytest.raises(TaskValidationError):
        app.task("greet")


def test_an_unknown_argument_is_rejected(app):
    with pytest.raises(TaskValidationError):
        app.task("greet", name="Ada", nope=1)


def test_a_wrongly_typed_argument_is_rejected(app):
    with pytest.raises(TaskValidationError):
        app.task("greet", name="Ada", greeting=object())


def test_an_optional_argument_may_be_omitted(app):
    assert app.task("greet", name="Ada").kwargs == {"name": "Ada"}


def test_an_instance_task_does_not_expect_self(app):
    assert app.task("whoami").kwargs == {}


# --- scheduling ---

def test_schedule_stores_a_pending_task(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"), deadline=utc_now())
    entry = stored(tasks, "greet")

    assert entry.status == "pending"
    assert entry.work.kwargs == {"name": "Ada"}


def test_schedule_validates_before_storing(app, tasks):
    with pytest.raises(TaskNotFound):
        app.task.schedule(CallSpec.new("does_not_exist"))

    assert tasks.count_documents({}) == 0


def test_schedule_defaults_to_now(app, tasks):
    before = utc_now()
    app.task.schedule(ExampleApp.greet(name="Ada"))

    # mongo keeps millisecond precision, so allow for the truncation
    assert abs(stored(tasks, "greet").deadline - before) < timedelta(milliseconds=100)


def test_schedule_stamps_the_default_factory(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))

    assert stored(tasks, "greet").factory.name == "default"


def test_schedule_stamps_a_named_factory(app, tasks):
    web = TaskFactory(name="web-api")
    app.task.schedule(ExampleApp.greet(name="Ada"), factory=web)
    entry = stored(tasks, "greet")

    assert entry.factory.name == "web-api"
    assert entry.factory.uid == web.uid


def test_scheduleFromDistribution_pushes_the_deadline_out(app, tasks):
    before = utc_now()
    app.task.scheduleFromDistribution(
        ExampleApp.greet(name="Ada"),
        distribution=app.distribution("constant", dailyFrequency=24),  # one hour
    )
    deadline = stored(tasks, "greet").deadline

    assert timedelta(minutes=59) < deadline - before < timedelta(minutes=61)


def test_scheduleFromDistribution_validates_the_distribution(app, tasks):
    with pytest.raises(Exception):
        app.task.scheduleFromDistribution(
            ExampleApp.greet(name="Ada"),
            distribution=CallSpec.new("does_not_exist"),
        )

    assert tasks.count_documents({}) == 0


# --- claiming ---

def test_a_due_task_is_claimed_and_run(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"), deadline=utc_now())
    app.task._work()
    entry = stored(tasks, "greet")

    assert entry.status == "success"
    assert entry.result == "Hello, Ada!"
    assert entry.error is None
    assert entry.executionTime is not None


def test_a_future_task_is_left_alone(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"), deadline=utc_now() + timedelta(hours=1))
    app.task._work()

    assert stored(tasks, "greet").status == "pending"


def test_working_an_empty_app_is_a_no_op(app):
    assert app.task._work() is None


def test_the_oldest_deadline_goes_first(app, tasks):
    now = utc_now()
    app.task.schedule(app.task("greet", name="second"), deadline=now - timedelta(minutes=1))
    app.task.schedule(app.task("greet", name="first"), deadline=now - timedelta(hours=1))
    app.task._work()

    assert tasks.find_one({"status": "success"})["work"]["kwargs"]["name"] == "first"


def test_a_task_is_never_claimed_twice(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))

    assert app.task._work() is not None
    assert app.task._work() is None  # already consumed
    assert tasks.count_documents({"status": "success"}) == 1


def test_an_instance_task_runs_with_its_app(app, tasks):
    app.task.schedule(ExampleApp.whoami())
    app.task._work()

    assert stored(tasks, "whoami").result == "ExampleApp"


# --- failure handling ---

def test_a_raising_task_is_marked_failed(app, tasks):
    app.task.schedule(ExampleApp.boom())
    app.task._work()
    entry = stored(tasks, "boom")

    assert entry.status == "failed"
    assert "ValueError: nope" in entry.error
    assert entry.executionTime is not None


def test_an_unstorable_result_does_not_leave_the_task_claimed(app, tasks):
    app.task.schedule(ExampleApp.unserializable())
    app.task._work()
    entry = stored(tasks, "unserializable")

    assert entry.status == "failed"
    assert entry.error
    assert tasks.count_documents({"status": "processing"}) == 0


# --- indexes ---

def test_indexes_back_the_claim_query(app, tasks):
    keys = [tuple(index["key"]) for index in tasks.index_information().values()]

    assert (("status", 1), ("leaseUntil", 1)) in keys
    assert (("uid", 1),) in keys


# --- startup policies ---

def test_an_abandoned_task_is_reclaimed_when_its_lease_lapses(db, app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    # a worker claimed it and died: still "processing", lease long expired
    tasks.update_many({}, {"$set": {
        "status": "processing", "leaseUntil": utc_now() - timedelta(hours=1)
    }})

    assert app.task._work() is not None          # picked up without any restart
    assert stored(tasks, "greet").status == "success"


def test_a_task_with_a_live_lease_is_left_alone(db, app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    tasks.update_many({}, {"$set": {
        "status": "processing", "leaseUntil": utc_now() + timedelta(minutes=5)
    }})

    assert app.task._work() is None              # somebody else is working it

    ExampleApp(db)                           # constructing a app does not touch it
    assert stored(tasks, "greet").status == "processing"


def test_a_held_lease_is_renewed_while_the_task_runs(db, app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    app.task._hold(stored(tasks, "greet").uid)
    before = stored(tasks, "greet").leaseUntil

    assert app.task.renewLeases() == 1
    assert stored(tasks, "greet").leaseUntil > before


def test_execute_now_keeps_overdue_tasks_pending(db, app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"), deadline=utc_now() - timedelta(days=1))

    appWith(ExampleApp, db, overdueTaskPolicy="execute now").init()

    assert stored(tasks, "greet").status == "pending"


def test_skip_outdates_overdue_tasks(db, app, tasks):
    app.task.schedule(app.task("greet", name="old"), deadline=utc_now() - timedelta(days=1))
    app.task.schedule(app.task("greet", name="future"), deadline=utc_now() + timedelta(days=1))

    appWith(ExampleApp, db, overdueTaskPolicy="skip").init()

    assert tasks.find_one({"work.kwargs.name": "old"})["status"] == "outdated"
    assert tasks.find_one({"work.kwargs.name": "future"})["status"] == "pending"


def test_a_task_whose_function_vanished_is_flagged_incompatible(db, app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))

    class Smaller(BaseApp):
        @task
        @staticmethod
        def other():
            return None

    Smaller(db).init()
    entry = stored(tasks, "greet")

    assert entry.status == "incompatible"
    assert "does not exist" in entry.error


def test_a_finished_task_is_not_flagged_incompatible(db, app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    app.task._work()

    class Smaller(BaseApp):
        @task
        @staticmethod
        def other():
            return None

    Smaller(db).init()

    assert stored(tasks, "greet").status == "success"
