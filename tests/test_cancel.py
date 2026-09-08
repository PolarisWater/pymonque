"""Cancelling queued work, the way an operator would from a UI."""

from datetime import timedelta

from pymonque import Task, utc_now

from conftest import ExampleApp


def stored(tasks, name="greet") -> Task:
    return Task.model_validate(tasks.find_one({"work.functionName": name}))


def test_a_waiting_task_can_be_cancelled(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"), deadline=utc_now() + timedelta(hours=1))

    assert app.task.cancel(stored(tasks).uid) is True
    assert stored(tasks).status == "canceled"


def test_a_cancelled_task_is_never_run(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    app.task.cancel(stored(tasks).uid)

    assert app.task._work() is None
    assert stored(tasks).status == "canceled"


def test_a_task_that_is_running_cannot_be_cancelled(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    tasks.update_many({}, {"$set": {
        "status": "processing", "leaseUntil": utc_now() + timedelta(minutes=5)
    }})

    assert app.task.cancel(stored(tasks).uid) is False   # somebody is on it
    assert stored(tasks).status == "processing"


def test_an_abandoned_task_can_be_cancelled(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    tasks.update_many({}, {"$set": {
        "status": "processing", "leaseUntil": utc_now() - timedelta(hours=1)
    }})

    assert app.task.cancel(stored(tasks).uid) is True    # nobody is running it
    assert stored(tasks).status == "canceled"


def test_a_finished_task_cannot_be_cancelled(app, tasks):
    app.task.schedule(ExampleApp.greet(name="Ada"))
    app.task._work()

    assert app.task.cancel(stored(tasks).uid) is False
    assert stored(tasks).status == "success"


def test_cancelling_an_unknown_uid_reports_nothing_happened(app):
    assert app.task.cancel("no-such-uid") is False


def test_cancelMany_takes_a_filter(app, tasks):
    for name in ("a", "a", "b"):
        app.task.schedule(app.task("greet", name=name))

    assert app.task.cancelMany({"work.kwargs.name": "a"}) == 2
    assert tasks.count_documents({"status": "canceled"}) == 2
    assert tasks.find_one({"work.kwargs.name": "b"})["status"] == "pending"


def test_cancelMany_leaves_running_work_alone(app, tasks):
    app.task.schedule(app.task("greet", name="waiting"))
    app.task.schedule(app.task("greet", name="running"))
    tasks.update_one({"work.kwargs.name": "running"}, {"$set": {
        "status": "processing", "leaseUntil": utc_now() + timedelta(minutes=5)
    }})

    assert app.task.cancelMany() == 1
    assert tasks.find_one({"work.kwargs.name": "running"})["status"] == "processing"


def test_a_scheduler_is_disabled_rather_than_cancelled(app):
    scheduler = app.scheduler.add(
        ExampleApp.greet(name="Ada"), app.distribution("constant", dailyFrequency=24)
    )

    assert app.scheduler.update(scheduler.uid, enabled=False).status == "disabled"
