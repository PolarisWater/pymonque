"""Reprs — they show up in log lines and error messages, so they should stay readable."""

from pymonque import CallSpec, Task, TaskFactory, Scheduler, utc_now

from conftest import ExampleApp


def work() -> CallSpec:
    return CallSpec.new("greet", name="Ada")


def test_callspec():
    assert repr(work()) == "greet({'name': 'Ada'})"


def test_funcspec():
    assert "greet" in repr(ExampleApp.greet)


def test_task():
    task = Task(work=work(), deadline=utc_now(), factory=TaskFactory(name="web-api"))

    assert repr(task) == "Task greet({'name': 'Ada'}) from Factory web-api"


def test_factory():
    assert repr(TaskFactory(name="web-api")) == "Factory web-api"


def test_scheduler():
    scheduler = Scheduler(
        name="nightly",
        work=work(),
        distribution=CallSpec.new("constant", dailyFrequency=1),
        deadline=utc_now(),
    )

    assert repr(scheduler) == "Scheduler nightly: greet({'name': 'Ada'})"


def test_item(app):
    item = app.outbox.add(to="a@b.c")

    assert repr(item) == f"Item {item.uid} (pending)"


def test_pile_engine(app):
    assert repr(app.outbox) == "Pile outbox (pymonque_pile_outbox)"


def test_pile_declaration():
    assert repr(ExampleApp.outbox) == "pile outbox"
