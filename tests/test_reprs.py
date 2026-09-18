"""Reprs show up in log lines and error messages, so they stay readable."""

from pydantic import BaseModel

from pymonque import (
    CallSpec, CollectionEngine, Document, Item, Scheduler, Task, TaskFactory,
    collection, pile, schedulers, task, tasks, utc_now,
)


class Email(BaseModel):
    to: str


class Group(Document):
    name: str = ""


class App:
    groups      = collection(Group)
    outbox      = pile(Email)
    scraps      = pile()
    heavy       = tasks()
    nightly     = schedulers()

    @task
    @staticmethod
    def greet(name: str) -> str:
        return f"hi {name}"


def work() -> CallSpec:
    return CallSpec.new("greet", name="Ada")


def test_callspec():
    assert repr(work()) == "greet({'name': 'Ada'})"


def test_funcspec():
    assert repr(App.greet) == "FuncSpec greet"


def test_task():
    assert repr(Task(work=work(), deadline=utc_now(), factory=TaskFactory(name="web-api"))) == (
        "Task greet({'name': 'Ada'}) from Factory web-api"
    )


def test_factory():
    assert repr(TaskFactory(name="web-api")) == "Factory web-api"


def test_scheduler():
    scheduler = Scheduler(
        name="nightly", work=work(), distribution=CallSpec.new("constant", dailyFrequency=1), deadline=utc_now(),
    )

    assert repr(scheduler) == "Scheduler nightly: greet({'name': 'Ada'})"


def test_item():
    item = Item[Email](data={"to": "a@b.c"})

    assert repr(item) == f"Item {item.uid} (pending)"


def test_declarations_name_their_kind_their_attribute_and_their_model():
    assert [repr(App.groups), repr(App.outbox), repr(App.scraps), repr(App.heavy), repr(App.nightly)] == [
        "collection groups (Group)",
        "pile outbox (Email)",
        "pile scraps (dict)",
        "tasks heavy (Task)",
        "schedulers nightly (Scheduler)",
    ]


def test_an_undeclared_declaration_names_its_kind_and_model():
    assert repr(pile(Email)) == "pile (Email)"


def test_a_task_declaration():
    assert repr(App.__dict__["greet"]) == "task greet"


def test_a_collection_engine_names_its_class_its_name_and_its_collection(db):
    assert repr(CollectionEngine(db["groups"], Group, name="groups")) == "CollectionEngine groups (groups)"
