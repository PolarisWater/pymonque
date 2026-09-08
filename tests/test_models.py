"""The MongoDB documents: Task, TaskFactory, Scheduler, and the Document base."""

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import Field, ValidationError

from pymonque import CallSpec, Document, Task, TaskFactory, Scheduler, utc_now


def work() -> CallSpec:
    return CallSpec.new("greet", name="Ada")


# --- utc_now ---

def test_utc_now_is_naive_utc():
    now = utc_now()

    assert now.tzinfo is None  # mongo stores naive datetimes
    assert abs(now - datetime.now(timezone.utc).replace(tzinfo=None)) < timedelta(seconds=1)


# --- Document ---

def test_model_dump_keeps_none_fields_so_a_set_can_clear_them():
    task = Task(work=work(), deadline=utc_now(), factory=TaskFactory(name="f"))
    dumped = task.model_dump()

    assert dumped["result"] is None
    assert dumped["error"] is None
    assert dumped["executionTime"] is None


def test_model_dump_keeps_falsy_values():
    class M(Document):
        zero: int = 0
        empty: str = ""

    dumped = M().model_dump()

    assert dumped["zero"] == 0 and dumped["empty"] == ""


def test_model_dump_is_still_pydantics_own():
    class M(Document):
        colorHex: str = Field("#FFF", alias="color_hex")

    m = M(color_hex="#000")

    assert m.model_dump()["color_hex"] == "#000"     # aliased by default, for an existing schema
    assert m.model_dump(by_alias=False)["colorHex"] == "#000"
    assert m.model_dump(mode="json")["color_hex"] == "#000"


# --- Task ---

def test_defaults():
    task = Task(work=work(), deadline=utc_now(), factory=TaskFactory(name="f"))

    assert task.status == "pending"
    assert task.result is None
    assert task.error is None
    assert task.executionTime is None


def test_uids_are_unique():
    factory = TaskFactory(name="f")
    a = Task(work=work(), deadline=utc_now(), factory=factory)
    b = Task(work=work(), deadline=utc_now(), factory=factory)

    assert a.uid != b.uid


def test_rejects_an_unknown_status():
    with pytest.raises(ValidationError):
        Task(work=work(), deadline=utc_now(), factory=TaskFactory(name="f"), status="banana")


@pytest.mark.parametrize("value, expected", [
    (2, timedelta(seconds=2)),
    (2.5, timedelta(seconds=2.5)),
    ("2.5", timedelta(seconds=2.5)),
    (timedelta(seconds=2), timedelta(seconds=2)),
    (None, None),
])
def test_executionTime_accepts(value, expected):
    task = Task(
        work=work(), deadline=utc_now(), factory=TaskFactory(name="f"),
        executionTime=value,
    )

    assert task.executionTime == expected


def test_executionTime_rejects_nonsense():
    # a bare TypeError, so pydantic passes it through rather than wrapping it
    with pytest.raises(TypeError):
        Task(
            work=work(), deadline=utc_now(), factory=TaskFactory(name="f"),
            executionTime=["nope"],
        )


def test_executionTime_serializes_to_seconds():
    task = Task(
        work=work(), deadline=utc_now(), factory=TaskFactory(name="f"),
        executionTime=timedelta(milliseconds=1500),
    )

    assert task.model_dump()["executionTime"] == 1.5


def test_task_roundtrips_through_mongo(db):
    task = Task(
        work=work(), deadline=utc_now(), factory=TaskFactory(name="f"),
        status="success", result={"ok": True}, executionTime=timedelta(seconds=3),
    )
    db["tasks"].insert_one(task.model_dump())
    loaded = Task.model_validate(db["tasks"].find_one())

    assert loaded.uid == task.uid
    assert loaded.status == "success"
    assert loaded.result == {"ok": True}
    assert loaded.executionTime == timedelta(seconds=3)
    assert loaded.work == task.work


# --- TaskFactory ---

def test_emit_stamps_the_factory():
    factory = TaskFactory(name="web-api")
    task = factory._emit(work(), deadline=utc_now())

    assert task.factory is factory
    assert task.status == "pending"
    assert task.model_dump()["factory"]["name"] == "web-api"


# --- Scheduler ---

def test_scheduler_defaults():
    scheduler = Scheduler(
        work=work(),
        distribution=CallSpec.new("constant", dailyFrequency=1),
        deadline=utc_now(),
    )

    assert scheduler.status == "enabled"
    assert scheduler.name == "Scheduler"


def test_an_emitted_task_points_back_at_its_scheduler():
    scheduler = Scheduler(
        work=work(),
        distribution=CallSpec.new("constant", dailyFrequency=1),
        deadline=utc_now(),
    )
    task = scheduler._emit(deadline=utc_now())

    assert task.factory.uid == scheduler.uid
    assert task.work == scheduler.work
    # stored as a TaskFactory, so the scheduler's own fields stay out of the task
    assert set(task.model_dump()["factory"]) == {"uid", "name"}
