"""The stored documents: the Document base, Task, TaskFactory, Scheduler and Item, their fields,
statuses and defaults, and how they are written to MongoDB."""

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from pydantic import BaseModel, Field, ValidationError

from pymonque_next import CallSpec, Document, Item, Scheduler, Task, TaskFactory, utc_now


def work() -> CallSpec:
    return CallSpec.new("greet", name="Ada")


def aTask(**fields) -> Task:
    return Task(**{"work": work(), "deadline": utc_now(), "factory": TaskFactory(name="f"), **fields})


def aScheduler(**fields) -> Scheduler:
    return Scheduler(**{
        "work": work(), "distribution": CallSpec.new("constant", dailyFrequency=1), "deadline": utc_now(), **fields,
    })


class Email(BaseModel):
    to:         str
    subject:    str = "(no subject)"


# --- times ---

def test_utc_now_is_naive_utc():
    now = utc_now()

    assert now.tzinfo is None
    assert abs(now - datetime.now(timezone.utc).replace(tzinfo=None)) < timedelta(seconds=1)


def test_an_aware_time_is_kept_as_naive_utc():
    task = aTask(deadline=datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=2))))

    assert task.deadline == datetime(2026, 1, 1, 10, 0)
    assert task.leaseUntil == datetime(2026, 1, 1, 10, 0)


# --- the Document base ---

def test_model_dump_keeps_none_and_falsy_values_so_a_write_can_clear_them():
    class M(Document):
        zero:   int = 0
        empty:  str = ""
        gone:   str | None = None

    dumped = M().model_dump()

    assert (dumped["zero"], dumped["empty"], dumped["gone"]) == (0, "", None)


def test_model_dump_is_still_pydantics_own():
    class M(Document):
        colorHex: str = Field("#FFF", alias="color_hex")

    assert M(color_hex="#000").model_dump()["color_hex"] == "#000"     # aliased by default, for an existing schema
    assert M(colorHex="#000").model_dump(by_alias=False)["colorHex"] == "#000"
    assert M(colorHex="#000").model_dump(mode="json")["color_hex"] == "#000"


# --- Task ---

def test_task_defaults():
    task = aTask()

    assert task.status == "pending"
    assert task.leaseUntil == task.deadline
    assert task.createdAt is not None
    assert (task.claimedAt, task.finishedAt, task.claimId) == (None, None, None)
    assert (task.result, task.error, task.executionTime) == (None, None, None)


def test_task_uids_are_unique():
    assert aTask().uid != aTask().uid


@pytest.mark.parametrize("status", ["pending", "running", "done", "failed", "canceled", "timeout", "outdated", "incompatible"])
def test_a_task_takes_the_status_vocabulary(status):
    assert aTask(status=status).status == status


@pytest.mark.parametrize("status", ["success", "processing", "claimed", "banana"])
def test_a_task_refuses_any_other_status(status):
    with pytest.raises(ValidationError):
        aTask(status=status)


def test_a_task_is_matched_by_its_claim_not_by_attempts():
    assert "claimId" in Task.model_fields
    assert "attempts" not in Task.model_fields


def test_a_task_stores_no_limits():
    assert not {"timeout", "skipAfter", "maxAttempts", "retryDelay"} & set(aTask().model_dump())


@pytest.mark.parametrize("value, expected", [
    (2, timedelta(seconds=2)),
    (2.5, timedelta(seconds=2.5)),
    ("2.5", timedelta(seconds=2.5)),
    (timedelta(seconds=2), timedelta(seconds=2)),
    (None, None),
])
def test_executionTime_accepts_seconds_or_a_timedelta(value, expected):
    assert aTask(executionTime=value).executionTime == expected


@pytest.mark.parametrize("value", [["nope"], "soon"])
def test_executionTime_refuses_nonsense(value):
    with pytest.raises(ValidationError):
        aTask(executionTime=value)


def test_executionTime_is_stored_as_seconds():
    assert aTask(executionTime=timedelta(milliseconds=1500)).model_dump()["executionTime"] == 1.5


def test_a_task_roundtrips_through_mongo(db):
    task = aTask(status="done", result={"ok": True}, executionTime=timedelta(seconds=3), claimId="c1")
    db["tasks"].insert_one(task.model_dump())

    loaded = Task.model_validate(db["tasks"].find_one())

    assert (loaded.uid, loaded.status, loaded.result, loaded.claimId) == (task.uid, "done", {"ok": True}, "c1")
    assert loaded.executionTime == timedelta(seconds=3)
    assert loaded.work == task.work


def test_a_task_subclass_stores_its_own_fields(db):
    class AccountTask(Task):
        accountId: int

    task = AccountTask(work=work(), deadline=utc_now(), factory=TaskFactory(name="f"), accountId=42)
    db["tasks"].insert_one(task.model_dump())

    assert AccountTask.model_validate(db["tasks"].find_one()).accountId == 42

    with pytest.raises(ValidationError):
        AccountTask(work=work(), deadline=utc_now(), factory=TaskFactory(name="f"))


# --- TaskFactory ---

def test_a_scheduler_as_a_tasks_factory_is_stored_as_a_factory():
    scheduler = aScheduler(name="nightly")
    task = aTask(factory=scheduler)

    assert task.factory is scheduler
    assert task.model_dump()["factory"] == {"uid": scheduler.uid, "name": "nightly"}


# --- Scheduler ---

def test_scheduler_defaults():
    scheduler = aScheduler()

    assert (scheduler.status, scheduler.name) == ("enabled", "Scheduler")
    assert scheduler.leaseUntil == scheduler.deadline
    assert scheduler.claimId is None


@pytest.mark.parametrize("status, valid", [("enabled", True), ("disabled", True), ("processing", False), ("pending", False)])
def test_a_scheduler_is_enabled_or_disabled_and_nothing_else(status, valid):
    if valid:
        assert aScheduler(status=status).status == status
    else:
        with pytest.raises(ValidationError):
            aScheduler(status=status)


def test_a_scheduler_stores_no_limits():
    assert not {"timeout", "skipAfter", "maxAttempts", "retryDelay"} & set(aScheduler().model_dump())


# --- Item ---

def test_item_defaults():
    item = Item[Email](data={"to": "a@b.c"})

    assert item.status == "pending"
    assert item.leaseUntil == item.createdAt       # claimable as soon as it exists
    assert (item.attempts, item.claimId, item.claimedAt, item.finishedAt) == (0, None, None, None)


def test_an_items_data_is_validated_against_its_payload():
    assert Item[Email](data={"to": "a@b.c"}).data == Email(to="a@b.c")

    with pytest.raises(ValidationError):
        Item[Email](data={"subject": "no recipient"})


def test_an_untyped_item_takes_any_dict():
    assert Item[dict[str, Any]](data={"anything": [1, 2]}).data == {"anything": [1, 2]}


@pytest.mark.parametrize("status", ["pending", "running", "done", "failed", "canceled"])
def test_an_item_takes_the_shared_status_vocabulary(status):
    assert Item[Email](data={"to": "a@b.c"}, status=status).status == status


@pytest.mark.parametrize("status", ["claimed", "timeout", "outdated", "incompatible"])
def test_an_item_refuses_task_only_and_old_statuses(status):
    with pytest.raises(ValidationError):
        Item[Email](data={"to": "a@b.c"}, status=status)


def test_an_item_roundtrips_through_mongo(db):
    item = Item[Email](data={"to": "a@b.c"}, attempts=2, claimId="c1")
    db["items"].insert_one(item.model_dump())

    loaded = Item[Email].model_validate(db["items"].find_one())

    assert (loaded.data, loaded.attempts, loaded.claimId) == (Email(to="a@b.c"), 2, "c1")
