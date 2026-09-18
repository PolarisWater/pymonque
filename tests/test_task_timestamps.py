"""The three moments a task records: when it was created, claimed and finished."""

from datetime import timedelta

import pytest

from pymonque import CallSpec, utc_now


def ok() -> str:
    return "ok"


def broken():
    raise RuntimeError("nope")


@pytest.fixture
def engine(taskEngine):
    return taskEngine({"ok": ok, "broken": broken})


def test_a_new_task_has_only_been_created(engine):
    before = utc_now()
    task = engine.get(engine.schedule(CallSpec.new("ok")).uid)

    assert abs(task.createdAt - before) < timedelta(seconds=1)
    assert (task.claimedAt, task.finishedAt) == (None, None)


@pytest.mark.parametrize("name, status", [("ok", "done"), ("broken", "failed")])
def test_a_run_task_records_its_claim_and_its_finish(engine, name, status):
    task = engine.schedule(CallSpec.new(name))
    engine.work()
    ended = engine.get(task.uid)

    assert ended.status == status
    assert ended.createdAt <= ended.claimedAt <= ended.finishedAt


def test_a_cancelled_task_finished_without_a_claim(engine):
    task = engine.schedule(CallSpec.new("ok"), deadline=utc_now() + timedelta(hours=1))
    engine.cancel(task.uid)
    cancelled = engine.get(task.uid)

    assert cancelled.claimedAt is None
    assert cancelled.finishedAt is not None
