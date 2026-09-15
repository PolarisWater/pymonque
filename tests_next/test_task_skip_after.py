"""skipAfter: a task claimed too long after its deadline is outdated instead of run — the one rule for
work going stale, whether the app was down or nobody kept up."""

import logging
from datetime import timedelta

import pytest

from pymonque_next import AppDefaults, CallSpec, task, utc_now


ran: list[str] = []


def job() -> str:
    ran.append("job")

    return "ran"


DEFAULTS = AppDefaults(taskSkipAfter=60)

DECLARED = {
    "bare":         task(staticmethod(job)),
    "patient":      task(skipAfter=3600)(staticmethod(job)),
    "eager":        task(skipAfter=1)(staticmethod(job)),
    "unskippable":  task(skipAfter=None)(staticmethod(job)),
}


@pytest.fixture
def engine(taskEngine):
    ran.clear()

    return taskEngine(
        {name: job for name in DECLARED},
        limits={name: declared.limitsWith(DEFAULTS) for name, declared in DECLARED.items()},
    )


def late(engine, name, seconds):
    queued = engine.schedule(CallSpec.new(name), deadline=utc_now() - timedelta(seconds=seconds))
    engine.work()

    return engine.get(queued.uid)


def test_without_a_limit_a_task_runs_however_late(taskEngine):
    assert late(taskEngine({"job": job}), "job", 86400).status == "done"


def test_the_app_default_applies_to_a_task_that_declares_none(engine):
    assert late(engine, "bare", 300).status == "outdated"
    assert late(engine, "bare", 10).status == "done"


def test_a_task_declares_its_own_in_either_direction(engine):
    assert late(engine, "patient", 300).status == "done"       # looser than the default
    assert late(engine, "eager", 10).status == "outdated"      # tighter


def test_none_makes_a_task_unskippable(engine):
    assert late(engine, "unskippable", 86400).status == "done"


def test_an_outdated_task_is_not_run_and_says_how_late(engine):
    ended = late(engine, "bare", 300)

    assert ran == []
    assert ended.result is None
    assert "300s after its deadline" in ended.error
    assert "the 60s it was worth running for" in ended.error


def test_an_outdated_task_was_claimed_and_finished(engine):
    ended = late(engine, "bare", 300)

    assert ended.claimedAt is not None and ended.finishedAt is not None
    assert ended.claimId is None


def test_outdating_a_task_is_logged(engine, caplog):
    with caplog.at_level(logging.WARNING, logger="pymonque"):
        late(engine, "bare", 300)

    assert "was outdated" in caplog.text
