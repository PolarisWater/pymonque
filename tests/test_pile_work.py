"""The work() block: one item held for the length of a block, ending in exactly one outcome — done at
its end, failed on an exception, or whatever w.done(), w.fail() or w.release() recorded before ending
the block early — and what happens when something gets in the way of that."""

import logging
import sys
import time

import pytest
from pydantic import BaseModel

from pymonque import CallSpec, utc_now


class Email(BaseModel):
    to: str


@pytest.fixture
def outbox(pileEngine):
    return pileEngine(Email, name="outbox")


# --- the block's own outcomes ---

def test_reaching_the_end_marks_the_item_done(outbox):
    item = outbox.add(to="a@b.c")

    with outbox.work() as w:
        assert w.data.to == "a@b.c"

    assert outbox.get(item.uid).status == "done"


def test_an_exception_marks_the_item_failed_and_is_re_raised(outbox):
    item = outbox.add(to="a@b.c")

    with pytest.raises(ValueError):
        with outbox.work():
            raise ValueError("nope")

    failed = outbox.get(item.uid)

    assert failed.status == "failed"
    assert "ValueError: nope" in failed.error


def test_sys_exit_in_a_block_fails_the_item(outbox):
    item = outbox.add(to="a@b.c")

    with pytest.raises(SystemExit):
        with outbox.work():
            sys.exit(2)

    assert outbox.get(item.uid).status == "failed"
    assert "SystemExit" in outbox.get(item.uid).error


def test_an_empty_pile_yields_none(outbox):
    with outbox.work() as w:
        assert w is None


def test_work_takes_a_filter(outbox):
    outbox.add(to="skip@x.y")
    outbox.add(to="take@x.y")

    with outbox.work({"data.to": "take@x.y"}) as w:
        assert w.data.to == "take@x.y"


def test_the_block_holds_its_item_and_lets_it_go_when_it_ends(outbox):
    outbox.add(to="a@b.c")

    with outbox.work() as w:
        assert len(outbox.leases) == 1
        assert w.attempts == 1

    assert len(outbox.leases) == 0


def test_the_lease_is_renewed_for_as_long_as_the_block_runs(pileEngine):
    jobs = pileEngine(leaseSeconds=0.3)
    item = jobs.add({"n": 1})

    with jobs.work() as w:
        time.sleep(0.7)     # more than two leases, so only renewal keeps it

        assert jobs.claim() is None
        assert jobs.get(w.uid).leaseUntil > utc_now()

    assert jobs.get(item.uid).status == "done"


# --- ending the block early ---

def test_w_done_records_its_result_and_ends_the_block(outbox):
    item = outbox.add(to="a@b.c")
    ran = []

    with outbox.work() as w:
        w.done("duplicate")
        ran.append("after w.done()")

    ran.append("after the block")
    done = outbox.get(item.uid)

    assert ran == ["after the block"]
    assert (done.status, done.result) == ("done", "duplicate")


def test_w_fail_records_its_error_and_ends_the_block_without_raising(outbox):
    item = outbox.add(to="a@b.c")
    ran = []

    with outbox.work() as w:
        w.fail("no recipient")
        ran.append("after w.fail()")

    failed = outbox.get(item.uid)

    assert ran == []
    assert (failed.status, failed.error) == ("failed", "no recipient")


def test_w_release_puts_the_item_back_with_its_try_and_ends_the_block(outbox):
    item = outbox.add(to="a@b.c")
    ran = []

    with outbox.work() as w:
        w.release()
        ran.append("after w.release()")

    released = outbox.get(item.uid)

    assert ran == []
    assert (released.status, released.attempts, released.claimId) == ("pending", 0, None)


def test_an_early_end_gets_past_except_exception(outbox):
    item = outbox.add(to="a@b.c")
    ran = []

    with outbox.work() as w:
        try:
            w.done()
        except Exception:
            ran.append("caught")

        ran.append("after the try")

    assert ran == []
    assert outbox.get(item.uid).status == "done"


def test_an_early_end_unwinds_helpers_and_runs_their_cleanups(outbox):
    outbox.add(to="a@b.c")
    ran = []

    def helper(w):
        try:
            w.release()
        finally:
            ran.append("finally")

    with outbox.work() as w:
        helper(w)
        ran.append("after the helper")

    ran.append("after the block")

    assert ran == ["finally", "after the block"]


def test_an_inner_blocks_early_end_ends_only_the_inner_block(pileEngine):
    first, second = pileEngine(name="first"), pileEngine(name="second")
    a, b = first.add({"n": 1}), second.add({"n": 2})
    ran = []

    with first.work():
        with second.work() as inner:
            inner.done("inner")

        ran.append("the outer block carries on")

    assert ran == ["the outer block carries on"]
    assert first.get(a.uid).status == "done"
    assert (second.get(b.uid).status, second.get(b.uid).result) == ("done", "inner")


def test_a_nested_block_passes_on_an_outer_blocks_early_end(pileEngine):
    first, second = pileEngine(name="first"), pileEngine(name="second")
    a, b = first.add({"n": 1}), second.add({"n": 2})
    ran = []

    with first.work() as outer:
        with second.work():
            outer.release()
            ran.append("inner, after the release")

        ran.append("outer, after the inner block")

    ran.append("after both")

    assert ran == ["after both"]
    assert first.get(a.uid).status == "pending"
    assert (second.get(b.uid).status, second.get(b.uid).attempts) == ("pending", 1)     # back at once, its try spent


def test_an_inner_item_requeued_by_an_outer_early_end_is_given_up_when_out_of_tries(pileEngine):
    first, second = pileEngine(name="first"), pileEngine(name="second")
    first.add({"n": 1})
    b = second.add({"n": 2})

    with first.work() as outer:
        with second.work():
            outer.release()

    assert second.claim() is None       # its one try went with the block that was ended from outside
    assert second.get(b.uid).status == "failed"


# --- when something gets in the way ---

def test_a_swallowed_early_end_keeps_its_outcome_writes_nothing_more_and_says_so(outbox, caplog):
    item = outbox.add(to="a@b.c")
    ran = []

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        with outbox.work() as w:
            try:
                w.release()
            except BaseException:       # as a bare except: would
                pass

            ran.append("kept running")

    released = outbox.get(item.uid)

    assert ran == ["kept running"]
    assert (released.status, released.attempts) == ("pending", 0)
    assert "kept running after w.release()" in caplog.text


def test_a_swallowed_early_end_then_an_exception_writes_nothing_more(outbox, caplog):
    item = outbox.add(to="a@b.c")

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        with pytest.raises(RuntimeError):
            with outbox.work() as w:
                try:
                    w.fail("bounced")
                except BaseException:
                    pass

                raise RuntimeError("later")

    assert outbox.get(item.uid).error == "bounced"
    assert "kept running after w.fail()" in caplog.text


def test_a_block_whose_claim_was_lost_writes_nothing_and_says_so(outbox, caplog):
    item = outbox.add(to="a@b.c")

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        with outbox.work() as w:
            outbox.collection.update_one({"uid": w.uid}, {"$set": {"claimId": "someone-else"}})

    assert "not recorded" in caplog.text
    assert outbox.get(item.uid).status == "running"


def test_an_early_end_whose_claim_was_lost_says_so_and_still_ends_the_block(outbox, caplog):
    item = outbox.add(to="a@b.c")
    ran = []

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        with outbox.work() as w:
            outbox.cancel(w.uid) or outbox.collection.update_one({"uid": w.uid}, {"$set": {"claimId": "someone-else"}})
            w.done()
            ran.append("after w.done()")

    assert ran == []
    assert "not recorded" in caplog.text
    assert outbox.get(item.uid).claimId == "someone-else"


# --- drained by a task ---

def test_a_task_can_drain_the_pile_and_succeeds_on_an_empty_one(outbox, taskEngine):
    def sendOne() -> str:
        with outbox.work() as w:
            if w is None:
                return "empty"

            return f"sent to {w.data.to}"

    tasks = taskEngine({"sendOne": sendOne})
    outbox.add(to="a@b.c")
    tasks.schedule(CallSpec.new("sendOne"))
    tasks.schedule(CallSpec.new("sendOne"))
    results = []

    while (task := tasks.work()) is not None:
        results.append((task.status, task.result))

    assert sorted(results) == [("done", "empty"), ("done", "sent to a@b.c")]
    assert outbox.count(status="done") == 1
