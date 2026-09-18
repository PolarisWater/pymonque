"""The pile engine: filling a pile, claiming from it atomically, tries and giving up, finishing items by
claim or by uid, releasing, renewing, cancelling, and looking at what is on it."""

import threading
from datetime import timedelta

import pytest
from pydantic import BaseModel, ValidationError
from pymongo import IndexModel

from pymonque import CollectionEngine, Document, Item, utc_now


class Email(BaseModel):
    to:         str
    subject:    str = "(no subject)"


@pytest.fixture
def outbox(pileEngine):
    return pileEngine(Email, name="outbox")


def fill(pile, count=3):
    return pile.addMany([{"to": f"user{n}@x.y", "subject": str(n)} for n in range(count)])


def lapse(pile, item):
    pile.collection.update_one({"uid": item.uid}, {"$set": {"leaseUntil": utc_now() - timedelta(seconds=1)}})


def abandoned(pile):
    """An item a worker claimed, and then stopped renewing."""

    item = pile.claim()
    lapse(pile, item)

    return item


# --- filling ---

def test_add_takes_a_model_a_dict_or_keyword_arguments(outbox):
    added = [
        outbox.add(Email(to="a@b.c", subject="model")),
        outbox.add({"to": "d@e.f", "subject": "dict"}),
        outbox.add(to="g@h.i", subject="kwargs"),
    ]

    assert [item.data.subject for item in added] == ["model", "dict", "kwargs"]
    assert outbox.count(status="pending") == 3


def test_the_payload_is_stored_as_plain_data(outbox):
    item = outbox.add(to="a@b.c", subject="hi")
    raw = outbox.collection.find_one({"uid": item.uid})

    assert raw["data"] == {"to": "a@b.c", "subject": "hi"}
    assert (raw["status"], raw["attempts"], raw["claimId"]) == ("pending", 0, None)


def test_a_payload_default_is_filled_in(outbox):
    assert outbox.add(to="a@b.c").data.subject == "(no subject)"


def test_an_invalid_payload_is_refused_and_nothing_is_stored(outbox):
    with pytest.raises(ValidationError):
        outbox.add({"subject": "no recipient"})

    assert outbox.count() == 0


def test_an_untyped_pile_takes_any_dict(pileEngine):
    scraps = pileEngine(name="scraps")
    scraps.add({"anything": [1, 2, {"deep": True}]})

    assert scraps.claim().data == {"anything": [1, 2, {"deep": True}]}


def test_a_payload_is_a_plain_model_not_a_document(outbox):
    data = outbox.add(to="a@b.c").data

    assert isinstance(data, Email) and not isinstance(data, Document)


def test_addMany_adds_everything_in_one_go(outbox):
    assert len(fill(outbox, 5)) == 5
    assert outbox.count(status="pending") == 5


def test_addMany_checks_every_payload_before_adding_any(outbox):
    with pytest.raises(ValidationError):
        outbox.addMany([{"to": "a@b.c"}, {"subject": "no recipient"}])

    assert outbox.count() == 0


def test_addMany_of_nothing_does_nothing(outbox):
    assert outbox.addMany([]) == []
    assert outbox.count() == 0


def test_two_piles_do_not_share_items(pileEngine):
    outbox, scraps = pileEngine(Email, name="outbox"), pileEngine(name="scraps")
    outbox.add(to="a@b.c")
    scraps.add({"anything": 1})

    assert (outbox.count(), scraps.count()) == (1, 1)
    assert outbox.claim().data.to == "a@b.c"


# --- claiming ---

def test_claim_takes_the_item_that_has_waited_longest(outbox):
    fill(outbox, 3)

    assert [outbox.claim().data.to for _ in range(3)] == ["user0@x.y", "user1@x.y", "user2@x.y"]


def test_a_claim_marks_the_item_running_and_uses_a_try(outbox):
    outbox.add(to="a@b.c")
    item = outbox.claim()

    assert (item.status, item.attempts) == ("running", 1)
    assert item.claimedAt is not None and item.claimId is not None
    assert outbox.get(item.uid).claimId == item.claimId


def test_claiming_an_empty_pile_gives_none(outbox):
    assert outbox.claim() is None


def test_a_claimed_item_is_not_offered_again(outbox):
    outbox.add(to="a@b.c")
    outbox.claim()

    assert outbox.claim() is None


def test_claim_takes_a_filter(outbox):
    outbox.addMany([{"to": "skip@x.y", "subject": "no"}, {"to": "take@x.y", "subject": "yes"}])

    assert outbox.claim({"data.subject": "yes"}).data.to == "take@x.y"


def test_a_filter_matching_nothing_claims_nothing(outbox):
    outbox.add(to="a@b.c", subject="hi")

    assert outbox.claim({"data.subject": "nope"}) is None
    assert outbox.count(status="pending") == 1


def test_no_two_threads_claim_the_same_item(pileEngine):
    jobs = pileEngine()
    jobs.addMany([{"n": n} for n in range(30)])
    claimed, lock = [], threading.Lock()

    def drain():
        while (item := jobs.claim()) is not None:
            with lock:
                claimed.append(item.data["n"])

    threads = [threading.Thread(target=drain) for _ in range(4)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join(10)

    assert sorted(claimed) == list(range(30))


def test_a_live_holder_is_not_taken_over(outbox):
    outbox.add(to="a@b.c")
    held = outbox.claim()

    assert outbox.claim() is None
    assert outbox.get(held.uid).claimId == held.claimId


def test_an_item_whose_holder_died_is_claimed_again_while_it_has_tries(pileEngine):
    jobs = pileEngine(maxAttempts=2)
    jobs.add({"n": 1})
    first = abandoned(jobs)
    again = jobs.claim()

    assert again.uid == first.uid
    assert again.attempts == 2
    assert again.claimId != first.claimId


def test_out_of_tries_an_item_is_given_up_at_the_next_claim(outbox):
    outbox.add(to="a@b.c")
    item = abandoned(outbox)

    assert outbox.claim() is None       # its one try went down with its worker

    given = outbox.get(item.uid)

    assert given.status == "failed"
    assert "gave up after 1 try" in given.error
    assert given.claimId is None and given.finishedAt is not None


def test_a_given_up_item_does_not_block_the_next_one(outbox):
    outbox.add(to="dead@b.c")
    abandoned(outbox)
    outbox.add(to="live@b.c")

    assert outbox.claim().data.to == "live@b.c"
    assert outbox.count(status="failed") == 1


def test_a_given_up_item_cannot_be_finished_by_its_old_holder(outbox):
    outbox.add(to="a@b.c")
    item = abandoned(outbox)
    outbox.claim()

    assert outbox.done(item) is False
    assert outbox.get(item.uid).status == "failed"


def test_a_pile_takes_its_max_tries_from_its_settings(pileEngine):
    assert pileEngine(maxAttempts=3).maxAttempts == 3


# --- finishing ---

def test_done_records_a_result_and_lets_the_item_go(outbox):
    outbox.add(to="a@b.c")
    item = outbox.claim()

    assert outbox.done(item, result="delivered") is True

    finished = outbox.get(item.uid)

    assert (finished.status, finished.result, finished.claimId) == ("done", "delivered", None)
    assert finished.finishedAt is not None


def test_fail_records_an_error_and_is_final(pileEngine):
    jobs = pileEngine(maxAttempts=3)
    jobs.add({"n": 1})
    item = jobs.claim()

    assert jobs.fail(item, error="bounced") is True

    failed = jobs.get(item.uid)

    assert (failed.status, failed.error) == ("failed", "bounced")
    assert jobs.claim() is None         # tries left, and never claimed again


def test_finishing_reports_whether_it_finished_an_item(outbox):
    outbox.add(to="a@b.c")
    item = outbox.claim()

    assert outbox.done(item) is True
    assert outbox.fail(item) is False           # one outcome per claim
    assert outbox.done("no-such-uid") is False


def test_a_stale_holder_cannot_finish_fail_release_or_renew_and_the_current_holder_can(pileEngine):
    jobs = pileEngine(maxAttempts=3)
    jobs.add({"n": 1})
    first = abandoned(jobs)
    second = jobs.claim()

    assert jobs.done(first, result="stale") is False
    assert jobs.fail(first, error="stale") is False
    assert jobs.release(first) is False
    assert jobs.renewLease(first) is False
    assert jobs.get(first.uid).claimId == second.claimId

    assert jobs.done(second, result="fresh") is True
    assert jobs.get(first.uid).result == "fresh"


def test_an_item_can_be_finished_by_uid_whatever_holds_it(outbox):
    item = outbox.add(to="a@b.c")
    outbox.claim()

    assert outbox.done(item.uid) is True        # an operator's verdict, not a worker racing for it
    assert outbox.get(item.uid).status == "done"


@pytest.mark.parametrize("verdict", ["done", "fail", "release"])
def test_a_verdict_by_uid_leaves_a_finished_item_as_it_ended(outbox, verdict):
    ended = {}

    for status, finish in (("done", outbox.done), ("failed", outbox.fail)):
        item = outbox.add(to=f"{status}@x.y")
        finish(outbox.claim())
        ended[item.uid] = status

    cancelled = outbox.add(to="canceled@x.y")
    outbox.cancel(cancelled.uid)
    ended[cancelled.uid] = "canceled"

    for uid, status in ended.items():
        assert getattr(outbox, verdict)(uid) is False
        assert outbox.get(uid).status == status


def test_an_item_never_claimed_cannot_be_finished_as_a_claim(outbox):
    added = outbox.add(to="a@b.c")

    with pytest.raises(ValueError, match="claim"):
        outbox.done(added)

    assert outbox.get(added.uid).status == "pending"
    assert outbox.done(added.uid) is True


# --- releasing ---

def test_release_puts_the_item_back_in_its_own_place(outbox):
    fill(outbox, 2)
    first = outbox.claim()

    assert outbox.release(first) is True
    assert outbox.claim().uid == first.uid      # ahead of the item that was added after it


def test_release_gives_the_try_back(outbox):
    outbox.add(to="a@b.c")
    outbox.release(outbox.claim())
    again = outbox.claim()                      # one try allowed, and it is still unused

    assert again is not None
    assert again.attempts == 1


def test_a_released_item_is_claimed_afresh(outbox):
    outbox.add(to="a@b.c")
    first = outbox.claim()
    outbox.release(first)
    released = outbox.get(first.uid)

    assert (released.status, released.claimId, released.claimedAt) == ("pending", None, None)
    assert outbox.claim().claimId != first.claimId


def test_only_a_held_item_can_be_released(outbox):
    item = outbox.add(to="a@b.c")

    assert outbox.release(item.uid) is False
    assert outbox.get(item.uid).attempts == 0
    assert outbox.release("no-such-uid") is False


# --- renewing ---

def test_a_held_items_lease_can_be_renewed(outbox):
    outbox.add(to="a@b.c")
    item = outbox.claim()
    lapse(outbox, item)

    assert outbox.renewLease(item) is True
    assert outbox.get(item.uid).leaseUntil > utc_now()
    assert outbox.claim() is None


def test_renewing_reports_whether_there_was_a_claim_to_renew(outbox):
    outbox.add(to="a@b.c")
    item = outbox.claim()
    outbox.done(item)

    assert outbox.renewLease(item) is False
    assert outbox.renewLease("no-such-uid") is False


# --- cancelling ---

def test_an_item_nobody_is_working_can_be_cancelled(outbox):
    item = outbox.add(to="a@b.c")

    assert outbox.cancel(item.uid) is True

    cancelled = outbox.get(item.uid)

    assert cancelled.status == "canceled"
    assert cancelled.claimedAt is None and cancelled.finishedAt is not None


def test_a_cancelled_item_is_never_claimed(outbox):
    outbox.cancel(outbox.add(to="a@b.c").uid)

    assert outbox.claim() is None


def test_an_item_being_worked_cannot_be_cancelled(outbox):
    outbox.add(to="a@b.c")
    item = outbox.claim()

    assert outbox.cancel(item.uid) is False
    assert outbox.get(item.uid).status == "running"


def test_an_item_whose_holder_died_can_be_cancelled_and_the_holder_cannot_finish_it(outbox):
    outbox.add(to="a@b.c")
    item = abandoned(outbox)

    assert outbox.cancel(item.uid) is True
    assert outbox.done(item) is False
    assert outbox.get(item.uid).status == "canceled"


def test_a_finished_item_cannot_be_cancelled(outbox):
    outbox.add(to="a@b.c")
    item = outbox.claim()
    outbox.done(item)

    assert outbox.cancel(item.uid) is False
    assert outbox.cancel("no-such-uid") is False


def test_cancelMany_takes_a_filter_and_leaves_items_being_worked_alone(outbox):
    outbox.addMany([{"to": "a@x.y"}, {"to": "a@x.y"}, {"to": "b@x.y"}])
    held = outbox.claim({"data.to": "a@x.y"})

    assert outbox.cancelMany({"data.to": "a@x.y"}) == 1
    assert outbox.get(held.uid).status == "running"
    assert outbox.count(status="pending") == 1


# --- looking at the pile ---

def test_count_by_status_and_counts(outbox):
    fill(outbox, 5)
    outbox.done(outbox.claim())
    outbox.fail(outbox.claim())
    outbox.claim()
    outbox.cancel(outbox.find({"status": "pending"})[0].uid)

    assert outbox.count() == 5
    assert outbox.count(status="pending") == 1
    assert outbox.counts() == {"pending": 1, "running": 1, "done": 1, "failed": 1, "canceled": 1}


def test_find_gives_typed_items(outbox):
    fill(outbox, 2)
    items = outbox.find(sort=[("createdAt", 1)])

    assert all(isinstance(item, Item) for item in items)
    assert items[0].data == Email(to="user0@x.y", subject="0")


def test_purge_removes_only_the_status_named(outbox):
    fill(outbox, 3)
    outbox.done(outbox.claim())
    outbox.fail(outbox.claim())

    assert outbox.purge("done") == 1
    assert outbox.counts() == {"pending": 1, "running": 0, "done": 0, "failed": 1, "canceled": 0}


def test_an_item_records_when_it_was_created_claimed_and_finished(outbox):
    added = outbox.add(to="a@b.c")

    assert (added.claimedAt, added.finishedAt) == (None, None)

    outbox.done(outbox.claim())
    finished = outbox.get(added.uid)

    assert finished.createdAt <= finished.claimedAt <= finished.finishedAt


def test_finished_and_held_items_are_untouched_by_another_engine_starting(outbox, pileEngine):
    fill(outbox, 2)
    outbox.done(outbox.claim())
    held = outbox.claim()
    before = sorted(outbox.collection.find({}, {"_id": 0}), key=lambda raw: raw["uid"])

    pileEngine(Email, name="outbox")            # another process, on the same collection

    assert sorted(outbox.collection.find({}, {"_id": 0}), key=lambda raw: raw["uid"]) == before
    assert outbox.get(held.uid).claimId == held.claimId


def test_a_pile_engine_is_a_collection_engine(outbox):
    item = outbox.add(to="a@b.c")

    assert isinstance(outbox, CollectionEngine)
    assert outbox.findOne({"data.to": "a@b.c"}).uid == item.uid


def test_indexes_back_the_claim_query_beside_extra_ones(pileEngine):
    jobs = pileEngine(Email, extraIndexes=[IndexModel([("data.to", 1)], name="byRecipient")])
    names = set(jobs.collection.index_information())

    assert {"status_1_leaseUntil_1", "uid_1", "byRecipient"} <= names
