"""Claims in any collection: taking the document whose lease ran out first, writing only under the
claim that holds a document, and renewing held leases in the background until the work ends."""

import time
from datetime import timedelta

import pytest

from pymonque_next import utc_now
from pymonque_next.claims import Leases, claimNext, writeClaimed

from tests_next.helpers import waitFor


@pytest.fixture
def jobs(db):
    return db["jobs"]


def add(jobs, uid, leaseUntil=None, **fields):
    jobs.insert_one({"uid": uid, "status": "pending", "leaseUntil": leaseUntil or utc_now() - timedelta(seconds=1), **fields})


def lapse(jobs, uid):
    jobs.update_one({"uid": uid}, {"$set": {"leaseUntil": utc_now() - timedelta(seconds=1)}})


def claim(jobs, leaseSeconds=60, **kwargs):
    return claimNext(jobs, {}, leaseSeconds=leaseSeconds, **kwargs)


# --- claiming ---

def test_a_claim_takes_the_document_whose_lease_ran_out_first(jobs):
    now = utc_now()
    add(jobs, "recent", now - timedelta(minutes=1))
    add(jobs, "oldest", now - timedelta(hours=1))

    assert claim(jobs).after["uid"] == "oldest"


def test_a_live_lease_is_left_alone(jobs):
    add(jobs, "held", utc_now() + timedelta(minutes=5))

    assert claim(jobs) is None


def test_a_claim_takes_only_what_matches(jobs):
    add(jobs, "finished", status="done")

    assert claimNext(jobs, {"status": "pending"}, leaseSeconds=60) is None


def test_a_claim_stores_its_claimId_and_its_lease(jobs):
    add(jobs, "a")
    taken = claim(jobs, leaseSeconds=60)
    stored = jobs.find_one({"uid": "a"})

    assert stored["claimId"] == taken.claimId
    assert timedelta(seconds=59) < stored["leaseUntil"] - utc_now() <= timedelta(seconds=60)


def test_every_claim_gets_its_own_claimId(jobs):
    add(jobs, "a")
    first = claim(jobs)
    lapse(jobs, "a")

    assert claim(jobs).claimId != first.claimId


def test_a_claim_shows_the_document_as_it_found_it_and_as_it_left_it(jobs):
    add(jobs, "a", status="running", tries=2)
    taken = claim(jobs, fields={"status": "running"}, inc={"tries": 1})

    assert (taken.before["tries"], taken.after["tries"]) == (2, 3)
    assert taken.before["status"] == "running"
    assert jobs.find_one({"uid": "a"})["tries"] == 3


# --- writing under a claim ---

def test_only_the_claim_holding_a_document_writes_to_it(jobs):
    add(jobs, "a")
    first = claim(jobs)
    lapse(jobs, "a")
    second = claim(jobs)

    assert writeClaimed(jobs, "a", first.claimId, {"status": "done"}) is False
    assert writeClaimed(jobs, "a", second.claimId, {"status": "done"}) is True
    assert jobs.find_one({"uid": "a"})["status"] == "done"


def test_an_outcome_lets_the_document_go_so_no_second_one_lands(jobs):
    add(jobs, "a")
    taken = claim(jobs)

    assert writeClaimed(jobs, "a", taken.claimId, {"status": "done"}) is True
    assert jobs.find_one({"uid": "a"})["claimId"] is None
    assert writeClaimed(jobs, "a", taken.claimId, {"status": "failed"}) is False


def test_nothing_is_written_under_no_claim(jobs):
    add(jobs, "a")

    with pytest.raises(ValueError, match="no claim"):
        writeClaimed(jobs, "a", None, {"status": "done"})


# --- leases ---

def test_renewing_pushes_out_the_lease_of_a_held_claim(jobs):
    add(jobs, "a")
    taken = claim(jobs, leaseSeconds=1)
    leases = Leases(jobs, 600, name="jobs")

    with leases.holding("a", taken.claimId):
        assert leases.renew() == 1

    assert jobs.find_one({"uid": "a"})["leaseUntil"] > utc_now() + timedelta(seconds=500)


def test_renewal_skips_a_document_another_worker_took(jobs):
    add(jobs, "a")
    first = claim(jobs)
    lapse(jobs, "a")
    second = claim(jobs, leaseSeconds=60)
    leases = Leases(jobs, 600, name="jobs")

    with leases.holding("a", first.claimId):
        assert leases.renew() == 0

    stored = jobs.find_one({"uid": "a"})

    assert stored["claimId"] == second.claimId
    assert stored["leaseUntil"] < utc_now() + timedelta(seconds=61)


def test_renewal_never_overwrites_an_outcome(jobs):
    add(jobs, "a")
    taken = claim(jobs)
    leases = Leases(jobs, 600, name="jobs")

    with leases.holding("a", taken.claimId):
        writeClaimed(jobs, "a", taken.claimId, {"status": "done"})
        ended = jobs.find_one({"uid": "a"})

        assert leases.renew() == 0

    assert jobs.find_one({"uid": "a"}) == ended


def test_a_held_claim_is_renewed_in_the_background(jobs):
    add(jobs, "a")
    taken = claim(jobs, leaseSeconds=0.3)
    leases = Leases(jobs, 0.3, name="jobs")

    with leases.holding("a", taken.claimId):
        time.sleep(0.7)     # more than two leases, so only renewal keeps it

        assert jobs.find_one({"uid": "a"})["leaseUntil"] > utc_now()
        assert claim(jobs) is None


def test_the_renewing_thread_ends_with_the_last_claim_let_go(jobs):
    leases = Leases(jobs, 600, name="jobs")
    leases.hold("a", "claim-a")
    leases.hold("b", "claim-b")
    leases.release("claim-a")

    assert leases.renewing

    leases.release("claim-b")

    assert waitFor(lambda: not leases.renewing, timeout=1)     # at once, not an interval of 200s later
    assert leases.renew() == 0


def test_a_claim_held_after_the_thread_ended_is_renewed_again(jobs):
    leases = Leases(jobs, 0.3, name="jobs")

    with leases.holding("x", "an-earlier-claim"):
        pass

    assert waitFor(lambda: not leases.renewing, timeout=1)

    add(jobs, "a")
    taken = claim(jobs, leaseSeconds=0.3)

    with leases.holding("a", taken.claimId):
        time.sleep(0.7)

        assert jobs.find_one({"uid": "a"})["leaseUntil"] > utc_now()
