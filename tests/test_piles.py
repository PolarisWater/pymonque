"""PileEngine: filling a pile, claiming from it atomically, and finishing items."""

from datetime import timedelta

import pytest
from pydantic import BaseModel, ValidationError

from pymonque import BaseApp, Item, PileEngine, task, pile, utc_now

from conftest import Email, ExampleApp, appWith


def fill(app, count: int = 3):
    return app.outbox.addMany([{"to": f"user{n}@x.y", "subject": str(n)} for n in range(count)])


# --- declaring piles ---

def test_each_pile_gets_its_own_collection(app):
    assert app.outbox.itemsCollection.name == "pymonque_pile_outbox"
    assert app.scraps.itemsCollection.name == "pymonque_pile_scraps"


def test_piles_are_listed_on_the_app(app):
    assert set(app.piles) == {"outbox", "scraps"}
    assert isinstance(app.outbox, PileEngine)


def test_class_access_returns_the_declaration():
    assert isinstance(ExampleApp.outbox, pile)
    assert ExampleApp.outbox.name == "outbox"


def test_the_collection_name_can_be_overridden(db):
    class Q(BaseApp):
        things = pile(itemsCollection="my_things")

    assert Q(db).things.itemsCollection.name == "my_things"


def test_a_subclass_can_override_a_pile(db):
    class Child(ExampleApp):
        outbox = pile(Email, itemsCollection="child_outbox")

    assert Child(db).outbox.itemsCollection.name == "child_outbox"


def test_two_piles_do_not_share_items(app):
    app.outbox.add(to="a@b.c")
    app.scraps.add({"anything": 1})

    assert app.outbox.count() == 1
    assert app.scraps.count() == 1
    assert app.outbox.claim().data.to == "a@b.c"


# --- filling ---

def test_add_accepts_a_model_a_dict_or_kwargs(app):
    a = app.outbox.add(Email(to="a@b.c", subject="model"))
    b = app.outbox.add({"to": "d@e.f", "subject": "dict"})
    c = app.outbox.add(to="g@h.i", subject="kwargs")

    assert [i.data.subject for i in (a, b, c)] == ["model", "dict", "kwargs"]
    assert app.outbox.count(status="pending") == 3


def test_the_payload_is_stored_as_plain_data(app):
    item = app.outbox.add(to="a@b.c", subject="hi")
    raw = app.outbox.itemsCollection.find_one({"uid": item.uid})

    assert raw["data"] == {"to": "a@b.c", "subject": "hi"}
    assert raw["status"] == "pending"
    assert raw["attempts"] == 0


def test_a_defaulted_payload_field_is_filled_in(app):
    assert app.outbox.add(to="a@b.c").data.subject == "(no subject)"


def test_an_invalid_payload_is_rejected(app):
    with pytest.raises(ValidationError):
        app.outbox.add({"subject": "no recipient"})

    assert app.outbox.count() == 0


def test_an_untyped_pile_takes_any_dict(app):
    app.scraps.add({"anything": [1, 2, {"deep": True}]})

    assert app.scraps.claim().data == {"anything": [1, 2, {"deep": True}]}


def test_addMany_inserts_in_one_go(app):
    items = fill(app, 5)

    assert len(items) == 5
    assert app.outbox.count(status="pending") == 5


def test_addMany_of_nothing_is_a_no_op(app):
    assert app.outbox.addMany([]) == []
    assert app.outbox.count() == 0


def test_addMany_validates_before_inserting_anything(app):
    with pytest.raises(ValidationError):
        app.outbox.addMany([{"to": "a@b.c"}, {"subject": "no recipient"}])

    assert app.outbox.count() == 0


# --- claiming ---

def test_claim_takes_the_oldest_item(app):
    fill(app, 3)

    assert [app.outbox.claim().data.to for _ in range(3)] == [
        "user0@x.y", "user1@x.y", "user2@x.y",
    ]


def test_claim_marks_the_item_and_counts_the_attempt(app):
    app.outbox.add(to="a@b.c")
    item = app.outbox.claim()

    assert item.status == "claimed"
    assert item.attempts == 1
    assert item.claimedAt is not None
    assert app.outbox.count(status="pending") == 0


def test_no_two_claims_return_the_same_item(app):
    fill(app, 2)
    first, second = app.outbox.claim(), app.outbox.claim()

    assert first.uid != second.uid
    assert app.outbox.count(status="claimed") == 2


def test_claim_on_an_empty_pile_returns_none(app):
    assert app.outbox.claim() is None


def test_a_claimed_item_is_not_offered_again(app):
    app.outbox.add(to="a@b.c")
    app.outbox.claim()

    assert app.outbox.claim() is None


def test_claim_can_filter(app):
    app.outbox.addMany([
        {"to": "skip@x.y", "subject": "no"},
        {"to": "take@x.y", "subject": "yes"},
    ])

    assert app.outbox.claim({"data.subject": "yes"}).data.to == "take@x.y"


def test_a_filter_that_matches_nothing_returns_none(app):
    app.outbox.add(to="a@b.c", subject="hi")

    assert app.outbox.claim({"data.subject": "nope"}) is None
    assert app.outbox.count(status="pending") == 1  # nothing was claimed


# --- finishing ---

def test_done_records_a_result(app):
    app.outbox.add(to="a@b.c")
    item = app.outbox.claim()
    app.outbox.done(item, result="delivered")
    after = app.outbox.find({"uid": item.uid})[0]

    assert after.status == "done"
    assert after.result == "delivered"
    assert after.finishedAt is not None


def test_fail_records_an_error(app):
    app.outbox.add(to="a@b.c")
    app.outbox.fail(app.outbox.claim(), error="bounced")
    after = app.outbox.find({"status": "failed"})[0]

    assert after.error == "bounced"


def test_release_puts_the_item_back(app):
    app.outbox.add(to="a@b.c")
    item = app.outbox.claim()
    app.outbox.release(item)
    after = app.outbox.find({"uid": item.uid})[0]

    assert after.status == "pending"
    assert after.claimedAt is None
    assert app.outbox.claim().uid == item.uid  # claimable again


def test_an_item_can_be_finished_by_uid(app):
    item = app.outbox.add(to="a@b.c")
    app.outbox.claim()
    app.outbox.done(item.uid)

    assert app.outbox.count(status="done") == 1


# --- work() ---

def test_work_marks_the_item_done(app):
    app.outbox.add(to="a@b.c")

    with app.outbox.work() as item:
        assert item.data.to == "a@b.c"

    assert app.outbox.count(status="done") == 1


def test_work_marks_the_item_failed_and_re_raises(app):
    app.outbox.add(to="a@b.c")

    with pytest.raises(ValueError):
        with app.outbox.work():
            raise ValueError("nope")

    failed = app.outbox.find({"status": "failed"})[0]
    assert "ValueError: nope" in failed.error


def test_work_yields_none_on_an_empty_pile(app):
    with app.outbox.work() as item:
        assert item is None


def test_work_takes_a_filter(app):
    app.outbox.addMany([
        {"to": "skip@x.y", "subject": "no"},
        {"to": "take@x.y", "subject": "yes"},
    ])

    with app.outbox.work({"data.subject": "yes"}) as item:
        assert item.data.to == "take@x.y"


def test_a_task_can_drain_the_pile(app, tasks):
    from pymonque import Task

    app.outbox.add(to="a@b.c")
    app.task.schedule(ExampleApp.send_one())
    app.task._work()
    result = Task.model_validate(tasks.find_one()).result

    assert result == "sent to a@b.c"
    assert app.outbox.count(status="done") == 1


def test_a_task_on_an_empty_pile_still_succeeds(app, tasks):
    from pymonque import Task

    app.task.schedule(ExampleApp.send_one())
    app.task._work()

    assert Task.model_validate(tasks.find_one()).result == "empty"


# --- inspecting and managing ---

def test_count_and_counts(app):
    fill(app, 4)
    app.outbox.done(app.outbox.claim())
    app.outbox.fail(app.outbox.claim())
    app.outbox.claim()

    assert app.outbox.count() == 4
    assert app.outbox.count(status="pending") == 1
    assert app.outbox.counts() == {"pending": 1, "claimed": 1, "done": 1, "failed": 1}


def test_find_returns_typed_items(app):
    fill(app, 2)
    items = app.outbox.find()

    assert len(items) == 2
    assert all(isinstance(i, Item) for i in items)
    assert items[0].data.to == "user0@x.y"


def test_purge_only_removes_the_named_status(app):
    fill(app, 3)
    app.outbox.done(app.outbox.claim())
    app.outbox.fail(app.outbox.claim())

    assert app.outbox.purge("done") == 1
    assert app.outbox.counts() == {"pending": 1, "claimed": 0, "done": 0, "failed": 1}


def test_indexes_back_the_claim_query(app):
    keys = [tuple(index["key"]) for index in app.outbox.itemsCollection.index_information().values()]

    assert (("status", 1), ("leaseUntil", 1)) in keys
    assert (("uid", 1),) in keys


# --- restart policies ---

def test_a_live_lease_survives_a_new_instance(db, app):
    app.outbox.add(to="a@b.c")
    item = app.outbox.claim()

    ExampleApp(db)                     # another process starts up
    after = app.outbox.find()[0]

    assert after.status == "claimed"     # still held by whoever has it
    assert after.uid == item.uid


def test_a_held_item_lease_can_be_renewed(app):
    app.outbox.add(to="a@b.c")
    item = app.outbox.claim()
    app.outbox.itemsCollection.update_one(
        {"uid": item.uid}, {"$set": {"leaseUntil": utc_now() - timedelta(hours=1)}}
    )

    app.outbox.renewLease(item)

    assert app.outbox.find()[0].leaseUntil > utc_now()   # holding on to it again
    assert app.outbox.claim() is None                    # so nobody else may take it


def abandoned(app, to="a@b.c"):
    """An item whose holder was claimed and then stopped renewing."""

    app.outbox.add(to=to)
    item = app.outbox.claim()
    app.outbox.itemsCollection.update_one(
        {"uid": item.uid}, {"$set": {"leaseUntil": utc_now() - timedelta(hours=1)}}
    )
    return item


def test_an_abandoned_item_is_given_up_on_at_the_next_claim(app):
    abandoned(app)

    assert app.outbox.claim() is None      # its one attempt went down with its worker

    after = app.outbox.find()[0]
    assert after.status == "failed"
    assert "gave up after 1 attempt(s)" in after.error
    assert after.claimId is None


def test_an_abandoned_item_with_attempts_left_is_claimed_again(db):
    app = appWith(ExampleApp, db, itemMaxAttempts=2)
    first = abandoned(app)

    again = app.outbox.claim()

    assert again.uid == first.uid
    assert again.attempts == 2


def test_a_given_up_item_does_not_block_the_next_one(app):
    abandoned(app, to="dead@b.c")
    app.outbox.add(to="live@b.c")

    assert app.outbox.claim().data.to == "live@b.c"
    assert app.outbox.count(status="failed") == 1


def test_a_live_holder_is_not_taken_over(app):
    """A worker that is still renewing must survive another worker's claim."""

    app.outbox.add(to="a@b.c")
    held = app.outbox.claim()              # lease is live, being renewed

    assert app.outbox.claim() is None
    assert app.outbox.get(held.uid).status == "claimed"


# --- retries: the same rule as tasks ---

def test_a_failure_is_final_by_default(app):
    app.outbox.add(to="a@b.c")

    with pytest.raises(ValueError):
        with app.outbox.work():
            raise ValueError("nope")

    assert app.outbox.count(status="failed") == 1
    assert app.outbox.claim() is None


def test_a_failure_with_attempts_left_goes_back_on_the_pile(db):
    class Q(BaseApp):
        jobs = pile(maxAttempts=3, retryDelay=0)

    app = Q(db)
    app.jobs.add({"n": 1})

    for _ in range(3):
        with pytest.raises(ValueError):
            with app.jobs.work():
                raise ValueError("nope")

    after = app.jobs.find()[0]
    assert after.status == "failed"
    assert after.attempts == 3
    assert "nope" in after.error
    assert app.jobs.claim() is None


def test_a_retry_waits_its_delay(db):
    class Q(BaseApp):
        jobs = pile(maxAttempts=2, retryDelay=60)

    app = Q(db)
    item = app.jobs.add({"n": 1})

    assert app.jobs.fail(app.jobs.claim(), error="nope") is True

    assert app.jobs.get(item.uid).status == "pending"
    assert app.jobs.claim() is None        # not for another minute


def test_a_success_after_a_retry_is_done(db):
    class Q(BaseApp):
        jobs = pile(maxAttempts=2, retryDelay=0)

    app = Q(db)
    item = app.jobs.add({"n": 1})
    app.jobs.fail(app.jobs.claim(), error="nope")

    with app.jobs.work() as again:
        pass

    assert again.attempts == 2
    assert app.jobs.get(item.uid).status == "done"


def test_failing_by_uid_is_final(db):
    class Q(BaseApp):
        jobs = pile(maxAttempts=3)

    app = Q(db)
    item = app.jobs.add({"n": 1})
    app.jobs.claim()

    assert app.jobs.fail(item.uid, error="by hand") is True
    assert app.jobs.get(item.uid).status == "failed"


def test_releasing_does_not_use_up_an_attempt(app):
    app.outbox.add(to="a@b.c")
    app.outbox.release(app.outbox.claim())

    again = app.outbox.claim()             # one attempt allowed, and it is still unused

    assert again is not None
    assert again.attempts == 1


def test_only_a_claimed_item_can_be_released(app):
    item = app.outbox.add(to="a@b.c")

    assert app.outbox.release(item.uid) is False
    assert app.outbox.get(item.uid).attempts == 0


def test_a_pile_can_override_the_app_limits(db):
    class Q(BaseApp):
        itemMaxAttempts = 5
        itemRetryDelay = 30
        strict = pile(maxAttempts=1, retryDelay=0)
        lenient = pile()

    app = Q(db)

    assert (app.strict.maxAttempts, app.strict.retryDelay) == (1, 0)
    assert (app.lenient.maxAttempts, app.lenient.retryDelay) == (5, 30)


@pytest.mark.parametrize("limits", [{"maxAttempts": 0}, {"retryDelay": -1}])
def test_invalid_item_limits_are_rejected(db, limits):
    class Q(BaseApp):
        jobs = pile(**limits)

    with pytest.raises(ValueError):
        Q(db)


def test_finished_items_survive_a_restart(db, app):
    app.outbox.add(to="a@b.c")
    app.outbox.done(app.outbox.claim())

    ExampleApp(db).init()

    assert app.outbox.count(status="done") == 1


# --- the api hands back its own values, not the driver's ---

def test_finishing_an_item_reports_whether_it_changed_one(app):
    app.outbox.add(to="a@b.c")
    item = app.outbox.claim()

    assert app.outbox.done(item) is True
    assert app.outbox.done("no-such-uid") is False


def test_failing_and_releasing_report_the_same_way(app):
    app.outbox.add(to="a@b.c")
    item = app.outbox.claim()

    assert app.outbox.fail(item, error="nope") is True
    assert app.outbox.release("no-such-uid") is False


def test_renewing_a_lease_reports_whether_there_was_one(app):
    app.outbox.add(to="a@b.c")
    item = app.outbox.claim()

    assert app.outbox.renewLease(item) is True
    assert app.outbox.renewLease("no-such-uid") is False


def test_sys_exit_inside_work_fails_the_item(app):
    import sys

    item = app.outbox.add(to="a@b.c")

    with pytest.raises(SystemExit):
        with app.outbox.work():
            sys.exit(2)

    assert app.outbox.get(item.uid).status == "failed"


def test_done_after_a_retry_clears_the_earlier_error(db):
    class Q(BaseApp):
        jobs = pile(maxAttempts=2, retryDelay=0)

    app = Q(db)
    item = app.jobs.add({"n": 1})
    app.jobs.fail(app.jobs.claim(), error="first try failed")

    with app.jobs.work():
        pass

    assert app.jobs.get(item.uid).error is None
