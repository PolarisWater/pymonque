"""PileEngine: filling a pile, claiming from it atomically, and finishing items."""

from datetime import timedelta

import pytest
from pydantic import BaseModel, ValidationError

from pymonque import BaseApp, Item, PileEngine, task, pile, utc_now

from conftest import Email, ExampleApp


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


def test_a_released_item_keeps_its_attempt_count(app):
    app.outbox.add(to="a@b.c")
    app.outbox.release(app.outbox.claim())

    assert app.outbox.claim().attempts == 2


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

def test_an_abandoned_item_is_claimable_again_when_its_lease_lapses(app):
    app.outbox.add(to="a@b.c")
    item = app.outbox.claim()

    assert app.outbox.claim() is None      # while the lease is live, nobody else gets it

    app.outbox.itemsCollection.update_one(
        {"uid": item.uid}, {"$set": {"leaseUntil": utc_now() - timedelta(hours=1)}}
    )
    again = app.outbox.claim()             # no restart needed

    assert again.uid == item.uid
    assert again.attempts == 2           # and we can tell it was tried before


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


def test_under_fail_an_abandoned_item_is_never_retried(db):
    app = ExampleApp(db, staleItemsPolicy="fail")
    app.outbox.add(to="a@b.c")
    item = app.outbox.claim()
    app.outbox.itemsCollection.update_one(
        {"uid": item.uid}, {"$set": {"leaseUntil": utc_now() - timedelta(hours=1)}}
    )

    assert app.outbox.claim() is None      # not handed to anyone else

    app.init()                             # a worker starting up writes it off
    after = app.outbox.find()[0]

    assert after.status == "failed"
    assert "lease" in after.error


def test_a_pile_can_override_the_app_policy(db):
    class Q(BaseApp):
        strict = pile(policy="fail")
        lenient = pile()

    first = Q(db, staleItemsPolicy="retry")
    first.strict.add({"n": 1})
    first.lenient.add({"n": 1})
    strictItem = first.strict.claim()
    first.lenient.claim()

    for engine in (first.strict, first.lenient):   # both holders died
        engine.itemsCollection.update_many(
            {}, {"$set": {"leaseUntil": utc_now() - timedelta(hours=1)}}
        )

    Q(db, staleItemsPolicy="retry").init()

    assert first.strict.count(status="failed") == 1     # its own policy wins
    assert first.lenient.claim() is not None     # claimable again


def test_finished_items_survive_a_restart(db, app):
    app.outbox.add(to="a@b.c")
    app.outbox.done(app.outbox.claim())

    ExampleApp(db).init()

    assert app.outbox.count(status="done") == 1
