"""Typed object storage: a collection of pydantic documents with CRUD."""

from datetime import datetime

import pytest
from pydantic import BaseModel
from pymongo import IndexModel, ASCENDING

from pymonque import (
    BaseApp, CollectionEngine, Document, collection, task, utc_now,
)
from pymonque.exceptions import UnboundDocument


class Group(Document):
    name:       str = ""
    colorHex:   str = "#FFFFFF"
    archived:   bool = False


class Account(Document):
    accountId:  int
    username:   str = ""
    createdAt:  datetime | None = None


class StoreApp(BaseApp):
    groups   = collection(Group)
    accounts = collection(Account, key="accountId", extraIndexes=[
        IndexModel([("username", ASCENDING)], name="username_idx"),
    ])
    legacy   = collection(Group, collection="my_existing_groups")

    @task
    def countGroups(self) -> int:
        return self.groups.count()


@pytest.fixture
def store(db):
    return StoreApp(db)


# --- declaring ---

def test_collections_are_collected(store):
    assert set(store.collections) == {"groups", "accounts", "legacy"}
    assert isinstance(store.groups, CollectionEngine)


def test_it_defaults_to_its_own_attribute_name(store):
    assert store.groups.collection.name == "groups"
    assert store.accounts.collection.name == "accounts"


def test_an_existing_collection_can_be_named(store):
    assert store.legacy.collection.name == "my_existing_groups"


def test_class_access_returns_the_declaration():
    assert isinstance(StoreApp.groups, collection)
    assert repr(StoreApp.groups) == "collection groups (Group)"


def test_a_subclass_can_override_one(db):
    class Child(StoreApp):
        groups = collection(Group, collection="other_groups")

    assert Child(db).groups.collection.name == "other_groups"


def test_the_key_is_indexed_uniquely(store):
    keys = [tuple(i["key"]) for i in store.accounts.collection.index_information().values()]

    assert (("accountId", 1),) in keys
    assert (("username", 1),) in keys


def test_a_task_can_reach_a_collection(store):
    store.groups.create(name="a")

    assert store.countGroups() == 1


# --- writing ---

def test_create_stores_and_returns_it(store):
    group = store.groups.create(name="beta")

    assert group.name == "beta"
    assert store.groups.count() == 1
    assert store.groups.get(group.uid).name == "beta"


def test_insert_takes_a_model_you_built(store):
    group = store.groups.insert(Group(name="beta"))

    assert store.groups.get(group.uid).name == "beta"


def test_insertMany(store):
    made = store.groups.insertMany(Group(name=n) for n in "abc")

    assert len(made) == 3
    assert store.groups.count() == 3


def test_insertMany_of_nothing_is_a_no_op(store):
    assert store.groups.insertMany([]) == []


def test_save_replaces_the_whole_document(store):
    group = store.groups.create(name="beta", colorHex="#111111")
    group.name = "gamma"
    store.groups.save(group)
    after = store.groups.get(group.uid)

    assert after.name == "gamma"
    assert after.colorHex == "#111111"
    assert store.groups.count() == 1


def test_save_creates_it_if_it_is_missing(store):
    store.groups.save(Group(name="beta"))

    assert store.groups.count() == 1


def test_update_merges_fields_without_reading_first(store):
    group = store.groups.create(name="beta", colorHex="#111111")
    after = store.groups.update(group.uid, name="gamma")

    assert after.name == "gamma"
    assert after.colorHex == "#111111"   # untouched


def test_update_of_a_missing_key_returns_none(store):
    assert store.groups.update("nope", name="x") is None


def test_delete(store):
    group = store.groups.create(name="beta")

    assert store.groups.delete(group.uid) is True
    assert store.groups.delete(group.uid) is False
    assert store.groups.count() == 0


def test_deleteMany(store):
    store.groups.insertMany([Group(name="a", archived=True), Group(name="b", archived=True),
                             Group(name="c")])

    assert store.groups.deleteMany({"archived": True}) == 2
    assert store.groups.count() == 1


# --- reading ---

def test_get_misses_cleanly(store):
    assert store.groups.get("nope") is None


def test_findOne(store):
    store.groups.create(name="beta")

    assert store.groups.findOne({"name": "beta"}).name == "beta"
    assert store.groups.findOne({"name": "nope"}) is None


def test_find_filters_sorts_and_limits(store):
    for n in "cab":
        store.groups.create(name=n)

    assert [g.name for g in store.groups.find(sort=[("name", 1)])] == ["a", "b", "c"]
    assert len(store.groups.find(limit=2)) == 2
    assert [g.name for g in store.groups.find({"name": "b"})] == ["b"]


def test_count_and_exists(store):
    group = store.groups.create(name="beta")

    assert store.groups.count() == 1
    assert store.groups.count({"name": "nope"}) == 0
    assert store.groups.exists(group.uid) is True
    assert store.groups.exists("nope") is False


def test_documents_come_back_typed(store):
    store.groups.create(name="beta")

    assert isinstance(store.groups.find()[0], Group)


# --- a custom key ---

def test_a_custom_key_is_used_throughout(store):
    account = store.accounts.create(accountId=42, username="ada")

    assert store.accounts.get(42).username == "ada"
    assert store.accounts.update(42, username="grace").username == "grace"
    assert store.accounts.delete(42) is True


def test_a_duplicate_key_is_refused(store):
    from pymongo.errors import DuplicateKeyError

    store.accounts.create(accountId=42)

    with pytest.raises(DuplicateKeyError):
        store.accounts.create(accountId=42)


# --- bound documents ---

def test_a_created_document_is_bound(store):
    group = store.groups.create(name="beta")

    assert group.bound


def test_a_fetched_document_is_bound(store):
    uid = store.groups.create(name="beta").uid

    assert store.groups.get(uid).bound
    assert store.groups.find()[0].bound
    assert store.groups.findOne({"name": "beta"}).bound


def test_a_document_can_save_itself(store):
    group = store.groups.create(name="beta")
    group.name = "gamma"
    group.save()

    assert store.groups.get(group.uid).name == "gamma"


def test_a_document_can_delete_itself(store):
    group = store.groups.create(name="beta")

    assert group.delete() is True
    assert store.groups.count() == 0


def test_a_document_can_reload_itself(store):
    group = store.groups.create(name="beta")
    store.groups.update(group.uid, name="changed elsewhere")

    assert group.reload().name == "changed elsewhere"
    assert group.name == "beta"   # the stale copy is left alone


def test_a_handmade_document_is_unbound_until_it_is_stored(store):
    group = Group(name="beta")

    assert not group.bound
    with pytest.raises(UnboundDocument):
        group.save()

    store.groups.insert(group)
    group.name = "gamma"
    group.save()

    assert store.groups.get(group.uid).name == "gamma"


def test_binding_is_not_stored(store):
    group = store.groups.create(name="beta")

    assert "_engine" not in store.groups.collection.find_one()


# --- the engines are collections too ---

def test_the_task_engine_is_one(app):
    from pymonque import TaskEngine
    from conftest import ExampleApp

    app.task.schedule(ExampleApp.greet(name="Ada"))

    assert isinstance(app.task, CollectionEngine)
    assert app.task.count() == 1
    assert app.task.find()[0].work.functionName == "greet"


def test_the_scheduler_engine_is_one(app):
    from conftest import ExampleApp

    scheduler = app.scheduler.add(
        ExampleApp.greet(name="Ada"), app.distribution("constant", dailyFrequency=24)
    )

    assert app.scheduler.get(scheduler.uid).uid == scheduler.uid
    assert app.scheduler.count() == 1


def test_the_pile_engine_is_one(app):
    app.outbox.add(to="a@b.c")

    assert isinstance(app.outbox, CollectionEngine)
    assert app.outbox.count() == 1
    assert app.outbox.findOne({"data.to": "a@b.c"}) is not None


def test_a_task_can_be_given_a_model(db):
    from pymonque import Task, TaskEngine

    class Tenanted(Task):
        tenant: str = "default"

    class Q(BaseApp):
        @task
        @staticmethod
        def ping() -> str: return "pong"

    app = Q(db)
    app.task.model = Tenanted     # the override schedulers and piles already had

    app.task.schedule(Q.ping())
    app.task.collection.update_many({}, {"$set": {"tenant": "acme"}})

    assert app.task.find()[0].tenant == "acme"


# --- nulls are stored, so a field can be cleared ---

def test_a_field_can_be_set_back_to_none(store):
    account = store.accounts.create(accountId=42, username="ada", createdAt=utc_now())

    assert store.accounts.update(42, createdAt=None).createdAt is None


def test_a_saved_document_stores_its_nones(store):
    account = store.accounts.create(accountId=42, createdAt=utc_now())
    account.createdAt = None
    account.save()

    assert store.accounts.get(42).createdAt is None
