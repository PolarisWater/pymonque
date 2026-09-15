"""Typed storage: a collection engine over pydantic documents, and documents that save, delete and
reload themselves by the key they were stored under."""

from datetime import datetime

import pytest
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pymongo import ASCENDING, IndexModel
from pymongo.errors import DuplicateKeyError

from pymonque_next import CollectionEngine, Document, utc_now
from pymonque_next.exceptions import UnboundDocument


class Group(Document):
    name:       str = ""
    colorHex:   str = "#FFFFFF"
    archived:   bool = False


class Account(Document):
    accountId:  int
    username:   str = ""
    createdAt:  datetime | None = None


class Member(BaseModel):
    name: str


class Team(Document):
    members:    list[Member] = []
    count:      int = 0


@pytest.fixture
def groups(db) -> CollectionEngine[Group]:
    return CollectionEngine(db["groups"], Group, name="groups")


@pytest.fixture
def accounts(db) -> CollectionEngine[Account]:
    return CollectionEngine(
        db["accounts"], Account, name="accounts", key="accountId",
        extraIndexes=[IndexModel([("username", ASCENDING)], name="username_idx")],
    )


@pytest.fixture
def teams(db) -> CollectionEngine[Team]:
    return CollectionEngine(db["teams"], Team, name="teams")


# --- indexes and keys ---

def test_the_key_is_indexed_uniquely_beside_the_extra_indexes(accounts):
    indexes = accounts.collection.index_information()
    byKey = {tuple(index["key"]): index for index in indexes.values()}

    assert byKey[(("accountId", 1),)]["unique"] is True
    assert "username_idx" in indexes


def test_a_duplicate_key_is_refused(accounts):
    accounts.create(accountId=42)

    with pytest.raises(DuplicateKeyError):
        accounts.create(accountId=42)


def test_a_custom_key_is_used_throughout(accounts):
    accounts.create(accountId=42, username="ada")

    assert accounts.get(42).username == "ada"
    assert accounts.exists(42)
    assert accounts.update(42, username="grace").username == "grace"
    assert accounts.delete(42) is True


def test_an_aliased_key_is_stored_and_found_under_its_alias(db):
    class Legacy(Document):
        accountId: int = Field(alias="account_id")

    legacy = CollectionEngine(db["legacy"], Legacy, name="legacy", key="accountId")
    legacy.create(accountId=7)

    assert legacy.collection.find_one()["account_id"] == 7
    assert legacy.get(7).accountId == 7
    assert (("account_id", 1),) in [tuple(i["key"]) for i in legacy.collection.index_information().values()]


# --- writing ---

def test_create_stores_and_returns_it(groups):
    group = groups.create(name="beta")

    assert group.name == "beta"
    assert groups.get(group.uid).name == "beta"


def test_insert_stores_a_model_you_built(groups):
    group = groups.insert(Group(name="beta"))

    assert groups.get(group.uid).name == "beta"


def test_insertMany_stores_them_all(groups):
    assert len(groups.insertMany(Group(name=n) for n in "abc")) == 3
    assert groups.count() == 3


def test_insertMany_of_nothing_is_a_no_op(groups):
    assert groups.insertMany([]) == []


def test_save_replaces_the_whole_document(groups):
    group = groups.create(name="beta", colorHex="#111111")
    group.name = "gamma"
    groups.save(group)

    after = groups.get(group.uid)

    assert (after.name, after.colorHex) == ("gamma", "#111111")
    assert groups.count() == 1


def test_save_creates_a_missing_document(groups):
    groups.save(Group(name="beta"))

    assert groups.count() == 1


def test_update_merges_the_fields_given(groups):
    group = groups.create(name="beta", colorHex="#111111")
    after = groups.update(group.uid, name="gamma")

    assert (after.name, after.colorHex) == ("gamma", "#111111")


def test_update_writes_only_what_changed(groups, monkeypatch):
    group = groups.create(name="beta", colorHex="#111111")
    written = []
    updateOne = groups.collection.update_one

    def spy(query, update, *args, **kwargs):
        written.append(update)
        return updateOne(query, update, *args, **kwargs)

    monkeypatch.setattr(groups.collection, "update_one", spy)

    groups.update(group.uid, name="gamma", colorHex="#111111")
    groups.update(group.uid, name="gamma")

    assert written == [{"$set": {"name": "gamma"}}]


def test_update_of_a_missing_document_is_none(groups):
    assert groups.update("nope", name="x") is None


def test_update_accepts_nested_models(teams):
    team = teams.create()

    assert teams.update(team.uid, members=[Member(name="Ada")]).members == [Member(name="Ada")]


@pytest.mark.parametrize("fields", [{"count": "many"}, {"nope": 1}], ids=["invalid", "unknown"])
def test_update_refuses_an_invalid_or_unknown_field_and_writes_nothing(teams, fields):
    team = teams.create(count=3)

    with pytest.raises(ValidationError):
        teams.update(team.uid, **fields)

    assert teams.get(team.uid).count == 3


def test_updating_the_key_renames_the_document(accounts):
    accounts.create(accountId=1, username="ada")

    assert accounts.update(1, accountId=2).username == "ada"
    assert accounts.get(1) is None
    assert accounts.count() == 1


def test_delete(groups):
    group = groups.create(name="beta")

    assert groups.delete(group.uid) is True
    assert groups.delete(group.uid) is False
    assert groups.count() == 0


def test_deleteMany(groups):
    groups.insertMany([Group(name="a", archived=True), Group(name="b", archived=True), Group(name="c")])

    assert groups.deleteMany({"archived": True}) == 2
    assert groups.count() == 1


# --- reading ---

def test_get_misses_cleanly(groups):
    assert groups.get("nope") is None


def test_findOne(groups):
    groups.create(name="beta")

    assert groups.findOne({"name": "beta"}).name == "beta"
    assert groups.findOne({"name": "nope"}) is None


def test_find_filters_sorts_and_limits(groups):
    for name in "cab":
        groups.create(name=name)

    assert [g.name for g in groups.find(sort=[("name", 1)])] == ["a", "b", "c"]
    assert len(groups.find(limit=2)) == 2
    assert [g.name for g in groups.find({"name": "b"})] == ["b"]


def test_count_and_exists(groups):
    group = groups.create(name="beta")

    assert groups.count() == 1
    assert groups.count({"name": "nope"}) == 0
    assert groups.exists(group.uid) is True
    assert groups.exists("nope") is False


def test_documents_come_back_typed(groups):
    groups.create(name="beta")

    assert isinstance(groups.find()[0], Group)


def test_a_model_that_forbids_extras_still_loads(db):
    class Strict(Document):
        model_config = ConfigDict(extra="forbid")
        name: str = ""

    strict = CollectionEngine(db["strict"], Strict, name="strict")
    strict.create(name="beta")

    assert strict.findOne().name == "beta"     # MongoDB's _id is left out


# --- nulls are stored, so a field can be cleared ---

def test_a_field_can_be_set_back_to_none(accounts):
    accounts.create(accountId=42, createdAt=utc_now())

    assert accounts.update(42, createdAt=None).createdAt is None


def test_a_saved_document_stores_its_nones(accounts):
    account = accounts.create(accountId=42, createdAt=utc_now())
    account.createdAt = None
    account.save()

    assert accounts.collection.find_one()["createdAt"] is None


# --- bound documents ---

def test_created_fetched_and_inserted_documents_are_bound(groups):
    created = groups.create(name="a")
    inserted = groups.insert(Group(name="b"))

    assert created.bound and inserted.bound
    assert groups.get(created.uid).bound
    assert all(g.bound for g in groups.find())
    assert groups.findOne({"name": "b"}).bound


def test_a_handmade_document_is_unbound_until_it_is_stored(groups):
    group = Group(name="beta")

    assert not group.bound

    with pytest.raises(UnboundDocument):
        group.save()

    groups.insert(group)
    group.name = "gamma"
    group.save()

    assert groups.get(group.uid).name == "gamma"


def test_a_built_document_is_bound_but_not_stored(groups):
    group = groups.build(name="beta")

    assert group.bound
    assert group.storedKey is None
    assert groups.count() == 0

    group.save()

    assert groups.get(group.uid).name == "beta"


def test_the_binding_is_not_stored(groups):
    groups.create(name="beta")

    assert set(groups.collection.find_one()) == {"_id", "uid", "name", "colorHex", "archived"}


def test_a_document_saves_deletes_and_reloads_itself(groups):
    group = groups.create(name="beta")
    group.name = "gamma"
    group.save()

    assert groups.get(group.uid).name == "gamma"

    groups.update(group.uid, name="changed elsewhere")

    assert group.reload().name == "changed elsewhere"
    assert group.name == "gamma"        # the stale copy is left alone
    assert group.delete() is True
    assert groups.count() == 0


# --- a document is a handle on one row, not a copy of it ---

def test_a_document_remembers_the_key_it_was_stored_under(accounts):
    assert accounts.create(accountId=1).storedKey == 1
    assert Account(accountId=1).storedKey is None


def test_changing_the_key_renames_rather_than_copies(accounts):
    account = accounts.create(accountId=1, username="ada")
    account.accountId = 2
    account.save()

    assert accounts.get(1) is None
    assert accounts.get(2).username == "ada"
    assert accounts.count() == 1


def test_delete_and_reload_use_the_key_it_was_stored_under(accounts):
    account = accounts.create(accountId=1, username="ada")
    accounts.update(1, username="grace")
    account.accountId = 99              # not saved

    assert account.reload().username == "grace"
    assert account.delete() is True
    assert accounts.count() == 0


def test_saving_again_after_a_rename_keeps_one_row(accounts):
    account = accounts.create(accountId=1, username="ada")
    account.accountId = 2
    account.save()
    account.username = "grace"
    account.save()

    assert accounts.count() == 1
    assert accounts.get(2).username == "grace"


def test_an_inserted_document_is_bound_to_its_key(accounts):
    account = accounts.insert(Account(accountId=7, username="hand built"))
    account.username = "changed"
    account.save()

    assert accounts.count() == 1
    assert accounts.get(7).username == "changed"
