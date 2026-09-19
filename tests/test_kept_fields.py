"""The fields an engine keeps: each stored model names them — a task's, an item's and a scheduler's
identity, state, claim and outcome — and the default API never writes them. `update()`, `build()`,
`create()` and the kinds' own builders refuse them; `save()` leaves them as stored, so a copy read
before a worker finished cannot put the work back or wipe the claim holding it."""

from datetime import timedelta

import pytest

from pymonque import CallSpec, Document, Item, Scheduler, Task, utc_now


def greet(name: str = "Ada") -> str:
    return f"Hello, {name}!"


class AccountTask(Task):
    accountId: int = 0


class Note(Document):
    text: str = ""


@pytest.fixture
def tasks(taskEngine):
    return taskEngine({"greet": greet})


@pytest.fixture
def outbox(pileEngine):
    return pileEngine(name="outbox")


# --- each stored model names its own ---

def test_each_stored_model_names_the_fields_its_engine_keeps():
    assert Document._kept == frozenset()
    assert {"uid", "status", "claimId", "leaseUntil", "result"} <= Task._kept
    assert {"uid", "status", "claimId", "leaseUntil", "attempts"} <= Item._kept
    assert Scheduler._kept == {"uid", "status", "claimId", "leaseUntil"}
    assert AccountTask._kept == Task._kept and "accountId" not in AccountTask._kept     # inherited; yours stay yours


# --- save() leaves them as stored ---

def test_a_stale_copy_of_an_item_cannot_put_it_back_on_the_pile(outbox):
    item = outbox.add({"to": "a"})
    stale = outbox.get(item.uid)

    with outbox.work():
        pass                                        # a worker finishes it

    stale.data = {"to": "b"}
    stale.save()

    stored = outbox.get(item.uid)
    assert (stored.status, stored.data, stored.attempts) == ("done", {"to": "b"}, 1)     # the edit lands, the outcome stays
    assert outbox.claim() is None                   # not worked twice


def test_a_stale_copy_of_a_task_cannot_run_it_again(tasks):
    task = tasks.schedule(tasks("greet", name="Ada"))
    stale = tasks.get(task.uid)
    tasks.work()

    stale.save()

    assert tasks.get(task.uid).status == "done"
    assert tasks.work() is None


def test_a_stale_copy_cannot_wipe_a_live_claim(outbox):
    item = outbox.add({"to": "a"})
    stale = outbox.get(item.uid)
    held = outbox.claim()

    stale.save()

    assert outbox.done(held) is True                # the holder's outcome still lands


def test_save_still_writes_your_fields(taskEngine):
    accounts = taskEngine({"greet": greet}, AccountTask, name="accounts")
    task = accounts.schedule(accounts("greet"), accountId=1)
    later = (utc_now() + timedelta(days=1)).replace(microsecond=0)

    task.accountId = 2
    task.deadline = later
    task.save()

    stored = accounts.get(task.uid)
    assert (stored.accountId, stored.deadline, stored.leaseUntil) == (2, later, later)    # a waiting task's lease follows


def test_a_document_saved_for_the_first_time_starts_with_fresh_kept_fields(tasks):
    task = tasks._newTask(tasks("greet"))
    task.status = "done"
    task.claimId = "made-up"
    task.result = "forged"
    tasks.save(task)

    stored = tasks.get(task.uid)
    assert (stored.status, stored.claimId, stored.result) == ("pending", None, None)
    assert stored.uid == task.uid                   # its key is its own
    assert tasks.work() is not None                 # and it runs


# --- update(), build() and create() refuse them ---

@pytest.mark.parametrize("field, message", [
    ("status", "cancel()"),
    ("claimId", "the engine keeps it"),
    ("result", "the engine keeps it"),
    ("uid", "the engine keeps it"),
])
def test_updating_a_task_refuses_what_the_engine_keeps(tasks, field, message):
    task = tasks.schedule(tasks("greet"))
    before = tasks.collection.find_one({"uid": task.uid})

    with pytest.raises(TypeError, match=message):
        tasks.update(task.uid, **{field: "x"})

    assert tasks.collection.find_one({"uid": task.uid}) == before


@pytest.mark.parametrize("field, message", [
    ("status", "done\\(\\), fail\\(\\), release\\(\\) or cancel\\(\\)"),
    ("attempts", "release\\(\\) gives it back"),
    ("leaseUntil", "the engine keeps it"),
])
def test_updating_an_item_refuses_what_the_engine_keeps(outbox, field, message):
    item = outbox.add({"to": "a"})

    with pytest.raises(TypeError, match=message):
        outbox.update(item.uid, **{field: 1})


def test_updating_your_own_fields_still_works(outbox, tasks):
    item = outbox.add({"to": "a"})
    task = tasks.schedule(tasks("greet"))
    later = (utc_now() + timedelta(days=1)).replace(microsecond=0)

    assert outbox.update(item.uid, data={"to": "b"}).data == {"to": "b"}
    assert tasks.update(task.uid, deadline=later).leaseUntil == later


def test_build_and_create_refuse_what_the_engine_keeps(tasks, outbox):
    with pytest.raises(TypeError, match="cancel\\(\\)"):
        tasks.create(work=tasks("greet"), deadline=utc_now(), factory=tasks.factory, status="done")

    with pytest.raises(TypeError, match="the engine keeps it"):
        outbox.build(data={"to": "a"}, claimId="x")

    assert tasks.count() == outbox.count() == 0


def test_a_plain_collection_keeps_nothing(db):
    from pymonque import CollectionEngine

    notes = CollectionEngine(db["notes"], Note, name="notes")
    note = notes.create(uid="n1", text="a")

    assert notes.update("n1", uid="n2", text="b").uid == "n2"      # renaming by key is still yours
    note = notes.get("n2")
    note.text = "c"
    note.save()

    assert notes.get("n2").text == "c"
