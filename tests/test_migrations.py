"""Upgrading a 2.0 database: the default engines' collections renamed into place, 2.0's statuses mapped
to this version's, tasks' retry counts dropped, refused beside a live worker or onto a queue in use,
and safe to run twice."""

from datetime import timedelta

import pytest
from pydantic import BaseModel

from pymonque import BaseApp, pile, schedulers, task, tasks, upgradeFrom2, utc_now
from pymonque.exceptions import VersionMismatch
from pymonque.schedulers import schedulerUid
from pymonque.tasks import WORKER_DIED


class Email(BaseModel):
    to: str


class Mail(BaseApp):
    outbox = pile(Email)
    heavy = tasks()

    @task
    @staticmethod
    def greet(name: str) -> str:
        return f"Hello, {name}!"


def oldTask(status="pending", **fields):
    """A task as 2.0 stored it."""

    now = utc_now()

    return {
        "uid": f"task-{status}-{fields.get('uid', '')}{id(fields)}",
        "status": status,
        "work": {"functionName": "greet", "kwargs": {"name": "Ada"}},
        "deadline": now,
        "factory": {"uid": "f", "name": "default"},
        "createdAt": now,
        "leaseUntil": now - timedelta(seconds=1),
        "attempts": 1 if status != "pending" else 0,
        **fields,
    }


def oldScheduler():
    now = utc_now()

    return {
        "uid": schedulerUid("nightly"), "name": "nightly", "status": "enabled",
        "work": {"functionName": "greet", "kwargs": {"name": "Ada"}},
        "distribution": {"functionName": "constant", "kwargs": {"dailyFrequency": 1}},
        "deadline": now + timedelta(hours=1), "leaseUntil": now + timedelta(hours=1),
    }


@pytest.fixture
def old(db):
    """A database a 2.0 app left behind: its tasks and schedulers under their old names, items claimed."""

    db["pymonque_tasks"].insert_many([oldTask("success"), oldTask("processing"), oldTask("pending"), oldTask("failed")])
    db["pymonque_tasks"].create_index("uid", unique=True)
    db["pymonque_schedulers"].insert_one(oldScheduler())
    db["pymonque_pile_outbox"].insert_one({
        "uid": "item", "status": "claimed", "data": {"to": "a@b.c"}, "createdAt": utc_now(),
        "leaseUntil": utc_now() - timedelta(seconds=1), "attempts": 1, "claimId": "old",
    })

    return db


# --- what it changes ---

def test_the_old_collections_become_the_default_engines(old):
    app = Mail(old)

    changed = upgradeFrom2(app)

    assert (changed["tasks renamed"], changed["schedulers renamed"]) == (4, 1)
    assert "pymonque_tasks" not in old.list_collection_names()
    assert "pymonque_schedulers" not in old.list_collection_names()
    assert app.task.count() == 4
    assert app.scheduler.byName("nightly") is not None


def test_task_statuses_become_this_versions(old):
    app = Mail(old)

    upgradeFrom2(app)

    assert sorted(t.status for t in app.task.find()) == ["done", "failed", "pending", "running"]


def test_tasks_lose_their_retry_count(old):
    app = Mail(old)

    changed = upgradeFrom2(app)

    assert changed["task attempts"] == 4
    assert app.task.collection.count_documents({"attempts": {"$exists": True}}) == 0


def test_a_claimed_item_becomes_running_and_the_next_claim_deals_with_it_by_its_tries(old):
    app = Mail(old)

    upgradeFrom2(app)

    assert app.outbox.get("item").status == "running"
    assert app.outbox.claim() is None          # its holder was stopped, and its one try is spent
    assert app.outbox.get("item").status == "failed"


def test_a_claimed_item_with_tries_left_is_taken_over(old):
    class Tried(Mail):
        outbox = pile(Email, maxAttempts=2)

    app = Tried(old)
    upgradeFrom2(app)

    assert app.outbox.claim().uid == "item"


def test_a_task_left_processing_is_written_off_by_the_next_claim(old):
    app = Mail(old)
    upgradeFrom2(app)

    ended = [app.task.work(), app.task.work()]

    written = next(t for t in ended if t.error == WORKER_DIED)
    assert written.status == "failed"
    assert app.task.count({"status": "done"}) == 2      # 2.0's success, and the pending one, run now


def test_upgraded_tasks_and_schedulers_run(old):
    app = Mail(old)
    upgradeFrom2(app)
    app.scheduler.update(schedulerUid("nightly"), deadline=utc_now())

    assert app.scheduler.work() is not None
    assert app.task.count({"factory.name": "nightly"}) == 1


def test_the_engines_indexes_are_made_on_the_renamed_collections(old):
    app = Mail(old)

    upgradeFrom2(app)

    assert any(index["key"] == [("leaseUntil", 1)] for index in app.scheduler.collection.index_information().values())


def test_a_declared_scheduler_engines_2_0_collection_is_renamed(db):
    class Planned(Mail):
        nightly = schedulers()
        kept = schedulers(collection="kept_schedulers")

    db["pymonque_schedulers_nightly"].insert_one(oldScheduler())
    db["pymonque_schedulers_kept"].insert_one(oldScheduler())
    app = Planned(db)

    assert upgradeFrom2(app)["schedulers renamed"] == 1
    assert app.nightly.byName("nightly") is not None
    assert "pymonque_schedulers_kept" in db.list_collection_names()     # it was told to use another


def test_every_task_engine_is_upgraded(db):
    db["pymonque_task_heavy"].insert_one(oldTask("success"))

    upgradeFrom2(Mail(db))

    assert Mail(db).heavy.find()[0].status == "done"


def test_collections_2_0_was_told_to_use_can_be_named(db):
    db["my_tasks"].insert_one(oldTask("success"))

    changed = upgradeFrom2(Mail(db), tasks="my_tasks")

    assert changed["tasks renamed"] == 1
    assert Mail(db).task.find()[0].status == "done"


# --- when it refuses, and running it twice ---

def test_it_is_refused_while_a_worker_is_live(old, stopAfter):
    running = stopAfter(Mail(old, backlogWarnAfter=None))
    running.startWorkers()

    with pytest.raises(VersionMismatch, match="stop every worker process before upgrading"):
        upgradeFrom2(Mail(old))

    assert "pymonque_tasks" in old.list_collection_names()


def test_a_2_0_worker_still_registered_refuses_it(old):
    old["pymonque_workers"].insert_one({"uid": "w", "fingerprint": "abc", "host": "h", "pid": 1, "lastSeen": utc_now(), "taskWorkers": 2})
    app = Mail(old)

    assert app.taskWorkers() == {"task": 0, "heavy": 0}    # its one number names no engine of this version

    with pytest.raises(VersionMismatch):
        upgradeFrom2(app)


def test_it_will_not_rename_onto_a_queue_already_in_use(old):
    app = Mail(old)
    app.task.schedule(Mail.greet(name="new"))

    with pytest.raises(ValueError, match="already holds 1 document"):
        upgradeFrom2(app)

    assert old["pymonque_tasks"].count_documents({}) == 4


def test_running_it_twice_changes_nothing_more(old):
    app = Mail(old)
    upgradeFrom2(app)

    assert set(upgradeFrom2(app).values()) == {0}
    assert app.task.count() == 4


def test_a_database_2_0_never_used_is_left_alone(db):
    assert set(upgradeFrom2(Mail(db)).values()) == {0}
