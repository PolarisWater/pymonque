"""Document identity, and the worker threads a process leaves behind."""

import threading
import time

import pytest

from pymonque import BaseApp, Document, collection, task, utc_now


class Account(Document):
    accountId:  int = 0
    name:       str = ""


class Store(BaseApp):
    accounts = collection(Account, key="accountId")

    @task
    @staticmethod
    def ping() -> str:
        return "pong"


@pytest.fixture
def store(db):
    return Store(db, enforceVersion=False)


# --- a document is a handle on one row, not a copy of it ---

def test_a_document_remembers_the_key_it_was_stored_under(store):
    account = store.accounts.create(accountId=1, name="ada")

    assert account.storedKey == 1


def test_a_document_built_by_hand_has_no_stored_key(store):
    assert Account(accountId=1).storedKey is None


def test_changing_the_key_renames_rather_than_copies(store):
    account = store.accounts.create(accountId=1, name="ada")

    account.accountId = 2
    account.save()

    assert store.accounts.get(1) is None
    assert store.accounts.get(2).name == "ada"
    assert store.accounts.count() == 1


def test_delete_uses_the_key_it_was_stored_under(store):
    account = store.accounts.create(accountId=1, name="ada")
    account.accountId = 99          # not saved

    assert account.delete() is True
    assert store.accounts.count() == 0


def test_reload_uses_the_key_it_was_stored_under(store):
    account = store.accounts.create(accountId=1, name="ada")
    store.accounts.update(1, name="grace")
    account.accountId = 99          # not saved

    assert account.reload().name == "grace"


def test_saving_again_after_a_rename_keeps_one_row(store):
    account = store.accounts.create(accountId=1, name="ada")
    account.accountId = 2
    account.save()
    account.name = "grace"
    account.save()

    assert store.accounts.count() == 1
    assert store.accounts.get(2).name == "grace"


def test_an_inserted_document_is_bound_to_its_key(store):
    account = store.accounts.insert(Account(accountId=7, name="hand built"))
    account.name = "changed"
    account.save()

    assert store.accounts.count() == 1
    assert store.accounts.get(7).name == "changed"


# --- what a process leaves running ---

def test_starting_twice_does_not_leave_two_heartbeats(db):
    app = Store(db, taskPoolInterval=0.02, heartbeatInterval=30, backlogInterval=30)

    try:
        app.startWorkers(taskWorkers=1, schedulerWorkers=0)
        first = dict(app._monitors)

        app.startWorkers(taskWorkers=1, schedulerWorkers=0)

        assert sorted(first) == ["backlog", "heartbeat"]
        assert app._monitors == first           # the same two threads, not four
    finally:
        app.stopWorkers(timeout=5)


def test_stopping_takes_the_monitor_threads_with_it(db):
    """They used to wait out a whole interval, so a stop left them running."""

    app = Store(db, taskPoolInterval=0.02, heartbeatInterval=30, backlogInterval=30)
    app.startWorkers(taskWorkers=1, schedulerWorkers=0)
    monitors = list(app._monitors.values())

    assert all(t.is_alive() for t in monitors)

    app.stopWorkers(timeout=5)

    for t in monitors:
        t.join(5)

    assert not any(t.is_alive() for t in monitors)


def test_a_stopped_process_does_not_block_the_next_version(db):
    class V2(BaseApp):
        @task
        @staticmethod
        def ping(loud: bool = False) -> str:
            return "pong"

    old = Store(db, heartbeatInterval=0.05, backlogWarnAfter=None)
    old.startWorkers(taskWorkers=1, schedulerWorkers=0)
    old.stopWorkers(timeout=5)

    new = V2(db, backlogWarnAfter=None)
    new.startWorkers(taskWorkers=1, schedulerWorkers=0)   # would raise if still registered
    new.stopWorkers(timeout=5)


def test_workers_can_be_started_again_after_a_stop(db):
    app = Store(db, taskPoolInterval=0.02, backlogWarnAfter=None)
    app.startWorkers(taskWorkers=1, schedulerWorkers=0)
    app.stopWorkers(timeout=5)

    assert app.task.workerCount == 0

    app.task.schedule(Store.ping())
    app.startWorkers(taskWorkers=1, schedulerWorkers=0)

    try:
        deadline = time.monotonic() + 5
        while app.task.count({"status": "success"}) < 1 and time.monotonic() < deadline:
            time.sleep(0.02)

        assert app.task.count({"status": "success"}) == 1
    finally:
        app.stopWorkers(timeout=5)
