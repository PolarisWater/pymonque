"""One clock for every process — leases, deadlines and heartbeats written on one host are compared on
another, so every process keeps time by the database server's clock, not its own; and a lease short
enough to be lost without anyone dying is logged."""

import logging
import os
from datetime import datetime, timedelta

import pytest

from pymonque import BaseApp, syncClock, task, utc_now
from pymonque import documents


class ServerAhead:
    """A database whose server clock runs `ahead` of this host's."""

    def __init__(self, ahead: timedelta, db=None):
        self.ahead = ahead
        self.db = db
        self.calls = 0

    def command(self, name, *args, **kwargs):
        if name != "hello":
            return self.db.command(name, *args, **kwargs)

        self.calls += 1

        return {"isWritablePrimary": True, "localTime": documents._localNow() + self.ahead}

    def __getattr__(self, name):
        return getattr(self.db, name)

    def __getitem__(self, name):
        return self.db[name]


def test_utc_now_follows_the_server_once_synced():
    offset = syncClock(ServerAhead(timedelta(minutes=10)))

    assert abs(offset - timedelta(minutes=10)) < timedelta(seconds=1)
    assert abs(utc_now() - (documents._localNow() + timedelta(minutes=10))) < timedelta(seconds=1)


def test_a_server_behind_this_host_is_followed_too():
    syncClock(ServerAhead(timedelta(minutes=-3)))

    assert abs(utc_now() - (documents._localNow() - timedelta(minutes=3))) < timedelta(seconds=1)


def test_an_aware_server_time_is_read_as_utc():
    class Aware(ServerAhead):
        def command(self, name, *args, **kwargs):
            reply = super().command(name)
            reply["localTime"] = reply["localTime"].replace(tzinfo=documents.timezone.utc)
            return reply

    assert abs(syncClock(Aware(timedelta(seconds=30))) - timedelta(seconds=30)) < timedelta(seconds=1)


def test_a_server_that_will_not_say_leaves_the_clock_as_it_was():
    syncClock(ServerAhead(timedelta(minutes=5)))
    before = documents._serverOffset

    class Unanswering(ServerAhead):
        def command(self, name, *args, **kwargs):
            raise NotImplementedError(name)     # as mongomock does

    assert syncClock(Unanswering(timedelta(0))) is None
    assert documents._serverOffset == before

    class Silent(ServerAhead):
        def command(self, name, *args, **kwargs):
            return {"isWritablePrimary": True}

    assert syncClock(Silent(timedelta(0))) is None
    assert documents._serverOffset == before


def test_a_clock_far_off_the_server_is_logged_once(caplog):
    server = ServerAhead(timedelta(seconds=42))

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        syncClock(server)
        syncClock(server)

    warnings = [r.getMessage() for r in caplog.records if "clock" in r.getMessage()]
    assert len(warnings) == 1 and "42.0s behind" in warnings[0]


def test_a_clock_close_to_the_server_is_not_logged(caplog):
    with caplog.at_level(logging.WARNING, logger="pymonque"):
        syncClock(ServerAhead(timedelta(milliseconds=200)))

    assert not [r for r in caplog.records if "clock" in r.getMessage()]


# --- the app keeps the server's time ---

class App(BaseApp):
    @task
    @staticmethod
    def noop() -> None: ...


def test_building_an_app_syncs_the_clock(db):
    server = ServerAhead(timedelta(minutes=7), db)
    App(server, enforceVersion=False)

    assert server.calls == 1
    assert abs(documents._serverOffset - timedelta(minutes=7)) < timedelta(seconds=1)


def test_a_host_whose_clock_runs_ahead_does_not_take_over_a_live_lease(db):
    """The case the server clock exists for: host A holds a task under a live lease; host B's clock is
    ten minutes ahead. By its own clock A's lease has long lapsed; by the server's it has not."""

    app = App(db, enforceVersion=False)
    held = app.task.schedule(App.noop())
    app.task.collection.update_one(
        {"uid": held.uid},
        {"$set": {"status": "running", "claimId": "host-a", "leaseUntil": utc_now() + timedelta(minutes=5)}},
    )

    documents._serverOffset = timedelta(minutes=10)     # host B, unsynced: its own clock, ten minutes ahead
    assert app.task.work() is not None                  # takes A's live task over, writing it off

    app.task.collection.update_one(
        {"uid": held.uid},
        {"$set": {"status": "running", "claimId": "host-a", "leaseUntil": documents._localNow() + timedelta(minutes=5)}},
    )

    syncClock(ServerAhead(timedelta(0)))                # host B, synced: the server's clock
    assert app.task.work() is None                      # A's lease is live, and left alone


def test_a_running_worker_keeps_following_the_server(db):
    from tests.helpers import waitFor

    server = ServerAhead(timedelta(minutes=1), db)
    app = App(server, enforceVersion=False, heartbeatInterval=0.02, backlogWarnAfter=None)
    app.startWorkers()

    try:
        server.ahead = timedelta(minutes=2)     # the clocks drift apart while it runs
        assert waitFor(lambda: abs(documents._serverOffset - timedelta(minutes=2)) < timedelta(seconds=1), timeout=2)
    finally:
        app.stopWorkers(1)


# --- short leases are said to be fragile ---

def test_a_short_lease_is_logged_where_the_app_is_built(db, caplog):
    from pymonque import pile, schedulers, tasks

    class Short(BaseApp):
        fast = tasks(leaseSeconds=3)
        beats = schedulers(leaseSeconds=5)
        outbox = pile(leaseSeconds=60)

    with caplog.at_level(logging.WARNING, logger="pymonque"):
        Short(db, enforceVersion=False)

    warned = [r.getMessage() for r in caplog.records if "lease of" in r.getMessage()]
    assert [m.split(" ")[0] for m in warned] == ["fast", "beats"]
    assert "about 2.0s" in warned[0]


def test_the_default_lease_is_not_logged(db, caplog):
    with caplog.at_level(logging.WARNING, logger="pymonque"):
        App(db, enforceVersion=False)

    assert not [r for r in caplog.records if "lease of" in r.getMessage()]


@pytest.mark.skipif(not os.environ.get("PYMONQUE_MONGO_URL"), reason="needs a real MongoDB: scripts/test-mongo.sh")
def test_a_real_server_reports_its_clock(db):
    offset = syncClock(db)

    # the server runs on this machine, so the two clocks agree to well within a second
    assert offset is not None and abs(offset) < timedelta(seconds=1)
