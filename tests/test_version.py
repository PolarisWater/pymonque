"""One version of the code at a time, and the worker loop's pacing."""

import time

import pytest

from pymonque import BaseApp, task, utc_now
from pymonque.exceptions import VersionMismatch

from conftest import ExampleApp, WORKER_POOL_INTERVAL, appWith, wait_for


class V1(BaseApp):
    @task
    @staticmethod
    def sync(accountId: int) -> str:
        return "v1"


class V1Renamed(BaseApp):
    """Same surface as V1, different class name and body."""

    @task
    @staticmethod
    def sync(accountId: int) -> str:
        return "something else entirely"


class V2Signature(BaseApp):
    @task
    @staticmethod
    def sync(accountId: int, full: bool = False) -> str:
        return "v2"


class V2Extra(BaseApp):
    @task
    @staticmethod
    def sync(accountId: int) -> str:
        return "v1"

    @task
    @staticmethod
    def added() -> str:
        return "new"


# --- what the fingerprint sees ---

def test_it_is_stable(db):
    assert V1(db).fingerprint == V1(db).fingerprint


def test_a_changed_signature_changes_it(db):
    assert V1(db).fingerprint != V2Signature(db).fingerprint


def test_an_added_task_changes_it(db):
    assert V1(db).fingerprint != V2Extra(db).fingerprint


def test_a_custom_distribution_registry_changes_it(db):
    from datetime import timedelta
    from pymonque import BaseDistributions

    class Custom(BaseDistributions):
        @staticmethod
        def fixed(dailyFrequency: float) -> timedelta:
            return timedelta(seconds=1)

    assert V1(db).fingerprint != V1(db.client["other"], distributionsRegistry=Custom).fingerprint


def test_a_changed_body_alone_does_not_change_it(db):
    # the documented limit: signatures are visible, bodies are not
    assert V1(db).fingerprint == V1Renamed(db).fingerprint


# --- the guard ---

def test_a_matching_worker_may_join(db):
    V1(db).startWorkers(taskWorkers=1)
    V1(db).startWorkers(taskWorkers=1)

    assert len(V1(db).liveWorkers()) == 2


def test_a_mismatched_worker_is_refused(db):
    V1(db).startWorkers(taskWorkers=1)

    with pytest.raises(VersionMismatch) as e:
        V2Signature(db).startWorkers(taskWorkers=1)

    assert "one version" in str(e.value)


def test_the_error_names_both_versions(db):
    first = V1(db)
    first.startWorkers(taskWorkers=1)
    second = V2Signature(db)

    with pytest.raises(VersionMismatch) as e:
        second.startWorkers(taskWorkers=1)

    assert first.fingerprint in str(e.value)
    assert second.fingerprint in str(e.value)


def test_constructing_a_app_is_never_refused(db):
    V1(db).startWorkers(taskWorkers=1)

    V2Signature(db)          # enqueuing and querying stay open to anyone
    assert V2Signature(db).task.tasksCollection.count_documents({}) == 0


def test_a_stale_worker_does_not_block_a_deploy(db):
    old = V1(db)
    old.startWorkers(taskWorkers=1)
    # the old process is gone: age its registration past the staleness window
    db["pymonque_workers"].update_many(
        {}, [{"$set": {"lastSeen": {"$subtract": ["$lastSeen", 1000 * 3600]}}}]
    )

    V2Signature(db).startWorkers(taskWorkers=1)   # must not raise

    assert len(V2Signature(db).liveWorkers()) == 1


def test_enforcement_can_be_turned_off(db):
    V1(db).startWorkers(taskWorkers=1)

    V2Signature(db, enforceVersion=False).startWorkers(taskWorkers=1)  # no raise


def test_a_registered_worker_reports_itself(db):
    app = V1(db)
    app.startWorkers(taskWorkers=1)
    worker = app.liveWorkers()[0]

    assert worker["fingerprint"] == app.fingerprint
    assert worker["uid"] == app.workerUid
    assert worker["pid"] and worker["host"]


# --- the worker loop no longer sleeps between tasks ---

def test_a_backlog_drains_without_waiting_per_task(db):
    app = ExampleApp(db, taskPoolInterval=5)   # a sleep this long would be obvious
    for n in range(20):
        app.task.schedule(app.task("greet", name=str(n)))

    started = time.monotonic()
    app.startWorkers(taskWorkers=1)
    drained = wait_for(
        lambda: app.task.tasksCollection.count_documents({"status": "success"}) == 20,
        timeout=4,
    )

    assert drained, "20 tasks did not drain — the loop is still sleeping between them"
    assert time.monotonic() - started < 4


def test_an_idle_worker_still_waits(db):
    app = ExampleApp(db, taskPoolInterval=WORKER_POOL_INTERVAL)
    app.startWorkers(taskWorkers=1)
    time.sleep(WORKER_POOL_INTERVAL * 2)

    app.task.schedule(app.task("greet", name="late"))

    assert wait_for(lambda: app.task.tasksCollection.count_documents({"status": "success"}) == 1)


def test_work_reports_whether_it_did_anything(app):
    assert app.task._work() is None
    assert app.scheduler._work() is None

    app.task.schedule(ExampleApp.greet(name="Ada"))
    assert app.task._work() is not None

    app.scheduler.add(ExampleApp.greet(name="Ada"), app.distribution("constant", dailyFrequency=24))
    now = utc_now()
    app.schedulersCollection.update_many({}, {"$set": {"deadline": now, "leaseUntil": now}})
    assert app.scheduler._work() is not None


def test_the_heartbeat_keeps_a_worker_live(db):
    app = V1(db, heartbeatInterval=0.05, workerStaleAfter=0.4)
    app.startWorkers(taskWorkers=0)
    first = db["pymonque_workers"].find_one({"uid": app.workerUid})["lastSeen"]

    assert wait_for(
        lambda: db["pymonque_workers"].find_one({"uid": app.workerUid})["lastSeen"] > first,
        timeout=2,
    ), "lastSeen never advanced — the worker would go stale and stop blocking a mismatch"

    time.sleep(0.5)   # longer than workerStaleAfter, but it keeps checking in
    assert len(app.liveWorkers()) == 1


# --- policies are shared behaviour, so they are part of the fingerprint ---

def test_two_apps_of_one_class_always_agree_on_policy(db):
    class App(ExampleApp):
        staleItemsPolicy = "fail"

    a, b = App(db), App(db)

    assert a.outbox.policy == b.outbox.policy == "fail"
    assert a.fingerprint == b.fingerprint


def test_a_policy_change_is_a_different_fingerprint(db):
    class Retrying(ExampleApp):
        staleItemsPolicy = "retry"

    class Failing(ExampleApp):
        staleItemsPolicy = "fail"

    assert Retrying(db).fingerprint != Failing(db).fingerprint


@pytest.mark.parametrize("policy", ["overdueTaskPolicy", "overdueSchedulersPolicy"])
def test_every_policy_reaches_the_fingerprint(db, policy):
    changed = {"overdueTaskPolicy": "skip", "overdueSchedulersPolicy": "skip"}[policy]

    assert ExampleApp(db).fingerprint != appWith(ExampleApp, db, **{policy: changed}).fingerprint


def test_a_policy_mismatch_cannot_run_workers_alongside(db):
    class Retrying(ExampleApp):
        staleItemsPolicy = "retry"

    class Failing(ExampleApp):
        staleItemsPolicy = "fail"

    running = Retrying(db)
    running.startWorkers(taskWorkers=1, schedulerWorkers=0)

    try:
        with pytest.raises(VersionMismatch):
            Failing(db).startWorkers(taskWorkers=1, schedulerWorkers=0)
    finally:
        running.stopWorkers()


def test_a_policy_is_not_a_constructor_argument(db):
    with pytest.raises(TypeError):
        ExampleApp(db, staleItemsPolicy="fail")


# --- housekeeping is guarded the same as starting workers ---

def test_init_is_refused_when_a_different_version_is_live(db):
    """init() decides what is runnable from this process's task list, so a process
    holding a different one must not run it."""

    full = V1(db)
    full.startWorkers(taskWorkers=1, schedulerWorkers=0)

    try:
        with pytest.raises(VersionMismatch):
            V2Signature(db).init()
    finally:
        full.stopWorkers()


def test_a_refused_process_cannot_disable_another_versions_scheduler(db):
    full = ExampleApp(db)
    full.startWorkers(taskWorkers=1, schedulerWorkers=0)

    try:
        stored = full.scheduler.ensure(
            "nightly", ExampleApp.greet(name="Ada"),
            full.distribution("constant", dailyFrequency=1),
        )

        class Smaller(BaseApp):
            @task
            @staticmethod
            def other() -> None: return None

        with pytest.raises(VersionMismatch):
            Smaller(db).init()

        assert full.scheduler.byUid(stored.uid).status == "enabled"
    finally:
        full.stopWorkers()


def test_init_is_allowed_when_nothing_else_is_running(db):
    ExampleApp(db).init()      # the ordinary case: no live worker to disagree with


def test_init_is_allowed_alongside_the_same_version(db):
    running = ExampleApp(db)
    running.startWorkers(taskWorkers=1, schedulerWorkers=0)

    try:
        ExampleApp(db).init()
    finally:
        running.stopWorkers()


def test_enforcement_off_skips_the_check_for_init_too(db):
    running = ExampleApp(db)
    running.startWorkers(taskWorkers=1, schedulerWorkers=0)

    try:
        V2Signature(db, enforceVersion=False).init()      # opted out, so unguarded
    finally:
        running.stopWorkers()
