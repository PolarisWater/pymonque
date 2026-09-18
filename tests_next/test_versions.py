"""One version at a time: what the app's one fingerprint sees and what it cannot, the worker registry
and its heartbeat, refusing a mismatched worker, and init() — housekeeping on every task engine, guarded
the same way."""

import time
from datetime import timedelta

import pytest
from pydantic import BaseModel

from pymonque_next import BaseApp, BaseDistributions, Document, Scheduler, Task, collection, pile, schedulers, task, tasks, utc_now
from pymonque_next.exceptions import VersionMismatch
from pymonque_next.tasks import WORKER_DIED

from tests_next.helpers import waitFor


class V1(BaseApp):
    @task
    @staticmethod
    def sync(accountId: int) -> str:
        return "v1"


class V1Body(BaseApp):
    """The same surface as V1: another class name, another body."""

    @task
    @staticmethod
    def sync(accountId: int) -> str:
        return "something else entirely"


class V2(BaseApp):
    @task
    @staticmethod
    def sync(accountId: int, full: bool = False) -> str:
        return "v2"


def variant(base, **attributes):
    """Shared behaviour lives on the class, so varying it for a test means a subclass."""

    return type(base.__name__, (base,), attributes)


@pytest.fixture
def quiet(stopAfter):
    """Build an app of a class, with its workers stopped after the test."""

    def build(cls, db, **kwargs):
        return stopAfter(cls(db, backlogWarnAfter=None, **kwargs))

    return build


# --- what the fingerprint sees ---

def test_it_is_stable(db):
    assert V1(db).fingerprint == V1(db).fingerprint


def test_two_apps_of_one_class_agree(db):
    assert V1(db).fingerprint == V1(db.client["elsewhere"]).fingerprint


def test_a_changed_body_alone_does_not_change_it(db):
    # the documented limit: signatures are visible, bodies are not
    assert V1(db).fingerprint == V1Body(db).fingerprint


def test_a_changed_signature_changes_it(db):
    assert V1(db).fingerprint != V2(db).fingerprint


def test_a_changed_default_argument_changes_it(db):
    class Default(BaseApp):
        @task
        @staticmethod
        def sync(accountId: int, full: bool = True) -> str: ...

    assert Default(db).fingerprint != V2(db).fingerprint


def test_an_added_task_changes_it(db):
    class Extra(V1):
        @task
        @staticmethod
        def added() -> None: ...

    assert V1(db).fingerprint != Extra(db).fingerprint


def test_a_declared_limit_changes_it(db):
    class Limited(BaseApp):
        @task(timeout=5)
        @staticmethod
        def sync(accountId: int) -> str: ...

    assert V1(db).fingerprint != Limited(db).fingerprint


def test_an_app_default_changes_it_through_what_it_resolves(db):
    assert V1(db).fingerprint != variant(V1, taskTimeout=5)(db).fingerprint
    assert V1(db).fingerprint != variant(V1, schedulerMissed="skip")(db).fingerprint


def test_a_default_every_task_overrides_does_not_matter(db):
    class Own(BaseApp):
        @task(timeout=5)
        @staticmethod
        def sync(accountId: int) -> str: ...

    assert Own(db).fingerprint == variant(Own, taskTimeout=60)(db).fingerprint


def test_a_default_that_prints_an_address_does_not_split_it(db):
    class One(BaseApp):
        @task
        @staticmethod
        def f(x: object = object()) -> None: ...

    class Two(BaseApp):
        @task
        @staticmethod
        def f(x: object = object()) -> None: ...

    assert One(db).fingerprint == Two(db).fingerprint


def test_a_distribution_registry_changes_it(db):
    class Custom(BaseDistributions):
        @staticmethod
        def fixed(seconds: float) -> timedelta:
            return timedelta(seconds=seconds)

    assert V1(db).fingerprint != variant(V1, distributions=Custom)(db).fingerprint


def test_a_piles_tries_change_it(db):
    class Piled(V1):
        outbox = pile()

    assert Piled(db).fingerprint != variant(Piled, pileMaxAttempts=3)(db).fingerprint
    assert Piled(db).fingerprint != variant(Piled, outbox=pile(maxAttempts=2))(db).fingerprint


@pytest.mark.parametrize("default", ["taskLeaseSeconds", "schedulerLeaseSeconds", "pileLeaseSeconds"])
def test_a_lease_length_changes_it(db, default):
    class Piled(V1):
        outbox = pile()

    assert Piled(db).fingerprint != variant(Piled, **{default: 60})(db).fingerprint


def test_a_declared_engine_changes_it(db):
    assert V1(db).fingerprint != variant(V1, heavy=tasks())(db).fingerprint
    assert V1(db).fingerprint != variant(V1, nightly=schedulers())(db).fingerprint


def test_which_engine_a_scheduler_engine_emits_into_changes_it(db):
    class Two(V1):
        heavy = tasks()
        nightly = schedulers()

    class Into(V1):
        heavy = tasks()
        nightly = schedulers(emitsInto=heavy)

    assert Two(db).fingerprint != Into(db).fingerprint


class Email(BaseModel):
    to: str


class LongEmail(BaseModel):
    to:         str
    subject:    str = ""


class Group(Document):
    groupId: int


class RichTask(Task):
    accountId: int = 0


class RichScheduler(Scheduler):
    accountId: int = 0


@pytest.mark.parametrize("before, after", [
    (pile(Email), pile(LongEmail)),
    (tasks(), tasks(RichTask)),
    (schedulers(), schedulers(RichScheduler)),
    (collection(Group), collection(Document)),
])
def test_a_declared_models_schema_changes_it(db, before, after):
    assert variant(V1, part=before)(db).fingerprint != variant(V1, part=after)(db).fingerprint


def test_a_collection_name_changes_it(db):
    assert variant(V1, outbox=pile())(db).fingerprint != variant(V1, outbox=pile(None, "outgoing"))(db).fingerprint


def test_a_collection_key_changes_it(db):
    class Keyed(Document):
        groupId:    int = 0
        code:       int = 0

    assert (
        variant(V1, groups=collection(Keyed, key="groupId"))(db).fingerprint
        != variant(V1, groups=collection(Keyed, key="code"))(db).fingerprint
    )


def test_a_model_with_no_json_schema_still_has_a_fingerprint(db):
    class Handle:
        pass

    class Odd(Document):
        model_config = {"arbitrary_types_allowed": True}

        handle: Handle | None = None

    assert variant(V1, odd=collection(Odd))(db).fingerprint


# --- the registry ---

def test_a_registered_worker_reports_itself(db, quiet):
    app = quiet(V1, db)
    app.startWorkers(taskWorkers=1)

    [worker] = app.liveWorkers()

    assert worker["fingerprint"] == app.fingerprint
    assert worker["uid"] == app.workerUid
    assert worker["pid"] and worker["host"]
    assert worker["taskWorkers"] == {"task": 1}
    assert worker["schedulerWorkers"] == {"scheduler": 0}
    assert worker["abandonedThreads"] == 0


def test_the_heartbeat_keeps_a_worker_live(db, quiet):
    app = quiet(V1, db, heartbeatInterval=0.05, workerStaleAfter=0.4)
    app.startWorkers()
    first = db["pymonque_workers"].find_one({"uid": app.workerUid})["lastSeen"]

    assert waitFor(lambda: db["pymonque_workers"].find_one({"uid": app.workerUid})["lastSeen"] > first, timeout=2)

    time.sleep(0.5)     # longer than workerStaleAfter, but it keeps checking in
    assert len(app.liveWorkers()) == 1


def test_a_matching_worker_may_join(db, quiet):
    quiet(V1, db).startWorkers(taskWorkers=1)
    quiet(V1Body, db).startWorkers(taskWorkers=1)

    assert len(V1(db).liveWorkers()) == 2


def test_a_mismatched_worker_is_refused_naming_both_versions(db, quiet):
    running = quiet(V1, db)
    running.startWorkers(taskWorkers=1)
    other = quiet(V2, db)

    with pytest.raises(VersionMismatch, match="Only one version may run at a time") as refused:
        other.startWorkers(taskWorkers=1)

    assert running.fingerprint in str(refused.value)
    assert other.fingerprint in str(refused.value)
    assert not other.running


def test_constructing_an_app_is_never_refused(db, quiet):
    quiet(V1, db).startWorkers(taskWorkers=1)

    app = V2(db)        # enqueuing and querying stay open to anyone
    app.task.schedule(V2.sync(accountId=1))

    assert app.task.count() == 1


def test_a_stale_worker_does_not_block_a_deploy(db, quiet):
    quiet(V1, db).startWorkers(taskWorkers=1)
    # the old process is gone: age its check-in past the staleness window
    db["pymonque_workers"].update_many({}, {"$set": {"lastSeen": utc_now() - timedelta(hours=1)}})

    quiet(V2, db).startWorkers(taskWorkers=1)

    assert len(V2(db).liveWorkers()) == 1


def test_enforcement_can_be_turned_off(db, quiet):
    quiet(V1, db).startWorkers(taskWorkers=1)

    quiet(V2, db, enforceVersion=False).startWorkers(taskWorkers=1)     # no raise


def test_a_limit_mismatch_cannot_run_workers_alongside(db, quiet):
    quiet(variant(V1, pileMaxAttempts=1, outbox=pile()), db).startWorkers(taskWorkers=1)

    with pytest.raises(VersionMismatch):
        quiet(variant(V1, pileMaxAttempts=3, outbox=pile()), db).startWorkers(taskWorkers=1)


# --- init(): housekeeping on every task engine, guarded ---

class Full(BaseApp):
    heavy = tasks()

    @task
    @staticmethod
    def sync(accountId: int) -> None: ...

    @task
    @staticmethod
    def gone() -> None: ...


class Smaller(BaseApp):
    heavy = tasks()

    @task
    @staticmethod
    def sync(accountId: int) -> None: ...


def leftRunningByADeadWorker(engine, task):
    engine.collection.update_one({"uid": task.uid}, {"$set": {
        "status": "running", "claimId": "dead", "leaseUntil": utc_now() - timedelta(seconds=1),
    }})


def test_init_flags_waiting_tasks_whose_function_is_gone_on_every_task_engine(db):
    full = Full(db)
    waiting = [full.task.schedule(Full.gone()), full.heavy.schedule(Full.gone())]
    finished = full.task.schedule(Full.gone())
    full.task.collection.update_one({"uid": finished.uid}, {"$set": {"status": "done"}})

    Smaller(db).init()

    assert [engine.get(t.uid).status for engine, t in zip((full.task, full.heavy), waiting)] == ["incompatible", "incompatible"]
    assert full.task.get(finished.uid).status == "done"


def test_init_writes_off_tasks_a_dead_worker_left_running_whose_function_is_gone(db):
    full = Full(db)
    stuck = full.heavy.schedule(Full.gone())
    leftRunningByADeadWorker(full.heavy, stuck)

    Smaller(db).init()

    written = full.heavy.get(stuck.uid)
    assert (written.status, written.error) == ("failed", WORKER_DIED)


def test_init_cleans_an_engine_no_process_works(db, quiet):
    full = Full(db)
    waiting = full.heavy.schedule(Full.gone())

    quiet(Smaller, db).startWorkers(taskWorkers={"task": 1})     # none on heavy

    assert full.heavy.get(waiting.uid).status == "incompatible"


def test_init_leaves_the_tasks_it_can_run_alone(db):
    app = Full(db)
    waiting = app.task.schedule(Full.sync(accountId=1))

    app.init()

    assert app.task.get(waiting.uid).status == "pending"


def test_constructing_an_app_does_no_housekeeping(db):
    full = Full(db)
    waiting = full.task.schedule(Full.gone())

    Smaller(db)

    assert full.task.get(waiting.uid).status == "pending"


def test_init_is_refused_beside_a_different_live_version(db, quiet):
    quiet(Full, db).startWorkers()

    with pytest.raises(VersionMismatch):
        Smaller(db).init()


def test_a_refused_process_cannot_flag_another_versions_tasks(db, quiet):
    full = quiet(Full, db)
    full.startWorkers()
    waiting = full.heavy.schedule(Full.gone())

    with pytest.raises(VersionMismatch):
        Smaller(db).init()

    assert full.heavy.get(waiting.uid).status == "pending"


def test_init_is_allowed_alone_or_beside_the_same_version(db, quiet):
    Full(db).init()

    quiet(Full, db).startWorkers()
    Full(db).init()


def test_enforcement_off_skips_the_check_for_init_too(db, quiet):
    quiet(Full, db).startWorkers()

    Smaller(db, enforceVersion=False).init()
