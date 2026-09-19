"""BaseApp, built: each declared part as an engine under its name, the default engines and replacing
them, following an emitsInto reference by name, the distribution registry as a class attribute, the
defaults and poll intervals that reach the engines, names an app reserves, and what constructing an
app may write."""

from datetime import timedelta

import pytest
from pydantic import BaseModel, ValidationError
from pymongo import IndexModel

from pymonque import (
    BaseApp, BaseDistributions, CollectionEngine, Document, DistributionEngine, PileEngine, Scheduler,
    SchedulerEngine, Task, TaskEngine, collection, pile, schedulers, task, tasks, utc_now,
)


class Email(BaseModel):
    to: str


class Group(Document):
    groupId:    int
    name:       str = ""


class AccountTask(Task):
    accountId: int = 0


class Shop(BaseApp):
    groups  = collection(Group, key="groupId")
    outbox  = pile(Email)
    heavy   = tasks(AccountTask, leaseSeconds=900)
    nightly = schedulers(emitsInto=heavy, missed="skip")

    @task
    @staticmethod
    def greet(name: str) -> str:
        return f"Hello, {name}!"

    @task
    def whoami(self) -> str:
        return type(self).__name__

    @task
    @classmethod
    def kind(cls) -> str:
        return cls.__name__

    @task
    def nameOf(self, groupId: int) -> str:
        return self.groups.get(groupId).name


@pytest.fixture
def shop(db):
    return Shop(db)


# --- the parts, built ---

def test_each_declared_part_is_built_as_its_engine_under_its_name(shop):
    assert isinstance(shop.groups, CollectionEngine)
    assert isinstance(shop.outbox, PileEngine)
    assert isinstance(shop.heavy, TaskEngine)
    assert isinstance(shop.nightly, SchedulerEngine)


def test_the_default_engines_are_built_beside_the_declared_ones(shop):
    assert set(shop.taskEngines) == {"task", "heavy"}
    assert set(shop.schedulerEngines) == {"scheduler", "nightly"}
    assert shop.task is shop.taskEngines["task"]
    assert shop.scheduler is shop.schedulerEngines["scheduler"]


def test_every_part_is_listed_on_the_app(shop):
    assert shop.collections == {"groups": shop.groups}
    assert shop.piles == {"outbox": shop.outbox}


def test_class_access_gives_the_declaration():
    assert isinstance(Shop.outbox, pile)
    assert isinstance(Shop.heavy, tasks)
    assert isinstance(Shop.task, tasks)
    assert isinstance(Shop.scheduler, schedulers)


def test_the_default_collection_names(shop):
    assert shop.task.collection.name == "pymonque_task"
    assert shop.scheduler.collection.name == "pymonque_scheduler"
    assert shop.heavy.collection.name == "pymonque_task_heavy"
    assert shop.nightly.collection.name == "pymonque_scheduler_nightly"
    assert shop.outbox.collection.name == "pymonque_pile_outbox"
    assert shop.groups.collection.name == "groups"


def test_the_parts_share_the_apps_database(shop, db):
    assert shop.db is db
    assert all(engine.collection.database is db for engine in [*shop.engines, shop.outbox, shop.groups])


def test_a_declared_model_reaches_its_engine(shop):
    assert shop.heavy.model is AccountTask
    assert shop.task.model is Task
    assert shop.scheduler.model is Scheduler
    assert shop.groups.key == "groupId"


def test_an_app_with_nothing_declared_still_builds(db):
    class Empty(BaseApp):
        pass

    app = Empty(db)

    assert list(app.functions) == ["cleanupFinished"]      # the one task every app has
    assert app.piles == {} and app.collections == {}
    assert set(app.taskEngines) == {"task"} and set(app.schedulerEngines) == {"scheduler"}


# --- tasks ---

def test_any_task_engine_runs_any_of_the_apps_tasks(shop):
    shop.task.schedule(Shop.greet(name="Ada"))
    shop.heavy.schedule(Shop.greet(name="Bob"))

    assert shop.task.work().result == "Hello, Ada!"
    assert shop.heavy.work().result == "Hello, Bob!"


def test_an_instance_task_runs_with_its_app(shop):
    shop.task.schedule(Shop.whoami())

    assert shop.task.work().result == "Shop"


def test_a_classmethod_can_be_a_task(shop):
    shop.task.schedule(Shop.kind())

    assert shop.task.work().result == "Shop"


def test_a_task_can_reach_a_collection(shop):
    shop.groups.create(groupId=7, name="makers")
    shop.task.schedule(Shop.nameOf(groupId=7))

    assert shop.task.work().result == "makers"


def test_a_subclass_redefining_a_task_as_a_plain_method_unregisters_it(db):
    class Smaller(Shop):
        def greet(self, name: str) -> str:
            return "not a task"

    assert "greet" not in Smaller(db).functions


def test_every_task_runs_under_its_resolved_limits(db):
    class Limited(BaseApp):
        taskTimeout = 30

        @task
        @staticmethod
        def bare() -> None: ...

        @task(timeout=None, skipAfter=5)
        @staticmethod
        def own() -> None: ...

    app = Limited(db)

    assert app.task.limits["bare"].timeout == 30
    assert (app.task.limits["own"].timeout, app.task.limits["own"].skipAfter) == (None, 5)
    assert app.task.limits == app.limits


# --- the default engines, replaced ---

def test_a_declaration_named_task_replaces_the_default_task_engine(db):
    class Accounts(BaseApp):
        @task
        @staticmethod
        def sync(accountId: int) -> None: ...

        task = tasks(AccountTask)

    app = Accounts(db)

    assert set(app.taskEngines) == {"task"}
    assert app.task.model is AccountTask
    assert app.task.collection.name == "pymonque_task"
    assert "sync" in app.functions


def test_a_declaration_named_scheduler_replaces_the_default_scheduler_engine(db):
    class Replaced(BaseApp):
        scheduler = schedulers(missed="replay")

    app = Replaced(db)

    assert set(app.schedulerEngines) == {"scheduler"}
    assert app.scheduler.missed == "replay"
    assert app.scheduler.collection.name == "pymonque_scheduler"


def test_a_subclass_can_override_an_engine_or_a_pile(db):
    class Bigger(Shop):
        outbox = pile(Email, "outgoing")
        heavy = tasks(AccountTask, leaseSeconds=60)

    app = Bigger(db)

    assert app.outbox.collection.name == "outgoing"
    assert app.heavy.settings.leaseSeconds == 60


def test_a_declaration_used_as_a_decorator_says_what_it_is():
    with pytest.raises(TypeError, match="is a declaration, not a decorator"):
        class Wrong(BaseApp):
            @pile()
            def outbox(self): ...


# --- which task engine a scheduler engine emits into ---

def test_a_scheduler_engine_emits_into_the_default_task_engine_unless_told(shop):
    assert shop.scheduler.tasks is shop.task


def test_a_scheduler_engine_emits_into_the_engine_its_reference_names(shop):
    assert shop.nightly.tasks is shop.heavy


def test_an_emitted_task_lands_in_the_engine_its_scheduler_emits_into(shop):
    scheduler = shop.nightly.add(Shop.greet(name="Ada"), shop.distribution("constant", dailyFrequency=1))
    shop.nightly.update(scheduler.uid, deadline=utc_now() - timedelta(seconds=1))

    shop.nightly.work()

    assert shop.heavy.count() == 1
    assert shop.task.count() == 0


def test_an_inherited_reference_follows_the_name_to_a_subclass_replacement(db):
    class Bigger(Shop):
        heavy = tasks(AccountTask, "bigger_heavy")

    app = Bigger(db)

    assert app.nightly.tasks is app.heavy
    assert app.heavy.collection.name == "bigger_heavy"


def test_a_subclass_replacing_the_default_task_engine_takes_the_default_schedulers_with_it(db):
    class Replaced(BaseApp):
        task = tasks(AccountTask)

    app = Replaced(db)

    assert app.scheduler.tasks is app.task
    assert app.task.model is AccountTask


def test_a_referenced_engine_replaced_by_something_else_is_refused_at_class_definition():
    with pytest.raises(TypeError, match="Broken.nightly emits into heavy"):
        class Broken(Shop):
            heavy = pile()


# --- the distribution registry ---

class Fixed(BaseDistributions):
    @staticmethod
    def fixed(seconds: float) -> timedelta:
        return timedelta(seconds=seconds)


def test_the_distribution_registry_is_a_class_attribute_and_reaches_the_engines(db):
    class Custom(Shop):
        distributions = Fixed

    app = Custom(db)

    assert isinstance(app.distribution, DistributionEngine)
    assert "fixed" in app.distribution.functions
    assert all(engine.distributions is app.distribution for engine in app.taskEngines.values())
    assert all(engine.distributions is app.distribution for engine in app.schedulerEngines.values())


def test_the_default_registry_is_the_built_in_one(shop):
    assert shop.distribution.registry is BaseDistributions


def test_a_registry_that_is_not_a_distributions_class_is_refused_at_class_definition():
    with pytest.raises(TypeError, match="Wrong.distributions must be a BaseDistributions subclass"):
        class Wrong(BaseApp):
            distributions = dict


# --- defaults and pacing ---

def test_the_apps_defaults_reach_the_engines(db):
    class Configured(Shop):
        schedulerMissed = "replay"
        pileMaxAttempts = 3
        taskLeaseSeconds = 60

    app = Configured(db)

    assert app.scheduler.missed == "replay"
    assert app.nightly.missed == "skip"             # declared on the engine, so it wins
    assert app.outbox.maxAttempts == 3
    assert app.task.settings.leaseSeconds == 60
    assert app.heavy.settings.leaseSeconds == 900   # likewise


def test_a_bad_app_default_is_refused_at_class_definition_naming_itself():
    with pytest.raises(ValidationError, match="pileMaxAttempts"):
        class Wrong(BaseApp):
            pileMaxAttempts = 0


def test_a_removed_app_default_is_refused_at_class_definition():
    with pytest.raises(TypeError, match="taskMaxAttempts is no longer a default"):
        class Old(BaseApp):
            taskMaxAttempts = 3


@pytest.mark.parametrize("setting", ["schedulerMissed", "pileMaxAttempts", "taskTimeout", "distributions"])
def test_shared_behaviour_is_not_a_constructor_argument(db, setting):
    with pytest.raises(TypeError):
        Shop(db, **{setting: 1})


def test_poll_intervals_reach_the_engines(db):
    app = Shop(db, taskPollInterval=2, schedulerPollInterval=3)

    assert app.task.workers.pollInterval == 2
    assert app.heavy.workers.pollInterval == 2
    assert app.scheduler.workers.pollInterval == 3


def test_a_declarations_own_poll_interval_wins(db):
    class Paced(BaseApp):
        fast = tasks(pollInterval=0.5)
        slow = schedulers(pollInterval=10)

    app = Paced(db, taskPollInterval=2, schedulerPollInterval=3)

    assert (app.fast.workers.pollInterval, app.task.workers.pollInterval) == (0.5, 2)
    assert (app.slow.workers.pollInterval, app.scheduler.workers.pollInterval) == (10, 3)


def test_extra_indexes_are_created(db):
    class Indexed(BaseApp):
        audit = schedulers(extraIndexes=[IndexModel([("name", 1)], name="byName")])

    app = Indexed(db)

    assert "byName" in app.audit.collection.index_information()


# --- names an app reserves ---

@pytest.mark.parametrize("name", ["backlog", "init", "run", "fingerprint", "db", "piles", "engines", "taskTimeout", "distributions"])
def test_a_declaration_cannot_shadow_the_app(name):
    with pytest.raises(TypeError, match=f"would replace BaseApp.{name}"):
        type("Clashing", (BaseApp,), {name: pile()})


def test_a_task_cannot_shadow_the_app():
    with pytest.raises(TypeError, match="would replace BaseApp.startWorkers"):
        class Clashing(BaseApp):
            @task
            @staticmethod
            def startWorkers() -> None: ...


@pytest.mark.parametrize("name, declaration", [("task", pile()), ("scheduler", tasks()), ("task", schedulers())])
def test_the_default_engines_are_replaced_only_by_their_own_kind(name, declaration):
    with pytest.raises(TypeError, match=f"would replace BaseApp.{name}; declare it as a"):
        type("Clashing", (BaseApp,), {name: declaration})


def test_the_reserved_names_cover_every_attribute_init_sets(db):
    app = BaseApp(db)
    public = {name for name in vars(app) if not name.startswith("_")}

    assert public - {"task", "scheduler"} <= BaseApp._INSTANCE_ATTRIBUTES


# --- two apps, one database ---

def test_two_apps_on_one_database_share_state(db):
    producer = Shop(db)
    consumer = Shop(db)

    producer.task.schedule(Shop.greet(name="Ada"))

    assert consumer.task.work().status == "done"
    assert producer.task.find()[0].status == "done"


def test_separate_databases_stay_separate(db):
    Shop(db).task.schedule(Shop.greet(name="Ada"))

    assert Shop(db.client["another"]).task.count() == 0


def test_constructing_an_app_writes_nothing_but_indexes(db):
    Shop(db)

    assert all(db[name].count_documents({}) == 0 for name in db.list_collection_names())


def test_a_new_app_does_not_disturb_work_in_flight(db):
    first = Shop(db)
    held = first.task.schedule(Shop.greet(name="Ada"))
    first.task.collection.update_one(
        {"uid": held.uid},
        {"$set": {"status": "running", "claimId": "someone", "leaseUntil": utc_now() + timedelta(minutes=5)}},
    )
    item = first.outbox.add(to="a@b.c")
    first.outbox.claim()

    Shop(db)

    stored = first.task.get(held.uid)
    assert (stored.status, stored.claimId) == ("running", "someone")
    assert stored.leaseUntil > utc_now()
    assert first.outbox.get(item.uid).status == "running"


def test_finished_items_survive_a_new_app_and_its_housekeeping(db):
    first = Shop(db)
    done = first.outbox.add(to="a@b.c")
    first.outbox.done(first.outbox.claim())

    restarted = Shop(db)
    restarted.init()

    assert restarted.outbox.get(done.uid).status == "done"
    assert restarted.outbox.claim() is None


def test_missed_applies_after_startup_too(db):
    """init() has no say over missed beats: a scheduler that fell behind while the app was up is dealt
    with like one that fell behind while it was down."""

    app = Shop(db)
    app.init()
    scheduler = app.nightly.add(Shop.greet(name="Ada"), app.distribution("constant", dailyFrequency=24))
    app.nightly.update(scheduler.uid, deadline=utc_now() - timedelta(days=1))

    app.nightly.work()

    assert app.heavy.count() == 0       # nightly skips what it missed


# --- reprs ---

def test_every_engine_of_an_app_shares_one_repr_pattern(shop):
    assert [repr(shop.task), repr(shop.heavy), repr(shop.nightly), repr(shop.outbox), repr(shop.groups)] == [
        "TaskEngine task (pymonque_task)",
        "TaskEngine heavy (pymonque_task_heavy)",
        "SchedulerEngine nightly (pymonque_scheduler_nightly)",
        "PileEngine outbox (pymonque_pile_outbox)",
        "CollectionEngine groups (groups)",
    ]


def test_an_app_names_its_class_database_and_version(shop):
    assert repr(shop) == f"Shop (pymonque_test, {shop.fingerprint})"
    assert repr(shop.task.workers) == "WorkerLoop task-task (0 workers, idle)"
