"""Declarations are checked where they are written, take one shape for every kind, and take the app's
default only for a setting left out."""

import pytest
from pydantic import BaseModel, ValidationError

from pymonque_next import AppDefaults, Document, Scheduler, Task, collection, pile, schedulers, task, tasks
from pymonque_next.declarations import Declaration, declaredOn
from pymonque_next.settings import PileSettings, SchedulerEngineSettings, TaskEngineSettings


class Payload(BaseModel):
    n: int = 0


class Account(Document):
    accountId: int = 0


class AccountTask(Task):
    accountId: int


class AccountScheduler(Scheduler):
    accountId: int


# --- checked where written ---

BAD_SETTINGS = {
    "collection model not a document":  (lambda: collection(Payload), "model"),
    "collection None":                  (lambda: collection(Document, collection=None), "collection"),
    "collection empty name":            (lambda: collection(Document, ""), "collection"),
    "collection not a collection":      (lambda: collection(Document, 42), "collection"),
    "collection key not a field":       (lambda: collection(Account, key="accountID"), "key"),
    "pile model not a model":           (lambda: pile(int), "model"),
    "pile maxAttempts 0":               (lambda: pile(maxAttempts=0), "maxAttempts"),
    "pile maxAttempts None":            (lambda: pile(maxAttempts=None), "maxAttempts"),
    "pile leaseSeconds 0":              (lambda: pile(leaseSeconds=0), "leaseSeconds"),
    "pile extraIndexes not indexes":    (lambda: pile(extraIndexes=["data.to"]), "extraIndexes"),
    "tasks model not a task":           (lambda: tasks(Scheduler), "model"),
    "tasks leaseSeconds None":          (lambda: tasks(leaseSeconds=None), "leaseSeconds"),
    "tasks pollInterval negative":      (lambda: tasks(pollInterval=-1), "pollInterval"),
    "schedulers model not a scheduler": (lambda: schedulers(Task), "model"),
    "schedulers missed unknown":        (lambda: schedulers(missed="execute once"), "missed"),
    "schedulers missed None":           (lambda: schedulers(missed=None), "missed"),
    "schedulers leaseSeconds negative": (lambda: schedulers(leaseSeconds=-1), "leaseSeconds"),
    "schedulers emitsInto by name":     (lambda: schedulers(emitsInto="heavy"), "emitsInto"),
    "schedulers emitsInto None":        (lambda: schedulers(emitsInto=None), "emitsInto"),
    "schedulers emitsInto a pile":      (lambda: schedulers(emitsInto=pile()), "emitsInto"),
    "task timeout 0":                   (lambda: task(timeout=0), "timeout"),
    "task skipAfter negative":          (lambda: task(skipAfter=-1), "skipAfter"),
}


@pytest.mark.parametrize("make, setting", BAD_SETTINGS.values(), ids=BAD_SETTINGS.keys())
def test_a_bad_setting_fails_where_it_is_written_and_names_itself(make, setting):
    with pytest.raises(ValidationError, match=setting):
        make()


@pytest.mark.parametrize("make", [
    lambda: collection(Document, nope=1),
    lambda: pile(nope=1),
    lambda: tasks(nope=1),
    lambda: schedulers(nope=1),
    lambda: task(nope=1),
], ids=["collection", "pile", "tasks", "schedulers", "task"])
def test_a_setting_that_does_not_exist_is_a_type_error_naming_it(make):
    with pytest.raises(TypeError, match="nope"):
        make()


RETIRED = {
    "task maxAttempts":             (lambda: task(maxAttempts=3), "maxAttempts.*retries inside its own code"),
    "task retryDelay":              (lambda: task(retryDelay=30), "retryDelay.*not retried"),
    "pile retryDelay":              (lambda: pile(retryDelay=10), "retryDelay.*no retry delay"),
    "pile payload":                 (lambda: pile(payload=Payload), "payload.*`model`"),
    "pile itemsCollection":         (lambda: pile(itemsCollection="x"), "itemsCollection.*`collection`"),
    "tasks tasksCollection":        (lambda: tasks(tasksCollection="x"), "tasksCollection.*`collection`"),
    "schedulers schedulersCollection": (lambda: schedulers(schedulersCollection="x"), "schedulersCollection.*`collection`"),
    "schedulers schedulerModel":    (lambda: schedulers(schedulerModel=Scheduler), "schedulerModel.*`model`"),
    "schedulers policy":            (lambda: schedulers(policy="skip"), "policy.*`missed`"),
}


@pytest.mark.parametrize("make, message", RETIRED.values(), ids=RETIRED.keys())
def test_a_removed_setting_says_what_became_of_it(make, message):
    with pytest.raises(TypeError, match=message):
        make()


def test_a_setting_is_coerced_like_any_pydantic_field():
    assert pile(leaseSeconds="5").leaseSeconds == 5.0
    assert pile(maxAttempts="3").maxAttempts == 3
    assert schedulers(pollInterval="0.5").pollInterval == 0.5


# --- one shape ---

def test_every_storage_declaration_takes_its_model_then_its_collection():
    declared = [
        (collection(Account, "accounts"), Account),
        (tasks(AccountTask, "account_tasks"), AccountTask),
        (schedulers(AccountScheduler, "account_ops"), AccountScheduler),
        (pile(Payload, "payloads"), Payload),
    ]

    for declaration, model in declared:
        assert declaration.model is model
        assert declaration.collection

    assert pile().model is None
    assert tasks().model is Task
    assert schedulers().model is Scheduler


@pytest.mark.parametrize("make", [
    lambda: collection(Account, "accounts", "accountId"),
    lambda: pile(Payload, "payloads", []),
    lambda: tasks(Task, "heavy", []),
    lambda: schedulers(Scheduler, "ops", []),
], ids=["collection", "pile", "tasks", "schedulers"])
def test_settings_after_the_collection_are_given_by_keyword(make):
    with pytest.raises(TypeError):
        make()


# --- left out takes the app's default, given wins ---

DEFAULTS = AppDefaults(
    taskLeaseSeconds=11, schedulerMissed="skip", schedulerLeaseSeconds=12, pileMaxAttempts=4, pileLeaseSeconds=13,
)


def test_left_out_settings_take_the_apps_defaults():
    assert tasks().settingsWith(DEFAULTS) == TaskEngineSettings(leaseSeconds=11)
    assert schedulers().settingsWith(DEFAULTS) == SchedulerEngineSettings(missed="skip", leaseSeconds=12)
    assert pile().settingsWith(DEFAULTS) == PileSettings(maxAttempts=4, leaseSeconds=13)


def test_given_settings_win():
    assert tasks(leaseSeconds=900).settingsWith(DEFAULTS) == TaskEngineSettings(leaseSeconds=900)
    assert schedulers(missed="replay", leaseSeconds=5).settingsWith(DEFAULTS) == SchedulerEngineSettings(
        missed="replay", leaseSeconds=5,
    )
    assert pile(maxAttempts=2, leaseSeconds=30).settingsWith(DEFAULTS) == PileSettings(maxAttempts=2, leaseSeconds=30)


# --- the app's defaults ---

def test_the_app_defaults():
    assert AppDefaults().model_dump() == {
        "taskTimeout": None, "taskSkipAfter": None, "taskLeaseSeconds": 300,
        "schedulerMissed": "once", "schedulerLeaseSeconds": 300,
        "pileMaxAttempts": 1, "pileLeaseSeconds": 300,
    }


def test_the_defaults_are_read_from_the_app_class_and_coerced():
    class App:
        pileMaxAttempts = "3"
        schedulerLeaseSeconds = 30

    defaults = AppDefaults.of(App)

    assert (defaults.pileMaxAttempts, defaults.schedulerLeaseSeconds, defaults.taskLeaseSeconds) == (3, 30, 300)


@pytest.mark.parametrize("default", [
    {"schedulerMissed": "sometimes"}, {"pileMaxAttempts": 0}, {"pileMaxAttempts": None},
    {"taskTimeout": 0}, {"taskSkipAfter": -1}, {"taskLeaseSeconds": 0}, {"pileLeaseSeconds": None},
])
def test_a_bad_app_default_is_refused_naming_itself(default):
    with pytest.raises(ValidationError, match=next(iter(default))):
        AppDefaults.of(type("App", (), default))


@pytest.mark.parametrize("name", ["taskMaxAttempts", "taskRetryDelay", "itemMaxAttempts", "itemRetryDelay"])
def test_a_removed_app_default_is_refused_saying_why(name):
    with pytest.raises(TypeError, match=name):
        AppDefaults.of(type("App", (), {name: 1}))


# --- names and collections ---

def test_class_access_gives_the_declaration():
    class App:
        outbox = pile(Payload)

    assert isinstance(App.outbox, pile)
    assert App.outbox.name == "outbox"
    assert App.outbox.owner is App


def test_the_default_collection_names(db):
    class App:
        groups      = collection(Document)
        task        = tasks()
        heavy       = tasks()
        scheduler   = schedulers()
        nightly     = schedulers()
        outbox      = pile()

    names = {name: declared.collectionIn(db).name for name, declared in declaredOn(App, Declaration).items()}

    assert names == {
        "groups":       "groups",
        "task":         "pymonque_task",
        "heavy":        "pymonque_task_heavy",
        "scheduler":    "pymonque_scheduler",
        "nightly":      "pymonque_scheduler_nightly",
        "outbox":       "pymonque_pile_outbox",
    }


def test_a_collection_can_be_named_or_given_as_an_existing_object(db):
    elsewhere = db["elsewhere"]

    class App:
        jobs    = pile(collection="my_jobs")
        things  = collection(Document, elsewhere)
        heavy   = tasks(Task, "render_tasks")

    assert App.jobs.collectionIn(db).name == "my_jobs"
    assert App.things.collectionIn(db) is elsewhere
    assert App.heavy.collectionIn(db).name == "render_tasks"


def test_a_declaration_on_no_class_has_no_default_collection(db):
    with pytest.raises(TypeError, match="not declared"):
        pile().collectionIn(db)


def test_declarations_are_collected_through_the_mro_and_a_subclass_replaces_one():
    class Parent:
        groups = collection(Document)
        outbox = pile()
        heavy  = tasks()

    class Child(Parent):
        groups = collection(Document, "other_groups")
        outbox = None                       # redefined as something else
        extra  = pile()

    assert declaredOn(Child, collection) == {"groups": Child.groups}
    assert declaredOn(Child, pile) == {"extra": Child.extra}
    assert declaredOn(Child, tasks) == {"heavy": Parent.heavy}


# --- emitsInto ---

def test_a_scheduler_engine_emits_into_a_task_engine_declared_above_it():
    class App:
        heavy   = tasks()
        nightly = schedulers(emitsInto=heavy)

    assert App.nightly.emitsInto is App.heavy


def test_emitsInto_may_name_a_task_engine_the_class_inherits():
    class Parent:
        heavy = tasks()

    class Child(Parent):
        nightly = schedulers(emitsInto=Parent.heavy)

    assert Child.nightly.emitsInto is Parent.heavy


def test_emitsInto_another_app_classes_task_engine_is_refused():
    class Other:
        heavy = tasks()

    with pytest.raises(ValidationError, match=r"(?s)emitsInto.*Other\.heavy"):
        class App:
            nightly = schedulers(emitsInto=Other.heavy)


def test_emitsInto_a_task_engine_this_class_replaced_is_refused():
    class Parent:
        heavy = tasks()

    with pytest.raises(ValidationError, match="emitsInto"):
        class Child(Parent):
            nightly = schedulers(emitsInto=Parent.heavy)
            heavy   = tasks(AccountTask)


def test_emitsInto_a_task_engine_declared_below_is_refused():
    loose = tasks()

    with pytest.raises(ValidationError, match="emitsInto"):
        class App:
            nightly = schedulers(emitsInto=loose)
            heavy   = loose


# --- a declaration is not a decorator ---

def test_a_declaration_named_task_hiding_the_decorator_says_so():
    with pytest.raises(TypeError, match="not a decorator.*declare it below the tasks"):
        class App:
            task = tasks()

            @task
            @staticmethod
            def ping() -> str:
                return "pong"


def test_a_declaration_named_task_below_the_tasks_is_fine():
    class App:
        @task
        @staticmethod
        def ping() -> str:
            return "pong"

        task = tasks(AccountTask)

    assert isinstance(App.task, tasks)
    assert set(declaredOn(App, task)) == {"ping"}
