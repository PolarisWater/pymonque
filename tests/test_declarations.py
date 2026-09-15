"""Every declaration is checked where it is written, and leaving a setting out is the only way
to take the app's default."""

import pytest
from pydantic import BaseModel, ValidationError

from pymonque import BaseApp, Document, collection, pile, schedulers, task


class Payload(BaseModel):
    n: int = 0


# --- checked where written, with one kind of error ---

BAD_DECLARATIONS = {
    "pile maxAttempts 0":           (lambda: pile(maxAttempts=0), "maxAttempts"),
    "pile maxAttempts None":        (lambda: pile(maxAttempts=None), "maxAttempts"),
    "pile retryDelay negative":     (lambda: pile(retryDelay=-1), "retryDelay"),
    "pile leaseSeconds 0":          (lambda: pile(leaseSeconds=0), "leaseSeconds"),
    "pile collection None":         (lambda: pile(itemsCollection=None), "itemsCollection"),
    "pile payload not a model":     (lambda: pile(payload=int), "payload"),
    "pile extraIndexes not indexes": (lambda: pile(extraIndexes=["data.to"]), "extraIndexes"),
    "schedulers missed unknown":    (lambda: schedulers(missed="execute once"), "missed"),
    "schedulers missed None":       (lambda: schedulers(missed=None), "missed"),
    "schedulers pollInterval None": (lambda: schedulers(pollInterval=None), "pollInterval"),
    "schedulers leaseSeconds < 0":  (lambda: schedulers(leaseSeconds=-1), "leaseSeconds"),
    "schedulers collection None":   (lambda: schedulers(schedulersCollection=None), "schedulersCollection"),
    "schedulers model not one":     (lambda: schedulers(Payload), "schedulerModel"),
    "collection model not one":     (lambda: collection(Payload), "model"),
    "collection name None":         (lambda: collection(Document, collection=None), "collection"),
    "task timeout 0":               (lambda: task(timeout=0), "timeout"),
    "task maxAttempts None":        (lambda: task(maxAttempts=None), "maxAttempts"),
}


@pytest.mark.parametrize("make, setting", BAD_DECLARATIONS.values(), ids=BAD_DECLARATIONS.keys())
def test_a_bad_setting_fails_where_it_is_written_and_names_itself(make, setting):
    with pytest.raises(ValidationError, match=setting):
        make()


@pytest.mark.parametrize("default", [
    {"schedulerMissed": "sometimes"}, {"itemMaxAttempts": 0}, {"itemRetryDelay": -1},
    {"taskTimeout": 0}, {"taskMaxAttempts": None},
])
def test_a_bad_app_default_fails_when_the_class_is_defined(default):
    with pytest.raises(ValidationError, match=next(iter(default))):
        type("Broken", (BaseApp,), default)


def test_a_setting_is_coerced_like_any_pydantic_field():
    assert pile(retryDelay="5").retryDelay == 5.0
    assert schedulers(pollInterval="0.5").pollInterval == 0.5


# --- left out takes the default, given wins ---

def test_left_out_settings_take_the_apps_defaults(db):
    class App(BaseApp):
        itemMaxAttempts = 4
        itemRetryDelay = 9
        schedulerMissed = "skip"

        jobs = pile()
        ops = schedulers()

    app = App(db, schedulerPollInterval=7, leaseSeconds=11)

    assert (app.jobs.maxAttempts, app.jobs.retryDelay, app.jobs.leaseSeconds) == (4, 9, 11)
    assert (app.ops.missed, app.ops.pollInterval, app.ops.leaseSeconds) == ("skip", 7, 11)
    assert app.jobs.collection.name == "pymonque_pile_jobs"
    assert app.ops.collection.name == "pymonque_schedulers_ops"


def test_given_settings_win(db):
    class App(BaseApp):
        itemMaxAttempts = 4
        schedulerMissed = "skip"

        jobs = pile(maxAttempts=2, retryDelay=0, leaseSeconds=30, itemsCollection="my_jobs")
        ops = schedulers(missed="replay", pollInterval=0.5, leaseSeconds=60, schedulersCollection="my_ops")
        things = collection(Document, collection="my_things")

    app = App(db, schedulerPollInterval=7, leaseSeconds=11)

    assert (app.jobs.maxAttempts, app.jobs.retryDelay, app.jobs.leaseSeconds) == (2, 0, 30)
    assert (app.ops.missed, app.ops.pollInterval, app.ops.leaseSeconds) == ("replay", 0.5, 60)
    assert [app.jobs.collection.name, app.ops.collection.name, app.things.collection.name] == [
        "my_jobs", "my_ops", "my_things",
    ]


def test_an_existing_collection_object_is_accepted(db):
    class App(BaseApp):
        things = collection(Document, collection=db["elsewhere"])

    assert App(db).things.collection.name == "elsewhere"
