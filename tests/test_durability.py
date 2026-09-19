"""Work collections are written by majority and read from the primary, whatever the app's database was
given: a claim acknowledged by a primary alone is lost if it fails over, and two workers then hold the
same work."""

from pymongo import ReadPreference, WriteConcern

from pymonque import BaseApp, collection, Document, pile, schedulers, task, tasks


class Note(Document):
    text: str = ""


class App(BaseApp):
    heavy = tasks()
    beats = schedulers(emitsInto=heavy)
    outbox = pile()
    notes = collection(Note)

    @task
    @staticmethod
    def noop() -> None: ...


def test_every_work_collection_is_majority_written_and_read_from_the_primary(db):
    loose = db.client.get_database(db.name, write_concern=WriteConcern(w=1), read_preference=ReadPreference.SECONDARY_PREFERRED)
    app = App(loose, enforceVersion=False)

    for engine in (app.task, app.heavy, app.scheduler, app.beats, app.outbox):
        assert engine.collection.write_concern.document == {"w": "majority"}, engine
        assert engine.collection.read_preference == ReadPreference.PRIMARY, engine

    assert app.registry.collection.write_concern.document == {"w": "majority"}


def test_a_plain_collection_keeps_the_options_it_was_given(db):
    loose = db.client.get_database(db.name, write_concern=WriteConcern(w=1))
    app = App(loose, enforceVersion=False)

    assert app.notes.collection.write_concern.document != {"w": "majority"}     # your data, your choice


def test_the_options_change_nothing_but_how_it_is_written(db):
    app = App(db, enforceVersion=False)
    queued = app.task.schedule(App.noop())

    assert db["pymonque_task"].find_one({"uid": queued.uid}) is not None
    assert app.task.work().status == "done"
