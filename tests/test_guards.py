"""Mistakes the library should catch, and where it should catch them."""

from datetime import timedelta

import pytest
from pydantic import BaseModel

from pymonque import (
    BaseApp, Document, Scheduler, collection, pile, schedulers, task, utc_now
)
from pymonque.exceptions import DistributionValidationError, TaskValidationError


class Group(Document):
    name: str = ""


# --- an interval must move a scheduler forward ---

class Ticking(BaseApp):
    @task
    @staticmethod
    def ping() -> str:
        return "pong"


@pytest.fixture
def ticking(db):
    return Ticking(db, enforceVersion=False)


@pytest.mark.parametrize("dailyFrequency", [0, -1, -0.5])
def test_a_frequency_that_cannot_give_a_positive_interval_is_refused(ticking, dailyFrequency):
    with pytest.raises(DistributionValidationError):
        ticking.distribution("constant", dailyFrequency=dailyFrequency)


@pytest.mark.parametrize("name", ["constant", "exponential"])
def test_every_distribution_guards_its_frequency(ticking, name):
    with pytest.raises(DistributionValidationError):
        ticking.distribution(name, dailyFrequency=0)


def test_a_custom_distribution_returning_a_dead_interval_is_refused(db):
    from pymonque import BaseDistributions

    class Custom(BaseDistributions):
        @staticmethod
        def stuck(dailyFrequency: float) -> timedelta:
            return timedelta(0)

    app = Ticking(db, distributionsRegistry=Custom, enforceVersion=False)
    spec = app.distribution("stuck", dailyFrequency=1)

    with pytest.raises(DistributionValidationError):
        app.distribution.gen(spec)


def test_a_scheduler_cannot_be_built_on_a_dead_interval(ticking):
    """It would emit on every poll for as long as it existed."""

    with pytest.raises(DistributionValidationError):
        ticking.scheduler.add(
            Ticking.ping(), ticking.distribution("constant", dailyFrequency=0)
        )


def test_normal_never_draws_a_dead_interval(ticking):
    """A wide spread draws below zero; the floor keeps it positive."""

    for _ in range(200):
        interval = ticking.distribution.gen(
            ticking.distribution("normal", dailyFrequency=1, stdFraction=5)
        )
        assert interval > timedelta(0)


# --- a task is called with keyword arguments only ---

def test_a_task_taking_star_args_is_refused_where_it_is_written(db):
    with pytest.raises(TypeError, match="positionally"):
        class Star(BaseApp):
            @task
            @staticmethod
            def positional(*rest) -> str:
                return "ok"

        Star(db, enforceVersion=False)


def test_a_task_taking_positional_only_args_is_refused(db):
    with pytest.raises(TypeError, match="positionally"):
        class PosOnly(BaseApp):
            @task
            @staticmethod
            def fixed(a, /) -> str:
                return "ok"

        PosOnly(db, enforceVersion=False)


def test_a_task_taking_kwargs_accepts_anything_extra(db):
    class Kw(BaseApp):
        @task
        @staticmethod
        def flexible(a: int, **extra) -> str:
            return f"{a}:{sorted(extra)}"

    app = Kw(db, enforceVersion=False)
    app.task.schedule(Kw.flexible(a=1, whatever=2, more=3))
    app.task._work()

    assert app.task.find()[0].result == "1:['more', 'whatever']"


def test_a_task_taking_kwargs_still_checks_the_named_ones(db):
    class Kw(BaseApp):
        @task
        @staticmethod
        def flexible(a: int, **extra) -> str:
            return "ok"

    app = Kw(db, enforceVersion=False)

    with pytest.raises(TaskValidationError):
        app.task.schedule(Kw.flexible(a="not an int"))


def test_an_unannotated_argument_accepts_anything(db):
    class Loose(BaseApp):
        @task
        @staticmethod
        def anything(x) -> str:
            return str(x)

    app = Loose(db, enforceVersion=False)
    app.task.schedule(Loose.anything(x={"a": 1}))

    assert app.task.count() == 1


def test_an_annotated_constraint_is_enforced(db):
    """get_type_hints keeps Annotated, so a task can declare its own bounds."""

    from typing import Annotated
    from pydantic import Field

    class Bounded(BaseApp):
        @task
        @staticmethod
        def retain(days: Annotated[int, Field(gt=0)]) -> int:
            return days

    app = Bounded(db, enforceVersion=False)
    app.task.schedule(Bounded.retain(days=30))

    with pytest.raises(TaskValidationError):
        app.task.schedule(Bounded.retain(days=0))


# --- the wrong model, caught at declaration ---

def test_a_collection_needs_a_document(db):
    class Plain(BaseModel):
        name: str = ""

    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="subclass of Document"):
        collection(Plain)


def test_a_scheduler_engine_needs_a_scheduler(db):
    class Plain(BaseModel):
        name: str = ""

    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="subclass of Scheduler"):
        schedulers(Plain)


def test_a_pile_takes_a_plain_model(db):
    """Payloads are not documents — they have no uid and are wrapped in an Item."""

    class Payload(BaseModel):
        n: int = 0

    class App(BaseApp):
        work = pile(Payload)

    app = App(db, enforceVersion=False)

    assert app.work.add(n=1).data.n == 1
