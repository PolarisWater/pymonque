"""BaseDistributions and DistributionEngine."""

import random
from datetime import timedelta

import pytest

from pymonque import (
    BaseApp, BaseDistributions, DistributionEngine, CallSpec, task,
)
from pymonque.exceptions import DistributionNotFound, DistributionValidationError

from conftest import ExampleApp


DAY = 86400


# --- the built-in distributions ---

def test_constant_is_exact():
    assert BaseDistributions.constant(dailyFrequency=4) == timedelta(seconds=DAY / 4)


@pytest.mark.parametrize("name, kwargs", [
    ("constant", {"dailyFrequency": 12}),
    ("normal", {"dailyFrequency": 12, "stdFraction": 0.2}),
    ("lognormal", {"dailyFrequency": 12, "sigma": 0.5}),
    ("exponential", {"dailyFrequency": 12}),
])
def test_every_distribution_returns_a_positive_timedelta(name, kwargs):
    random.seed(0)
    samples = [getattr(BaseDistributions, name)(**kwargs) for _ in range(100)]

    assert all(isinstance(s, timedelta) for s in samples)
    assert all(s >= timedelta(0) for s in samples)


@pytest.mark.parametrize("name, kwargs", [
    ("normal", {"dailyFrequency": 12, "stdFraction": 0.2}),
    ("lognormal", {"dailyFrequency": 12, "sigma": 0.5}),
    ("exponential", {"dailyFrequency": 12}),
])
def test_the_mean_interval_matches_the_daily_frequency(name, kwargs):
    random.seed(0)
    expected = DAY / kwargs["dailyFrequency"]
    samples = [getattr(BaseDistributions, name)(**kwargs).total_seconds() for _ in range(5000)]
    mean = sum(samples) / len(samples)

    assert expected * 0.9 < mean < expected * 1.1


def test_normal_never_goes_negative():
    random.seed(0)
    # a std wider than the mean would otherwise produce negative intervals
    samples = [BaseDistributions.normal(dailyFrequency=1, stdFraction=5) for _ in range(500)]

    assert min(samples) >= timedelta(0)


def test_registry_walks_the_mro():
    assert set(BaseDistributions._getDistributions()) == {
        "constant", "normal", "lognormal", "exponential",
    }


# --- DistributionEngine ---

def test_call_builds_and_validates_a_callspec(app):
    spec = app.distribution("constant", dailyFrequency=2)

    assert isinstance(spec, CallSpec)
    assert spec.functionName == "constant"


def test_unknown_distribution_is_rejected(app):
    with pytest.raises(DistributionNotFound):
        app.distribution("does_not_exist", dailyFrequency=1)


def test_missing_argument_is_rejected(app):
    with pytest.raises(DistributionValidationError):
        app.distribution("normal", dailyFrequency=1)  # stdFraction is required


def test_unknown_argument_is_rejected(app):
    with pytest.raises(DistributionValidationError):
        app.distribution("constant", dailyFrequency=1, nope=2)


def test_wrongly_typed_argument_is_rejected(app):
    with pytest.raises(DistributionValidationError):
        app.distribution("constant", dailyFrequency="often")


def test_gen_produces_an_interval(app):
    assert app.distribution.gen(app.distribution("constant", dailyFrequency=2)) == timedelta(seconds=DAY / 2)


def test_gen_rejects_a_distribution_that_returns_the_wrong_type():
    class Bad(BaseDistributions):
        @staticmethod
        def seconds(dailyFrequency: float) -> float:
            return 1.0  # not a timedelta

    engine = DistributionEngine(Bad)

    with pytest.raises(DistributionValidationError):
        engine.gen(CallSpec.new("seconds", dailyFrequency=1))


# --- custom registries ---

def test_a_custom_registry_extends_the_built_ins(db):
    class Custom(BaseDistributions):
        @staticmethod
        def fixed(dailyFrequency: float, seconds: float) -> timedelta:
            return timedelta(seconds=seconds)

    class Q(ExampleApp):
        pass

    app = Q(db, distributionsRegistry=Custom)

    assert app.distribution.gen(app.distribution("fixed", dailyFrequency=1, seconds=7)) == timedelta(seconds=7)
    assert app.distribution("constant", dailyFrequency=1)  # inherited ones still work


def test_the_default_registry_does_not_see_custom_distributions(app):
    with pytest.raises(DistributionNotFound):
        app.distribution("fixed", dailyFrequency=1, seconds=7)
