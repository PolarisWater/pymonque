"""Distributions: the built-in intervals, the registry, and the engine that builds, checks and draws
from calls of them."""

import random
from datetime import timedelta

import pytest

from pymonque import BaseDistributions, CallSpec, DistributionEngine
from pymonque.exceptions import DistributionNotFound, DistributionValidationError


DAY = 86400


class Custom(BaseDistributions):
    @staticmethod
    def fixed(dailyFrequency: float, seconds: float) -> timedelta:
        return timedelta(seconds=seconds)

    @staticmethod
    def stuck(dailyFrequency: float) -> timedelta:
        return timedelta(0)

    @staticmethod
    def seconds(dailyFrequency: float) -> float:
        return 1.0

    @staticmethod
    def _hidden(dailyFrequency: float) -> timedelta:
        return timedelta(seconds=1)


@pytest.fixture
def engine() -> DistributionEngine:
    return DistributionEngine()


@pytest.fixture
def custom() -> DistributionEngine:
    return DistributionEngine(Custom)


# --- the built-ins ---

def test_constant_is_exact():
    assert BaseDistributions.constant(dailyFrequency=4) == timedelta(seconds=DAY / 4)


@pytest.mark.parametrize("name, kwargs", [
    ("constant", {"dailyFrequency": 12}),
    ("normal", {"dailyFrequency": 12, "stdFraction": 0.2}),
    ("lognormal", {"dailyFrequency": 12, "sigma": 0.5}),
    ("exponential", {"dailyFrequency": 12}),
])
def test_every_distribution_gives_a_positive_interval(engine, name, kwargs):
    random.seed(0)

    assert all(engine.gen(engine(name, **kwargs)) > timedelta(0) for _ in range(100))


@pytest.mark.parametrize("name, kwargs", [
    ("normal", {"dailyFrequency": 12, "stdFraction": 0.2}),
    ("lognormal", {"dailyFrequency": 12, "sigma": 0.5}),
    ("exponential", {"dailyFrequency": 12}),
])
def test_the_mean_interval_matches_the_daily_frequency(name, kwargs):
    random.seed(0)
    samples = [getattr(BaseDistributions, name)(**kwargs).total_seconds() for _ in range(5000)]
    expected = DAY / kwargs["dailyFrequency"]

    assert expected * 0.9 < sum(samples) / len(samples) < expected * 1.1


def test_normal_never_draws_a_dead_interval(engine):
    """A spread wider than the mean would draw below zero; the floor keeps it positive."""

    random.seed(0)
    distribution = engine("normal", dailyFrequency=1, stdFraction=5)

    assert min(engine.gen(distribution) for _ in range(500)) > timedelta(0)


# --- the registry ---

def test_the_registry_holds_the_public_staticmethods(engine, custom):
    assert set(engine.functions) == {"constant", "normal", "lognormal", "exponential"}
    assert set(custom.functions) == {"constant", "normal", "lognormal", "exponential", "fixed", "stuck", "seconds"}


def test_a_custom_registry_extends_the_built_ins(custom):
    assert custom.gen(custom("fixed", dailyFrequency=1, seconds=7)) == timedelta(seconds=7)
    assert custom.gen(custom("constant", dailyFrequency=24)) == timedelta(hours=1)


def test_the_default_registry_does_not_see_custom_distributions(engine):
    with pytest.raises(DistributionNotFound):
        engine("fixed", dailyFrequency=1, seconds=7)


def test_a_registry_must_subclass_base_distributions():
    with pytest.raises(TypeError):
        DistributionEngine(object)


# --- building and checking a call ---

def test_calling_the_engine_builds_a_checked_call(engine):
    assert engine("constant", dailyFrequency=2) == CallSpec.new("constant", dailyFrequency=2)


@pytest.mark.parametrize("kwargs", [
    {"dailyFrequency": 1},                          # stdFraction missing
    {"dailyFrequency": 1, "stdFraction": 0.1, "nope": 2},
    {"dailyFrequency": "often", "stdFraction": 0.1},
    {"dailyFrequency": 1, "stdFraction": -0.1},
], ids=["missing", "unknown", "wrongly typed", "negative spread"])
def test_arguments_that_do_not_fit_are_refused(engine, kwargs):
    with pytest.raises(DistributionValidationError):
        engine("normal", **kwargs)


def test_an_unknown_distribution_is_refused(engine):
    with pytest.raises(DistributionNotFound):
        engine("does_not_exist", dailyFrequency=1)


@pytest.mark.parametrize("dailyFrequency", [0, -1, -0.5])
@pytest.mark.parametrize("name", ["constant", "exponential"])
def test_a_frequency_that_cannot_give_a_positive_interval_is_refused(engine, name, dailyFrequency):
    with pytest.raises(DistributionValidationError):
        engine(name, dailyFrequency=dailyFrequency)


# --- drawing ---

def test_gen_coerces_what_validation_accepted(engine):
    assert engine.gen(engine("constant", dailyFrequency="24")).total_seconds() == 3600


def test_gen_refuses_a_distribution_returning_something_else(custom):
    with pytest.raises(DistributionValidationError, match="not a timedelta"):
        custom.gen(custom("seconds", dailyFrequency=1))


def test_gen_refuses_a_custom_dead_interval(custom):
    with pytest.raises(DistributionValidationError, match="positive"):
        custom.gen(custom("stuck", dailyFrequency=1))


def test_gen_checks_a_call_that_was_never_checked(engine):
    with pytest.raises(DistributionValidationError):
        engine.gen(CallSpec.new("constant", dailyFrequency=0))
