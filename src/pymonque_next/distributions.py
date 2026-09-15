"""Interval functions for schedulers: the built-in distributions, and the engine that calls a registry
of them."""

from __future__ import annotations

import math
import random
from datetime import timedelta
from typing import Any, Callable

from pydantic import NonNegativeFloat, PositiveFloat

from .calls import CallSpec, Functions, nearestAttributes
from .exceptions import DistributionNotFound, DistributionValidationError


DAY = 86400


def getStaticmethods(cls: type) -> dict[str, Callable]:
    """Every public staticmethod on a class or its parents, the nearest definition winning."""

    return {
        name: value.__func__
        for name, value in nearestAttributes(cls).items()
        if isinstance(value, staticmethod) and not name.startswith("_")
    }


class BaseDistributions:
    """The built-in distributions.

    Subclass it to add your own: staticmethods not starting with `_`, each taking `dailyFrequency` —
    how many times a day, on average — and returning a positive timedelta. The built-ins stay
    available in a subclass.
    """

    # dailyFrequency is positive throughout: zero divides, and a negative interval walks a
    # scheduler backwards, which no missed-beats rule can stop

    @staticmethod
    def constant(dailyFrequency: PositiveFloat) -> timedelta:
        return timedelta(seconds=DAY / dailyFrequency)

    @staticmethod
    def normal(dailyFrequency: PositiveFloat, stdFraction: NonNegativeFloat) -> timedelta:
        mean = DAY / dailyFrequency
        interval = random.gauss(mean, mean * stdFraction)

        # a wide enough spread draws below zero; floor it well clear of it
        return timedelta(seconds=max(interval, mean / 100))

    @staticmethod
    def lognormal(dailyFrequency: PositiveFloat, sigma: NonNegativeFloat) -> timedelta:
        mean = DAY / dailyFrequency
        mu = math.log(mean) - sigma ** 2 / 2   # so the mean, not the median, matches the frequency

        return timedelta(seconds=random.lognormvariate(mu, sigma))

    @staticmethod
    def exponential(dailyFrequency: PositiveFloat) -> timedelta:
        return timedelta(seconds=random.expovariate(dailyFrequency / DAY))


class DistributionEngine:
    """A registry of distributions, and the calls that name them."""

    def __init__(self, registry: type[BaseDistributions] = BaseDistributions):
        if not (isinstance(registry, type) and issubclass(registry, BaseDistributions)):
            raise TypeError(f"a distribution registry subclasses BaseDistributions, not {registry!r}")

        self.registry: type[BaseDistributions] = registry
        self.functions = Functions(
            getStaticmethods(registry),
            what="distribution",
            notFound=DistributionNotFound,
            invalid=DistributionValidationError,
        )

    def __call__(self, functionName: str, /, **kwargs: Any) -> CallSpec:
        """A call of the distribution of this name, checked."""

        return self.functions.build(functionName, **kwargs)

    def validate(self, distribution: CallSpec) -> CallSpec:
        return self.functions.validate(distribution)

    def gen(self, distribution: CallSpec) -> timedelta:
        """Draw an interval."""

        interval = self.functions.call(distribution)

        if not isinstance(interval, timedelta):
            raise DistributionValidationError(f"{distribution!r} returned {interval!r}, not a timedelta")

        # a custom distribution too: a scheduler on an interval that is not positive never moves
        # forward, so it would emit on every poll for as long as it exists
        if interval <= timedelta(0):
            raise DistributionValidationError(f"{distribution!r} returned {interval}; an interval must be positive")

        return interval
