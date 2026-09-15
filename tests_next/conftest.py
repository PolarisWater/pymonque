"""Shared fixtures for the rebuilt package.

Every test gets a fresh in-memory MongoDB, so nothing leaks between tests.
"""

import pytest
from mongomock import MongoClient


@pytest.fixture
def db():
    return MongoClient()["pymonque_test"]
