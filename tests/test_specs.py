"""CallSpec, FuncSpec, the @task descriptor, and the reflection helpers."""

import pytest
from pydantic import BaseModel

from pymonque import CallSpec, FuncSpec, BaseApp, task
from pymonque.core import buildValidator, getStaticmethods, uuid4str

from conftest import ExampleApp


# --- CallSpec ---

def test_new_packs_kwargs():
    spec = CallSpec.new("greet", name="Ada", greeting="Hi")

    assert spec.functionName == "greet"
    assert spec.kwargs == {"name": "Ada", "greeting": "Hi"}


def test_call_dispatches_against_a_mapping():
    spec = CallSpec.new("greet", name="Ada")

    assert spec({"greet": lambda name: f"hi {name}"}) == "hi Ada"


def test_call_dispatches_against_an_object():
    class Source:
        @staticmethod
        def greet(name):
            return f"hi {name}"

    assert CallSpec.new("greet", name="Ada")(Source) == "hi Ada"


def test_call_raises_keyerror_for_an_unknown_name():
    with pytest.raises(KeyError):
        CallSpec.new("missing")({})


def test_roundtrips_through_mongo(db):
    spec = CallSpec.new("greet", name="Ada", greeting="Hi")
    db["specs"].insert_one(spec.model_dump())

    assert CallSpec.model_validate(db["specs"].find_one()) == spec


def test_repr():
    assert repr(CallSpec.new("greet", name="Ada")) == "greet({'name': 'Ada'})"


# --- the @task descriptor ---

def test_class_access_returns_a_funcspec():
    assert isinstance(ExampleApp.greet, FuncSpec)


def test_calling_a_funcspec_builds_a_callspec():
    spec = ExampleApp.greet(name="Ada")

    assert isinstance(spec, CallSpec)
    assert spec.functionName == "greet"
    assert spec.kwargs == {"name": "Ada"}


def test_instance_access_executes_a_staticmethod_task(app):
    assert app.greet(name="Ada") == "Hello, Ada!"


def test_instance_access_binds_an_instance_task(app):
    assert app.whoami() == "ExampleApp"


def test_tasks_are_discovered_across_the_mro():
    names = set(ExampleApp._getTasks())

    assert {"greet", "boom", "whoami", "send_one"} <= names


def test_a_subclass_can_override_a_task(db):
    class Child(ExampleApp):
        @task
        @staticmethod
        def greet(name: str, greeting: str = "Hello") -> str:
            return f"override {name}"

    child = Child(db)

    assert child.greet(name="Ada") == "override Ada"
    assert Child._getTasks()["greet"] is Child.__dict__["greet"]


def test_a_plain_method_is_not_a_task(db):
    class WithHelper(BaseApp):
        def helper(self):
            return 1

        @task
        @staticmethod
        def real():
            return 2

    assert set(WithHelper._getTasks()) == {"real"}


# --- buildValidator ---

def test_validator_requires_arguments_without_a_default():
    Args = buildValidator(lambda name, greeting="Hello": None)

    assert Args(name="Ada").greeting == "Hello"

    with pytest.raises(ValueError):
        Args()


def test_validator_forbids_unknown_arguments():
    Args = buildValidator(lambda name: None)

    with pytest.raises(ValueError):
        Args(name="Ada", nope=1)


def test_validator_enforces_annotations():
    def annotated(count: int): ...

    Args = buildValidator(annotated)

    assert Args(count="3").count == 3  # coerced

    with pytest.raises(ValueError):
        Args(count="three")


def test_validator_accepts_anything_for_unannotated_arguments():
    Args = buildValidator(lambda value: None)

    assert Args(value=object()) is not None


def test_validator_skips_self_on_a_bound_method(app):
    Args = buildValidator(app.whoami)

    assert Args() is not None  # `self` is already bound, so not a field


# --- getStaticmethods ---

def test_collects_staticmethods_through_the_mro():
    class Base:
        @staticmethod
        def a(): ...

    class Child(Base):
        @staticmethod
        def b(): ...

        @staticmethod
        def _private(): ...

        def instance(self): ...

    assert set(getStaticmethods(Child)) == {"a", "b"}


def test_a_child_staticmethod_wins():
    class Base:
        @staticmethod
        def a():
            return "base"

    class Child(Base):
        @staticmethod
        def a():
            return "child"

    assert getStaticmethods(Child)["a"]() == "child"


def test_uuid4str_is_unique():
    assert uuid4str() != uuid4str()
