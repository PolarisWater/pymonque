"""Calls stored as data: CallSpec, and checking a call against its function's signature when it is
built and again when it runs."""

from typing import Annotated

import pytest
from pydantic import BaseModel, Field

from pymonque import CallSpec, Functions, task
from pymonque.calls import nearestAttributes
from pymonque.declarations import declaredOn
from pymonque.documents import uuid4str
from pymonque.exceptions import TaskNotFound, TaskValidationError


# --- CallSpec ---

def test_new_packs_kwargs():
    spec = CallSpec.new("greet", name="Ada", greeting="Hi")

    assert spec.functionName == "greet"
    assert spec.kwargs == {"name": "Ada", "greeting": "Hi"}


def test_a_kwarg_may_be_called_functionName():
    assert CallSpec.new("rename", functionName="other").kwargs == {"functionName": "other"}


def test_a_call_dispatches_against_a_mapping():
    assert CallSpec.new("greet", name="Ada")({"greet": lambda name: f"hi {name}"}) == "hi Ada"


def test_a_call_dispatches_against_an_object():
    class Source:
        @staticmethod
        def greet(name):
            return f"hi {name}"

    assert CallSpec.new("greet", name="Ada")(Source) == "hi Ada"


def test_a_call_of_an_unknown_name_raises_keyerror():
    with pytest.raises(KeyError):
        CallSpec.new("missing")({})


def test_a_call_roundtrips_through_mongo(db):
    spec = CallSpec.new("greet", name="Ada", greeting="Hi")
    db["specs"].insert_one(spec.model_dump())

    assert CallSpec.model_validate(db["specs"].find_one()) == spec


def test_bind_merges_kwargs():
    assert CallSpec.new("sync", full=True).bind(accountId=7).kwargs == {"full": True, "accountId": 7}


def test_bind_overrides_and_leaves_the_original_alone():
    spec = CallSpec.new("sync", accountId=1)

    assert spec.bind(accountId=2).kwargs == {"accountId": 2}
    assert spec.kwargs == {"accountId": 1}


# --- checking a call against its function ---

class Member(BaseModel):
    name: str


class App:
    @task
    @staticmethod
    def greet(name: str, greeting: str = "Hello") -> str:
        return f"{greeting}, {name}!"

    @task
    def whoami(self) -> str:
        return type(self).__name__

    @task
    @staticmethod
    def welcome(member: Member, times: int) -> str:
        return f"{type(member).__name__} {member.name} x{times!r}"

    @task
    @staticmethod
    def flexible(a: int, **extra) -> str:
        return f"{a}:{sorted(extra)}"

    @task
    @staticmethod
    def anything(x):
        return x

    @task
    @staticmethod
    def retain(days: Annotated[int, Field(gt=0)]) -> int:
        return days


@pytest.fixture
def functions() -> Functions:
    app = App()

    return Functions(
        {name: getattr(app, name) for name in declaredOn(App, task)},
        what="task", notFound=TaskNotFound, invalid=TaskValidationError,
    )


def test_building_by_name_and_through_the_class_agree(functions):
    assert functions.build("greet", name="Ada") == App.greet(name="Ada")


def test_an_unknown_function_is_refused(functions):
    with pytest.raises(TaskNotFound, match="does_not_exist"):
        functions.build("does_not_exist")


@pytest.mark.parametrize("kwargs", [
    {},                                         # missing
    {"name": "Ada", "nope": 1},                 # unknown
    {"name": "Ada", "greeting": object()},      # wrongly typed
], ids=["missing", "unknown", "wrongly typed"])
def test_arguments_that_do_not_fit_are_refused(functions, kwargs):
    with pytest.raises(TaskValidationError, match="greet"):
        functions.build("greet", **kwargs)


def test_a_partial_check_takes_the_arguments_given_and_leaves_the_rest(functions):
    call = CallSpec.new("greet", greeting="Hi")

    assert functions.validate(call, complete=False) is call

    with pytest.raises(TaskValidationError):
        functions.validate(call)


@pytest.mark.parametrize("kwargs", [
    {"nope": 1},
    {"greeting": object()},
], ids=["unknown", "wrongly typed"])
def test_a_partial_check_still_refuses_arguments_given_wrong(functions, kwargs):
    with pytest.raises(TaskValidationError, match="greet"):
        functions.validate(CallSpec.new("greet", **kwargs), complete=False)


def test_a_partial_check_refuses_an_unknown_function(functions):
    with pytest.raises(TaskNotFound):
        functions.validate(CallSpec.new("does_not_exist"), complete=False)


def test_an_optional_argument_may_be_omitted_and_the_function_applies_its_default(functions):
    call = functions.build("greet", name="Ada")

    assert call.kwargs == {"name": "Ada"}
    assert functions.call(call) == "Hello, Ada!"


def test_an_instance_task_does_not_expect_self(functions):
    call = functions.build("whoami")

    assert call.kwargs == {}
    assert functions.call(call) == "App"


def test_kwargs_accept_anything_extra(functions):
    assert functions.call(CallSpec.new("flexible", a=1, whatever=2, more=3)) == "1:['more', 'whatever']"


def test_kwargs_still_check_the_named_arguments(functions):
    with pytest.raises(TaskValidationError):
        functions.build("flexible", a="not an int")


def test_an_unannotated_argument_accepts_anything(functions):
    marker = object()

    assert functions.call(functions.build("anything", x=marker)) is marker


def test_an_annotated_constraint_is_enforced(functions):
    assert functions.call(functions.build("retain", days=30)) == 30

    with pytest.raises(TaskValidationError):
        functions.build("retain", days=0)


def test_the_function_receives_models_and_coerced_values_not_stored_data(functions):
    stored = CallSpec.model_validate(CallSpec.new("welcome", member=Member(name="Ada"), times="3").model_dump())

    assert functions.call(stored) == "Member Ada x3"


def test_a_call_is_checked_again_when_it_runs(functions):
    with pytest.raises(TaskValidationError):
        functions.call(CallSpec.new("greet"))


def test_any_parameter_name_is_accepted():
    def odd(_private: int, model_config: str, schema: int = 0):
        return (_private, model_config, schema)

    functions = Functions({"odd": odd})

    assert functions.call(CallSpec.new("odd", _private="1", model_config="x")) == (1, "x", 0)


def test_a_function_taking_positional_arguments_is_refused():
    def positional(*rest):
        return rest

    with pytest.raises(TypeError, match="positionally"):
        Functions({"positional": positional})


def test_functions_know_their_names(functions):
    assert "greet" in functions
    assert "nope" not in functions
    assert set(functions) == {"greet", "whoami", "welcome", "flexible", "anything", "retain"}


# --- reflection ---

def test_nearest_attributes_keep_the_nearest_definition_in_first_defined_order():
    class Base:
        a = "base"
        b = "base"

    class Child(Base):
        b = "child"
        c = "child"

    resolved = {name: value for name, value in nearestAttributes(Child).items() if name in "abc"}

    assert resolved == {"a": "base", "b": "child", "c": "child"}
    assert list(resolved) == ["a", "b", "c"]


def test_uuid4str_is_unique():
    assert uuid4str() != uuid4str()
