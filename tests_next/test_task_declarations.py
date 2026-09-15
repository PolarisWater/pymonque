"""@task: declaring a function as a task, the limits it runs under, and what it gives through the class
and through an instance."""

import pytest

from pymonque_next import AppDefaults, CallSpec, FuncSpec, Task, TaskLimits, task
from pymonque_next.declarations import declaredOn


class App:
    @task
    @staticmethod
    def greet(name: str, greeting: str = "Hello") -> str:
        return f"{greeting}, {name}!"

    @task(timeout=5)
    def whoami(self) -> str:
        return type(self).__name__

    @task(timeout=None, skipAfter=None)
    @classmethod
    def kind(cls) -> str:
        return cls.__name__

    def helper(self) -> int:
        return 1


DEFAULTS = AppDefaults(taskTimeout=300, taskSkipAfter=60)


def declared(name: str) -> task:
    return declaredOn(App, task)[name]


# --- limits ---

def test_a_bare_task_takes_the_app_defaults():
    assert declared("greet").limitsWith(DEFAULTS) == TaskLimits(timeout=300, skipAfter=60)


def test_a_declared_limit_wins_and_the_rest_are_the_defaults():
    assert declared("whoami").limitsWith(DEFAULTS) == TaskLimits(timeout=5, skipAfter=60)


def test_none_means_no_limit_whatever_the_default():
    assert declared("kind").limitsWith(DEFAULTS) == TaskLimits(timeout=None, skipAfter=None)


def test_without_defaults_a_task_has_no_limits():
    assert declared("greet").limitsWith(AppDefaults()) == TaskLimits()


# --- declaring ---

def test_both_forms_declare_a_task():
    assert set(declaredOn(App, task)) == {"greet", "whoami", "kind"}


def test_a_plain_method_is_not_a_task():
    assert "helper" not in declaredOn(App, task)


def test_a_task_is_declared_once():
    def job() -> None:
        return None

    declaration = task(timeout=1)
    declaration(staticmethod(job))

    with pytest.raises(TypeError, match="already declared"):
        declaration(staticmethod(job))


def test_limits_without_a_function_are_refused():
    with pytest.raises(TypeError, match="no function"):
        class Broken:
            limits = task(timeout=1)


def test_a_task_engine_is_not_declared_with_task():
    with pytest.raises(TypeError, match=r"tasks\(\.\.\.\)"):
        task(Task)


# --- through the class, and through an instance ---

def test_class_access_gives_a_funcspec_that_builds_the_call():
    assert isinstance(App.greet, FuncSpec)
    assert App.greet(name="Ada") == CallSpec.new("greet", name="Ada")


def test_the_call_is_named_after_the_attribute():
    def job() -> None:
        return None

    class Named:
        alias = task(staticmethod(job))

    assert Named.alias().functionName == "alias"


def test_instance_access_runs_a_staticmethod_task():
    assert App().greet(name="Ada") == "Hello, Ada!"


def test_instance_access_binds_an_instance_task():
    assert App().whoami() == "App"


def test_a_classmethod_task_is_bound_to_the_instances_class():
    class Child(App):
        pass

    assert Child().kind() == "Child"


# --- keyword arguments only ---

def test_star_args_are_refused_where_they_are_written():
    with pytest.raises(TypeError, match="positionally"):
        class Star:
            @task
            @staticmethod
            def positional(*rest) -> None:
                return None


def test_positional_only_arguments_are_refused_where_they_are_written():
    with pytest.raises(TypeError, match="positionally"):
        class PositionalOnly:
            @task
            @staticmethod
            def fixed(a, /) -> None:
                return None


def test_self_may_be_positional_only():
    class Fine:
        @task
        def ok(self, /, x: int) -> int:
            return x

    assert Fine().ok(x=1) == 1


def test_an_instance_task_without_self_is_refused():
    with pytest.raises(TypeError, match="self"):
        class NoSelf:
            @task
            def broken() -> None:
                return None


# --- discovery ---

def test_a_subclass_overrides_a_task_and_a_plain_method_unregisters_one():
    class Child(App):
        @task
        @staticmethod
        def greet(name: str) -> str:
            return f"override {name}"

        def whoami(self) -> str:
            return "plain"

    found = declaredOn(Child, task)

    assert set(found) == {"greet", "kind"}
    assert found["greet"] is Child.__dict__["greet"]
    assert Child().greet(name="Ada") == "override Ada"
