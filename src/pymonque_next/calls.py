"""Calls stored as data: CallSpec, what a function's signature accepts, and checking a call against it
when the call is built or stored and again when it runs."""

from __future__ import annotations

import inspect
from typing import Any, Callable, Iterator, Mapping, NotRequired, Required, TypedDict, TypeVar, get_type_hints, overload

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError, with_config

from .documents import MONGO_CONFIG


T = TypeVar("T")


def nearestAttributes(cls: type) -> dict[str, Any]:
    """Every name defined on a class or its parents, resolved to the definition that wins — so a
    subclass that redefines a name as something else replaces it, rather than leaving the parent's
    beside it. Kept in the order names were first defined."""

    resolved: dict[str, Any] = {}

    for base in reversed(cls.__mro__):  # furthest first, so nearer definitions overwrite
        resolved.update(base.__dict__)

    return resolved


class CallSpec(BaseModel):
    """A call of a named function with keyword arguments: all a stored task or scheduler knows about
    the work it does."""

    model_config = MONGO_CONFIG

    functionName:   str
    kwargs:         dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def new(cls, functionName: str, /, **kwargs: Any) -> CallSpec:
        return cls(functionName=functionName, kwargs=kwargs)

    @overload
    def __call__(self, source: Mapping[str, Callable[..., T]]) -> T: ...

    @overload
    def __call__(self, source: object) -> Any: ...

    def __call__(self, source: Any) -> Any:
        """Call the function of this name in a mapping, or on an object, as it is — unchecked."""

        func = source.get(self.functionName) if isinstance(source, Mapping) else getattr(source, self.functionName, None)

        if func is None:
            raise KeyError(f"{source!r} has no function {self.functionName}")

        return func(**self.kwargs)

    def bind(self, **kwargs: Any) -> CallSpec:
        """A copy of this call with kwargs merged in; the original is untouched."""

        return CallSpec(functionName=self.functionName, kwargs={**self.kwargs, **kwargs})

    def __repr__(self) -> str:
        return f"{self.functionName}({self.kwargs})"


class FuncSpec:
    """What reaching a task through the app class gives: calling it builds the call that names the task.

        App.sync(accountId=7)       # -> sync({'accountId': 7})
    """

    def __init__(self, name: str, func: Callable):
        self.name = name
        self.func = func

    def __call__(self, **kwargs: Any) -> CallSpec:
        return CallSpec.new(self.name, **kwargs)

    def __repr__(self) -> str:
        return f"FuncSpec {self.name}"


def refusePositional(func: Callable, *, bound: bool = False, name: str | None = None) -> None:
    """Refuse a function a CallSpec could never fill in.

    A CallSpec is {functionName, kwargs}, so a parameter that can only be passed by position could
    never be given. Say so where the function is declared, not on every call that tries to use it.
    `bound` skips the `self` or `cls` a method is bound to.
    """

    label = name or getattr(func, "__name__", repr(func))
    parameters = list(inspect.signature(func).parameters.values())

    if bound:
        if not parameters or parameters[0].kind is inspect.Parameter.VAR_POSITIONAL:
            raise TypeError(f"{label} takes no self or cls to be bound to")

        parameters = parameters[1:]

    for parameter in parameters:
        if parameter.kind is parameter.VAR_POSITIONAL:
            raise TypeError(f"{label} takes *{parameter.name} positionally; a stored call passes keyword arguments only")

        if parameter.kind is parameter.POSITIONAL_ONLY:
            raise TypeError(f"{label} takes {parameter.name} positionally; a stored call passes keyword arguments only")


def argumentsOf(func: Callable, *, complete: bool = True) -> TypeAdapter[dict[str, Any]]:
    """What a call of `func` may pass: each named parameter as its annotation declares it — anything
    if unannotated, required unless it has a default — and other names only if it takes **kwargs.
    With `complete=False` nothing is required: the arguments given are checked, and more may follow.

    Arguments left out stay left out, so the function applies its own defaults.
    """

    refusePositional(func)

    hints = get_type_hints(func, include_extras=True)
    fields: dict[str, Any] = {}
    extra = "forbid"

    for name, parameter in inspect.signature(func).parameters.items():
        if parameter.kind is parameter.VAR_KEYWORD:
            extra = "allow"     # whatever else is passed is the function's business
            continue

        annotation = hints.get(name, Any)
        required = complete and parameter.default is parameter.empty
        fields[name] = Required[annotation] if required else NotRequired[annotation]

    # a TypedDict rather than a model, so a parameter may have any name a model field could not
    arguments = TypedDict(f"{getattr(func, '__name__', 'call')}Arguments", fields)  # type: ignore[misc]

    return TypeAdapter(with_config(ConfigDict(extra=extra, arbitrary_types_allowed=True))(arguments))


class Functions:
    """Named functions a CallSpec may call, each checked against its signature.

    The app's tasks are one set, a distribution registry another. A call is checked when it is built
    or stored, so a bad one is never stored, and again when it runs, so the function receives what
    its annotations promise: a model argument as the model rather than the dict it was stored as, and
    a value pydantic coerced ("3" for an int) as the coerced value.
    """

    def __init__(
            self,
            functions:  Mapping[str, Callable],
            *,
            what:       str = "function",
            notFound:   type[Exception] = LookupError,
            invalid:    type[Exception] = ValueError
        ):

        self.functions: dict[str, Callable] = dict(functions)
        self.what = what
        self.notFound = notFound
        self.invalid = invalid

        self.arguments: dict[str, TypeAdapter[dict[str, Any]]] = {
            name: argumentsOf(func) for name, func in self.functions.items()
        }

        self.given: dict[str, TypeAdapter[dict[str, Any]]] = {
            name: argumentsOf(func, complete=False) for name, func in self.functions.items()
        }

    def __contains__(self, name: object) -> bool:
        return name in self.functions

    def __iter__(self) -> Iterator[str]:
        return iter(self.functions)

    def __len__(self) -> int:
        return len(self.functions)

    def _checked(self, call: CallSpec, complete: bool = True) -> dict[str, Any]:
        arguments = (self.arguments if complete else self.given).get(call.functionName)

        if arguments is None:
            raise self.notFound(f"there is no {self.what} {call.functionName}")

        try:
            return arguments.validate_python(dict(call.kwargs))
        except ValidationError as e:
            raise self.invalid(f"{call!r} does not fit {self.what} {call.functionName}: {e}") from e

    def validate(self, call: CallSpec, *, complete: bool = True) -> CallSpec:
        """Raise unless the call fits its function; return it unchanged. With `complete=False` only the
        arguments given are checked, for a call that something adds to before it runs."""

        self._checked(call, complete)

        return call

    def build(self, functionName: str, /, **kwargs: Any) -> CallSpec:
        """A call of the function of this name, checked."""

        return self.validate(CallSpec.new(functionName, **kwargs))

    def call(self, call: CallSpec) -> Any:
        """Run the call, its arguments checked and coerced into the types the function declares."""

        arguments = self._checked(call)

        return self.functions[call.functionName](**arguments)
