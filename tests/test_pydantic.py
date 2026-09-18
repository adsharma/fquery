from dataclasses import is_dataclass
from typing import Optional

import pytest
from pydantic import BaseModel, ValidationError

from fquery.pydantic import pydantic


@pydantic
class User:
    name: str
    age: int
    is_active: bool = True


def test_pydantic():
    u1 = User(name="John Doe", age=42)
    u2 = User(name="John Doe", age=42, is_active=False)
    assert is_dataclass(u1)
    assert is_dataclass(u2)

    v1 = u1.validator()
    v2 = u2.validator()
    assert isinstance(v1, BaseModel)
    assert isinstance(v2, BaseModel)

    assert v1.model_dump() == u1.__dict__
    assert v2.model_dump() == u2.__dict__


def test_pydantic_fail():
    u1 = User(name="John Doe", age=42.3)
    with pytest.raises(ValidationError):
        _ = u1.validator()


def test_pydantic_namespace_hook_static():
    # Forward ref resolved from a static mapping, no import of the
    # target module needed at class-definition time.
    @pydantic(namespace={"Address": Address})  # noqa: F821
    class Customer:
        name: str
        address: Optional["Address"] = None  # noqa: F821

    c = Customer(name="x")
    assert isinstance(c.validator(), BaseModel)


def test_pydantic_namespace_hook_callable():
    # Callable receives the decorated class; inherited when not overridden.
    seen = []

    def provider(cls):
        seen.append(cls.__name__)
        return {"Address": Address}

    @pydantic(namespace=provider)
    class Order:
        ref: Optional["Address"] = None  # noqa: F821

    assert isinstance(Order().validator(), BaseModel)
    assert seen == ["Order"]


@pydantic
class Address:
    street: str = ""
