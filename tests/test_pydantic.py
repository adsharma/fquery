from dataclasses import is_dataclass

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
