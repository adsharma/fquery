import dataclasses
from dataclasses import dataclass, fields
from typing import Type

from pydantic import BaseModel, ConfigDict, Field


def pydantic(cls):
    return model(dataclass(kw_only=True)(cls))


def validator(self) -> BaseModel:
    attrs = {name: getattr(self, name) for name in self.__pydantic__.model_fields}
    return self.__pydantic__(**attrs)


def get_field_def(cls, field):
    # if the dataclass has a default_factory, or a default value, use it in pydantic Field
    kwargs = {}
    if not isinstance(field.default, dataclasses._MISSING_TYPE):
        kwargs["default"] = field.default
    if not isinstance(field.default_factory, dataclasses._MISSING_TYPE):
        kwargs["default_factory"] = field.default_factory
    return Field(**kwargs)


def model(cls: Type) -> Type:
    """
    Decorator to convert a dataclass to a Pydantic model.
    """
    # Generate the SQLModel class
    pydantic_cls = type(
        cls.__name__ + "Model",
        (BaseModel,),
        {
            # Add type annotations to the generated fields
            "__annotations__": {**{field.name: field.type for field in fields(cls)}},
            # Actual field defs
            **{field.name: get_field_def(cls, field) for field in fields(cls)},
        },
    )
    cls.__pydantic__ = pydantic_cls
    cls.model_config = ConfigDict(extra="ignore")
    cls.validator = validator

    return cls
