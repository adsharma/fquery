from dataclasses import fields, is_dataclass
from datetime import date, datetime, time
from typing import ClassVar

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    String,
    Time,
)
from sqlmodel import Column, Field, SQLModel

SA_TYPEMAP = {
    int: Integer,
    float: Float,
    str: String,
    bool: Boolean,
    datetime: DateTime,
    date: Date,
    time: Time,
    bytes: LargeBinary,  # or Binary for smaller data
}


def model(table_name: str = None):
    """
    A decorator that generates a SQLModel from a dataclass.

    Args:
        table_name (str): The name of the database table. Defaults to the name of the dataclass.

    Returns:
        A decorator that generates a SQLModel from a dataclass.
    """

    def sqlmodel(self) -> SQLModel:
        return self.__sqlmodel__(**self.__dict__)

    def decorator(cls):
        # Check if the class is a dataclass
        if not is_dataclass(cls):
            raise ValueError("The class must be a dataclass")

        class Config:
            exclude = {"__sqlmodel__", "sqlmodel"}

        Config.ignored_types = (Config,)

        # Generate the SQLModel class
        sqlmodel_cls = type(
            cls.__name__ + "SQLModel",
            (SQLModel,),
            {
                # Table name is a plural and hence the 's' at the end
                "__tablename__": table_name or cls.__name__.lower() + "s",
                # Add type annotations to the generated fields
                "__annotations__": {
                    **{field.name: field.type for field in fields(cls)},
                    **{
                        "Config": ClassVar,
                    },
                },
                # pydantic wants this
                "__module__": cls.__module__,
                "Config": Config,
                **{
                    field.name: Field(
                        default_factory=getattr(cls, field.name, None),
                        # TODO: revisit the idea of using string for unknown types
                        sa_column=Column(
                            SA_TYPEMAP.get(field.type, String),
                            primary_key=field.name == "id",
                        ),
                    )
                    for field in fields(cls)
                },
            },
            # For SQLModel's SQLModelMetaClass
            table=True,
        )

        cls.__sqlmodel__ = sqlmodel_cls
        cls.sqlmodel = sqlmodel
        return cls

    return decorator