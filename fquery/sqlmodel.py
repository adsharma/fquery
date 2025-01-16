from dataclasses import fields, is_dataclass
from datetime import date, datetime, time
from typing import ClassVar, ForwardRef, List, Optional, Union, get_type_hints

import inflection
from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    LargeBinary,
    Sequence,
    String,
    Time,
)
from sqlmodel import Column, Field, Relationship, SQLModel

SA_TYPEMAP = {
    int: Integer,
    int | None: Integer,
    float: Float,
    str: String,
    bool: Boolean,
    datetime: DateTime,
    date: Date,
    time: Time,
    bytes: LargeBinary,  # or Binary for smaller data
}

GLOBAL_ID_SEQ = Sequence("global_id_seq")  # define sequence explicitly
SQL_PK = {"metadata": {"SQL": {"primary_key": True}}}


def model(table: bool = True, table_name: str = None, global_id: bool = False):
    """
    A decorator that generates a SQLModel from a dataclass.

    Args:
        table_name (str): The name of the database table. Defaults to the name of the dataclass.

    Returns:
        A decorator that generates a SQLModel from a dataclass.
    """

    def sqlmodel(self) -> SQLModel:
        attrs = {name: getattr(self, name) for name in self.__sqlmodel__.__fields__}
        return self.__sqlmodel__(**attrs)

    def get_field_def(cls, field) -> Union[Field, Relationship]:
        sql_meta = field.metadata.get("SQL", {})
        has_foreign_key = bool(sql_meta.get("foreign_key", None))
        has_relationship = bool(sql_meta.get("relationship", None))

        if not sql_meta or not (has_foreign_key or has_relationship):
            return Field(
                default_factory=getattr(cls, field.name, None),
                # TODO: revisit the idea of using string for unknown types
                sa_column=Column(
                    SA_TYPEMAP.get(field.type, String),
                    GLOBAL_ID_SEQ if global_id else None,
                    primary_key=(
                        field.name == "id"
                        or field.metadata.get("SQL", {}).get("primary_key", False)
                    ),
                ),
            )
        if has_relationship:
            back_populates = sql_meta.get("back_populates", None)
            if not back_populates:
                back_populates = inflection.underscore(cls.__name__)
                if sql_meta.get("many_to_one", False):
                    back_populates = inflection.pluralize(back_populates)
            return Relationship(back_populates=back_populates)
        if has_foreign_key:
            return Field(default=None, foreign_key=sql_meta["foreign_key"])
        raise "Unsupported case"

    def get_field_type(field, cls):
        sql_meta = field.metadata.get("SQL", {})
        has_foreign_key = bool(sql_meta.get("foreign_key", None))
        has_relationship = bool(sql_meta.get("relationship", None))
        has_many_to_one_relationship = bool(sql_meta.get("many_to_one", None))
        if has_foreign_key:
            # Translate ClassName to id type.
            # TODO: what if the id type is different?
            return Optional[int]
        if has_relationship:
            type_class = field.type
            other_class = type_class.__args__[0]
            if has_many_to_one_relationship:
                type_class = get_type_hints(cls)[field.name]
                return Optional[other_class.__sqlmodel__]
            if isinstance(other_class, ForwardRef):
                other_class = other_class.__forward_arg__
            if isinstance(other_class, str):
                return List[Optional[other_class + "SQLModel"]]
        return field.type

    def decorator(cls):
        # Check if the class is a dataclass
        if not is_dataclass(cls):
            raise ValueError("The class must be a dataclass")

        nonlocal table_name
        table_name = table_name or inflection.underscore(
            inflection.pluralize(cls.__name__)
        )

        # Generate the SQLModel class
        sqlmodel_cls = type(
            cls.__name__ + "SQLModel",
            (SQLModel,),
            {
                # Table name is a plural and hence the 's' at the end
                "__tablename__": table_name,
                # Add type annotations to the generated fields
                "__annotations__": {
                    **{field.name: get_field_type(field, cls) for field in fields(cls)},
                    **{
                        "Config": ClassVar,
                    },
                },
                # pydantic wants this
                "__module__": cls.__module__,
                "Config": {"exclude": {"__sqlmodel__", "sqlmodel"}},
                **{field.name: get_field_def(cls, field) for field in fields(cls)},
            },
            # For SQLModel's SQLModelMetaClass
            table=table,
        )

        cls.__sqlmodel__ = sqlmodel_cls
        cls.sqlmodel = sqlmodel
        return cls

    return decorator
