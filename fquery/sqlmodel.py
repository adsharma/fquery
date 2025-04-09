import dataclasses
from dataclasses import _FIELD, dataclass, field, fields, is_dataclass
from datetime import date, datetime, time
from typing import (
    ClassVar,
    Dict,
    ForwardRef,
    List,
    Optional,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

import inflection
from sqlalchemy import (
    JSON,
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
from sqlalchemy.orm.base import Mapped
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
    Dict: JSON,
}

GLOBAL_ID_SEQ = Sequence("global_id_seq")  # define sequence explicitly
SQL_PK = {"metadata": {"SQL": {"primary_key": True}}}


def unique():
    return field(default=None, metadata={"SQL": {"unique": True}})


def foreign_key(name):
    return field(
        default=None,
        metadata={
            "SQL": {"relationship": True, "back_populates": False, "fk_name": name}
        },
    )


def one_to_many(back_populates=None):
    return field(
        default=None,
        metadata={"SQL": {"relationship": True, "back_populates": back_populates}},
    )


def many_to_one(key_column=None, back_populates=None):
    ret = field(
        default=None, metadata={"SQL": {"relationship": True, "many_to_one": True}}
    )
    # if key_column is None, we default to {key_table_name}.id
    if key_column is not None:
        ret.metadata["SQL"]["key_column"] = key_column
    if back_populates is not None:
        ret.metadata["SQL"]["back_populates"] = back_populates
    return ret


def sqlmodel(cls):
    return model()(dataclass(kw_only=True)(cls))


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

    def check_self_reference(clsname: str, field):
        # Check if the field is a self-referential relationship
        if (
            field.type == ForwardRef(clsname)
            or field.type == Optional[ForwardRef(clsname)]
        ):
            return True
        return False

    def get_field_def(cls, field) -> Union[Field, Relationship]:
        sql_meta = field.metadata.get("SQL", {})
        has_foreign_key = bool(sql_meta.get("foreign_key", None))
        has_relationship = bool(sql_meta.get("relationship", None))
        has_unique_constraint = sql_meta.get("unique", False)
        if has_unique_constraint:
            return Field(unique=True)

        if not sql_meta or not (has_foreign_key or has_relationship):
            sql_default_factory = field.default_factory
            if isinstance(sql_default_factory, dataclasses._MISSING_TYPE):
                sql_default_factory = None
            return Field(
                default_factory=sql_default_factory,
                # TODO: revisit the idea of using string for unknown types
                sa_column=Column(
                    SA_TYPEMAP.get(field.type, String),
                    GLOBAL_ID_SEQ if global_id else cls.id_seq,
                    primary_key=(
                        field.name == "id"
                        or field.metadata.get("SQL", {}).get("primary_key", False)
                    ),
                ),
            )
        if has_relationship:
            back_populates = sql_meta.get("back_populates", None)
            if back_populates is False:
                return Relationship()
            if not back_populates:
                back_populates = inflection.underscore(cls.__name__)
                if sql_meta.get("many_to_one", False):
                    back_populates = inflection.pluralize(back_populates)

            key_column = sql_meta.get("key_column", None)
            self_reference = check_self_reference(cls.__name__, field)
            sa_relationship_kwargs = (
                dict(remote_side=key_column) if key_column and self_reference else None
            )
            return Relationship(
                back_populates=back_populates,
                sa_relationship_kwargs=sa_relationship_kwargs,
            )
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
                try:
                    type_class = get_type_hints(cls)[field.name]
                except NameError:
                    # TODO: log exception?
                    pass
                else:
                    return Optional[other_class.__sqlmodel__]
        return field.type

    def patch_back_populates_types(field, back_populates, cls, sqlmodel_cls):
        sql_meta = field.metadata.get("SQL", {})
        has_relationship = bool(sql_meta.get("relationship", None))
        has_many_to_one_relationship = bool(sql_meta.get("many_to_one", None))
        if has_relationship:
            if has_many_to_one_relationship:
                type_class = field.type
                try:
                    type_class = get_type_hints(cls)[field.name]
                except NameError:
                    # TODO: log exception?
                    pass
                inner = type_class.__args__[0]
                if not isinstance(inner, ForwardRef):
                    other_class = inner.__sqlmodel__
                    old = other_class.__annotations__[back_populates]
                    # Should be sqlalchemy.orm.base.Mapped[typing.List[ForwardRef('T')]]
                    # replace it with Mapped[List[sqlmodel_cls]]
                    origin = get_origin(old)
                    inner = get_args(old)
                    if origin == Mapped and len(inner) and get_origin(inner[0]) is list:
                        other_class.__annotations__[back_populates] = Mapped[
                            List[sqlmodel_cls]
                        ]
                        other_class.sqlmodel_rebuild()

        # Replace Optional['T'] with Optional[TSQLModel]
        old = field.type
        origin = get_origin(old)
        inner = get_args(old)
        needs_rebuild = False
        if origin == Union and len(inner) and inner[0] == ForwardRef(cls.__name__):
            sqlmodel_cls.__annotations__[field.name] = Optional[sqlmodel_cls]
            needs_rebuild = True
        if origin == list and len(inner) and inner[0] == ForwardRef(cls.__name__):
            sqlmodel_cls.__annotations__[field.name] = List[sqlmodel_cls]
            needs_rebuild = True

        # Replace Optional[T] with Optional[TSQLModel] if T is a dataclass
        if origin == Union and len(inner) and is_dataclass(inner[0]):
            sqlmodel_cls.__annotations__[field.name] = Optional[inner[0].__sqlmodel__]
            needs_rebuild = True

        if needs_rebuild:
            sqlmodel_cls.sqlmodel_rebuild()

    def default_table_name(clsname: str) -> str:
        return inflection.underscore(inflection.pluralize(clsname))

    def decorator(cls):
        # Check if the class is a dataclass
        if not is_dataclass(cls):
            raise ValueError("The class must be a dataclass")

        nonlocal table_name
        table_name = table_name or default_table_name(cls.__name__)

        if not global_id:
            cls.id_seq = Sequence(f"{table_name}_seq")

        # Insert any foreign keys as necessary
        for cfield in fields(cls):
            sql_meta = cfield.metadata.get("SQL", {})
            has_relationship = bool(sql_meta.get("relationship", None))
            if has_relationship:
                many_to_one = sql_meta.get("many_to_one", False)
                foreign_key_name = cfield.name + "_id"
                key_table_name = table_name
                key_column_name = sql_meta.get("fk_name", f"{key_table_name}.id")
                if many_to_one:
                    type_class = cfield.type
                    other_class = type_class.__args__[0]
                    if isinstance(other_class, ForwardRef):
                        other_class = other_class.__forward_arg__
                    other_class = getattr(other_class, "__name__", other_class)
                    key_table_name = default_table_name(other_class)
                    key_column_name = sql_meta.get(
                        "key_column_name", f"{key_table_name}.id"
                    )
                back_populates = sql_meta.get("back_populates", None)
                if back_populates is False or many_to_one:

                    new_field = field(
                        default=None,
                        metadata={"SQL": {"foreign_key": key_column_name}},
                    )
                    new_field._field_type = _FIELD
                    new_field.name = foreign_key_name
                    new_field.type = Optional[int]
                    cls.__dataclass_fields__[foreign_key_name] = new_field
                    setattr(cls, new_field.name, new_field.default)

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
        # Update type annotations in any class with a relationship with this class to point
        # to the SQLModel, not the dataclass
        for cfield in fields(cls):
            if not cfield.name in sqlmodel_cls.__sqlmodel_relationships__:
                continue
            rel = sqlmodel_cls.__sqlmodel_relationships__.get(cfield.name, None)
            if rel and hasattr(rel, "back_populates"):
                patch_back_populates_types(
                    cfield, rel.back_populates, cls, sqlmodel_cls
                )
        cls.sqlmodel = sqlmodel
        return cls

    return decorator
