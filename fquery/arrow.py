import types
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import (
    Any,
    Dict,
    List,
    Tuple,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

import pyarrow as pa

UNION_TYPES = (Union,)
if hasattr(types, "UnionType"):
    UNION_TYPES = UNION_TYPES + (types.UnionType,)


TYPEMAP = {
    bool: pa.bool_(),
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bytes: pa.binary(),
    datetime: pa.timestamp("us"),
    date: pa.date32(),
    time: pa.time64("us"),
    Decimal: pa.decimal128(38, 9),
}


def arrow(cls):
    return model(dataclass(kw_only=True)(cls))


def table(cls, rows) -> pa.Table:
    return pa.Table.from_pylist(
        [_record(cls, row) for row in rows], schema=cls.__arrow__
    )


def _record(cls: Type, row) -> Dict[str, Any]:
    field_names = [field.name for field in fields(cls)]
    if is_dataclass(row):
        return {name: getattr(row, name) for name in field_names}
    if isinstance(row, Mapping):
        return {name: row[name] for name in field_names}
    raise TypeError(f"Expected {cls.__name__} instance or mapping, got {type(row)!r}")


def _is_optional(origin, args) -> bool:
    return origin in UNION_TYPES and type(None) in args


def _arrow_type(annotation) -> Tuple[pa.DataType, bool]:
    if isinstance(annotation, pa.DataType):
        return annotation, False

    origin = get_origin(annotation)
    args = get_args(annotation)

    if _is_optional(origin, args):
        non_none = [arg for arg in args if arg is not type(None)]
        if len(non_none) != 1:
            raise TypeError(f"Unsupported union type {annotation!r}")
        arrow_type, _ = _arrow_type(non_none[0])
        return arrow_type, True

    if origin in (list, List):
        if len(args) != 1:
            raise TypeError(f"List fields must specify one item type: {annotation!r}")
        item_type, _ = _arrow_type(args[0])
        return pa.list_(item_type), False

    if origin in (dict, Dict):
        if len(args) != 2:
            raise TypeError(
                f"Dict fields must specify key and value types: {annotation!r}"
            )
        key_type, _ = _arrow_type(args[0])
        item_type, _ = _arrow_type(args[1])
        return pa.map_(key_type, item_type), False

    try:
        return TYPEMAP[annotation], False
    except KeyError:
        raise TypeError(f"Unsupported Arrow field type {annotation!r}") from None


def _nullable(field, annotation_nullable: bool) -> bool:
    if annotation_nullable:
        return True
    return field.default is None


def _schema_field(field, annotation) -> pa.Field:
    arrow_type, annotation_nullable = _arrow_type(annotation)
    return pa.field(
        field.name, arrow_type, nullable=_nullable(field, annotation_nullable)
    )


def model(cls: Type) -> Type:
    """
    Decorator to convert a dataclass to a PyArrow-backed row type.
    """
    type_hints = get_type_hints(cls)
    cls.__arrow__ = pa.schema(
        [
            _schema_field(field, type_hints.get(field.name, field.type))
            for field in fields(cls)
        ]
    )
    cls.to_arrow = classmethod(table)
    return cls
