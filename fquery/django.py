import dataclasses
from datetime import date, datetime
from uuid import UUID

from django.db import models
from django.db.models.fields import (
    BooleanField,
    DateField,
    DateTimeField,
    FloatField,
    IntegerField,
    TextField,
    UUIDField,
)
from django.db.models.fields.related import ForeignKey

from .view_model import get_edges, get_return_type


def model(cls):
    def make_django_model(name, bases, **kwattrs):
        return type(name, bases, dict(**kwattrs))

    def map_type(dataclass_field):
        DATACLASS_TO_DJANGO_FIELD = {
            int: IntegerField,
            float: FloatField,
            bool: BooleanField,
            # TODO: Figure out how to expose CharField
            str: TextField,
            datetime: DateTimeField,
            date: DateField,
            UUID: UUIDField,
        }
        return DATACLASS_TO_DJANGO_FIELD.get(dataclass_field, TextField)()

    fields = dataclasses.fields(cls)
    django_fields = {djf.name: map_type(djf.type) for djf in fields if djf.name != "id"}
    django_fields["__module__"] = cls.__module__
    django_foreign_key_funcs = get_edges(cls)
    for name, f in django_foreign_key_funcs.items():
        ret_type = get_return_type(f._old)
        name = "_" + name  # so it doesn't conflict with the async method name
        django_fields[name] = ForeignKey(ret_type, on_delete=models.CASCADE)
    return make_django_model(cls.__name__, (models.Model,), **django_fields)
