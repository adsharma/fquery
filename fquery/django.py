import dataclasses

from django.db import models
from django.db.models.fields import (
    BooleanField,
    IntegerField,
    TextField,
    DateField,
    DateTimeField,
    UUIDField,
    FloatField,
)
from uuid import UUID
from datetime import datetime, date


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
    return make_django_model(cls.__name__, (models.Model,), **django_fields)
