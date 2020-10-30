import strawberry
import sys

from enum import Enum


def graphql(cls):
    if Enum in cls.__mro__:
        return strawberry.enum(cls)
    return strawberry.type(cls)


def root(cls):
    cls_module = sys.modules[cls.__module__]
    cls_module.schema = strawberry.Schema(query=cls)
    return cls


def obj(cls):
    return strawberry.type(cls)


def field(func):
    return strawberry.field(func)
