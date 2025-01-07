# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
# Copyright (c) 2016-present, Facebook, Inc. All rights reserved.
import sys
from enum import Enum

import strawberry


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
