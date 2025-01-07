# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
# Copyright (c) 2016-present, Facebook, Inc. All rights reserved.
import inspect
from collections import OrderedDict
from dataclasses import dataclass
from typing import ForwardRef, get_type_hints

from .resolve import VISITED_EDGES_KEY


@dataclass
class ViewModel(OrderedDict):
    """Like an OrderedDict, but treats :id as special for
    equality purposes and hashable (so you can create sets).
    """

    id: int

    IDKEY = ":id"
    TYPE_KEY = ":type"

    def __init__(self, other_dict):
        super().__init__(other_dict)
        self.__visited_edges__ = set()

    def __lt__(self, other):
        return self[ViewModel.IDKEY] < other[ViewModel.IDKEY]

    def __eq__(self, other):
        if ViewModel.IDKEY in self:
            return self[ViewModel.IDKEY] == other[ViewModel.IDKEY]
        else:
            return dict.__eq__(self, other)

    def __repr__(self):
        return dict.__repr__(self)

    def __hash__(self) -> int:
        if ViewModel.IDKEY in self:
            return hash(self[ViewModel.IDKEY])
        else:
            return 0

    def __setitem__(self, name, value):
        if name == "id":
            name = ViewModel.IDKEY
        elif name == "_type":
            name = ViewModel.TYPE_KEY
        elif name == VISITED_EDGES_KEY:
            return
        super().__setitem__(name, value)

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        self.__setitem__(name, value)

    async def resolve_edge(self, edge_name: str, edge_ctx):
        async for i in self.__getattribute__(edge_name)():
            yield i


def node(cls):
    def post_init(self):
        ViewModel.__init__(self, {})

    extra = {"__post_init__": post_init}
    cls = type(cls.__name__, (ViewModel,), {**cls.__dict__, **extra})
    return cls


def edge(fn):
    def decorated(*args, **kwargs):
        function_instance = fn(*args, **kwargs)

        async def inner():
            async for v in function_instance:
                yield v

        return inner()

    decorated._edge = True
    decorated._old = fn
    return decorated


def get_edges(cls):
    return {
        name: f
        for name, f in inspect.getmembers(cls, predicate=inspect.isfunction)
        if hasattr(f, "_edge")
    }


def get_return_type(func):
    ret = get_type_hints(func)["return"]
    if hasattr(ret, "_name") and ret._name == "List":
        ret = ret.__args__[0]
    if isinstance(ret, ForwardRef):
        return ret.__forward_arg__
    if isinstance(ret, type):
        return ret.__name__
    return ret
