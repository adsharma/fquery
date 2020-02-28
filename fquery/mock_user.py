# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import random

from dataclasses import dataclass
from query import Query, QueryableOp
from view_model import ViewModel


@dataclass
class MockUserBase(ViewModel):
    name: str
    age: int


class MockUser(MockUserBase):
    def __init__(self, id):
        ViewModel().__init__()
        self.id = id

    def __setattr__(self, name, value):
        super().__setattr__(name, value)
        ViewModel.__setitem__(self, name, value)

    def get(self):
        # A typical implementation may fetch fields from a database
        # based on self.id here
        self.name = f"id{self.id}"
        self._type = random.choice([1, 2])
        self.age = random.choice([16, 17, 18])
        return self


class UserQuery(Query):
    OP = QueryableOp.LEAF

    def __init__(self, ids=None, items=None):
        super().__init__(None, ids, items)

    def edge(self, edge_name):
        if edge_name == "friends":
            self._unbound_class = UserQuery
        return super(UserQuery, self).edge(edge_name)

    async def iter(self):
        if not self.parent_edge:
            yield {str(None): [MockUser(i).get() for i in self.ids]}
        else:
            async for item in self._items:
                yield [
                    MockUser(m).get()
                    for m in range(3 * item[":id"], 3 * item[":id"] + 3)
                ]
