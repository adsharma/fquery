# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import random

from dataclasses import dataclass
from query import Query, QueryableOp
from typing import List
from view_model import ViewModel


def edge(func):
    return func


@dataclass
class MockUser(ViewModel):
    name: str
    age: int

    @edge
    async def friends(self) -> List["MockUser"]:
        yield [MockUser.get(m) for m in range(3 * self.id, 3 * self.id + 3)]

    @staticmethod
    def get(id):
        # A typical implementation may fetch fields from a database
        # based on self.id here
        u = MockUser(id=id, name=f"id{id}", age=random.choice([16, 17, 18]))
        u._type = random.choice([1, 2])
        return u


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
            yield {str(None): [MockUser.get(i) for i in self.ids]}
        else:
            async for item in self._items:
                async for i in item.friends():
                    yield i

UserQuery.EDGE_NAME_TO_QUERY_TYPE = { "friends" : UserQuery }
