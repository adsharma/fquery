# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import random

from dataclasses import dataclass
from query import Query, QueryableOp
from typing import List, Optional
from view_model import edge, ViewModel


@dataclass
class MockUser(ViewModel):
    name: str
    age: int

    @edge
    async def friends(self) -> List["MockUser"]:
        yield [MockUser.get(m) for m in range(3 * self.id, 3 * self.id + 3)]

    @staticmethod
    def get(id: int) -> "MockUser":
        # A typical implementation may fetch fields from a database
        # based on self.id here
        u = MockUser(id=id, name=f"id{id}", age=random.choice([16, 17, 18]))
        return u


class UserQuery(Query):
    OP = QueryableOp.LEAF

    def __init__(self, ids=None, items=None):
        super().__init__(None, ids, items)

    @staticmethod
    def resolve_obj(_id: int, edge: str = "") -> Optional[ViewModel]:
        return MockUser.get(_id)


# TODO: Handle this via @edge decorator
UserQuery.EDGE_NAME_TO_QUERY_TYPE = {"friends": UserQuery}
