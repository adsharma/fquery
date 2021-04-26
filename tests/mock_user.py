# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
from __future__ import annotations

import random

from dataclasses import dataclass
from typing import List, Optional

from fquery.query import query, Query, QueryableOp
from fquery.view_model import edge, node, ViewModel


@dataclass
@node
class MockUser:
    name: str
    age: int

    @property
    async def async_age(self) -> int:
        return self.age

    @edge
    async def friends(self) -> List["MockUser"]:
        yield [MockUser.get(m) for m in range(3 * self.id, 3 * self.id + 3)]

    @edge
    async def reviews(self) -> List["MockReview"]:
        yield [
            MockReview.get(m) for m in range(3 * self.id + 300, 3 * self.id + 300 + 5)
        ]

    @staticmethod
    def get(id: int) -> "MockUser":
        # A typical implementation may fetch fields from a database
        # based on self.id here
        u = MockUser(id=id, name=f"id{id}", age=random.choice([16, 17, 18]))
        u._type = 1
        return u


@query
class UserQuery:
    OP = QueryableOp.LEAF
    TYPE = MockUser

    def __init__(self, ids=None, items=None):
        Query.__init__(self, None, ids, items)

    @staticmethod
    def resolve_obj(_id: int, edge: str = "") -> Optional[ViewModel]:
        return MockUser.get(_id)


@dataclass
@node
class MockReview:
    business: str
    rating: int

    @edge
    async def author(self) -> MockUser:
        # TODO: Figure out how to make this work for relational vs graph
        self._author = 1
        yield MockUser.get(self._author)

    @staticmethod
    def get(id: int) -> "MockReview":
        # A typical implementation may fetch fields from a database
        # based on self.id here
        r = MockReview(
            id=id, business=f"business{id}", rating=random.choice(range(1, 6))
        )
        r._type = 2
        return r


@query
class ReviewQuery:
    OP = QueryableOp.LEAF
    TYPE = MockReview

    def __init__(self, ids=None, items=None):
        Query.__init__(self, None, ids, items)

    @staticmethod
    def resolve_obj(_id: int, edge: str = "") -> Optional[ViewModel]:
        return MockReview.get(_id)
