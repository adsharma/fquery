# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import random

from view_model import ViewModel
from query import Query, QueryableOp
from walk import ViewModel

class MockUser(ViewModel):
    def __init__(self, id):
        super().__init__()
        self.id = id

    def get(self):
        id = self.id
        return ViewModel(
            {
                ":id": id,
                "name": "id%d" % id,
                ":type": random.choice([1, 2]),
                "age": random.choice([16, 17, 18]),
            }
        )


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
