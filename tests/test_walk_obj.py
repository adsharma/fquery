# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import unittest

from .async_test import async_test
from .mock_user import UserQuery


class MaterializeWalkObjTests(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

    @async_test
    async def test_as_list(self):
        actual = await UserQuery(range(1, 5)).as_list()
        self.assertEqual(4, len(actual))
        self.assertEqual([f"id{i}" for i in range(1, 5)], [u.name for u in actual])

    @async_test
    async def test_as_dict(self):
        actual = await UserQuery(range(1, 5)).as_dict()
        for i in range(1, 5):
            self.assertEqual(actual[str(i)].id, i)
            self.assertEqual(actual[str(i)].name, f"id{i}")

    @async_test
    async def test_as_list_with_edges(self):
        actual = await UserQuery(range(1, 5)).edge("reviews").take(2).as_list()
        self.assertEqual(4, len(actual))
        self.assertEqual(2, len(actual[0].reviews))
        self.assertEqual([f"id{i}" for i in range(1, 5)], [u.name for u in actual])

    @async_test
    async def test_as_list_with_two_edges(self):
        actual = (
            await UserQuery(range(1, 5))
            .edge("friends")
            .take(1)
            .parent()
            .parent()
            .edge("reviews")
            .take(2)
            .as_list()
        )
        self.assertEqual(4, len(actual))
        self.assertEqual(2, len(actual[0].reviews))
        self.assertEqual(1, len(actual[0].friends))
        self.assertEqual([f"id{i}" for i in range(1, 5)], [u.name for u in actual])


if __name__ == "__main__":
    unittest.main()
