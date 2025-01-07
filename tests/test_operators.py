# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import ast
import os
import random
import unittest

from .async_test import async_test
from .mock_user import UserQuery

TEST_DATA = os.path.join(os.path.dirname(__file__), "test_data")


class QueryTests(unittest.TestCase):
    def setUp(self):
        random.seed(100)
        self.maxDiff = None

    @async_test
    async def test_project(self):
        resp = await UserQuery(range(1, 10)).project(["name", ":id"]).take(3).to_json()
        expected = [
            {
                "None": [
                    {"name": "id1", ":id": 1},
                    {"name": "id2", ":id": 2},
                    {"name": "id3", ":id": 3},
                ]
            }
        ]
        self.assertEqual(expected, resp)

    def test_sync_project(self):
        resp = UserQuery(range(1, 10)).project(["name", ":id"]).take(3).to_json().send()
        expected = [
            {
                "None": [
                    {"name": "id1", ":id": 1},
                    {"name": "id2", ":id": 2},
                    {"name": "id3", ":id": 3},
                ]
            }
        ]
        self.assertEqual(expected, resp)

    @async_test
    async def test_edge_project(self):
        resp = (
            await UserQuery(range(1, 5))
            .edge("friends")
            .project(["name", ":id"])
            .take(3)
            .to_json()
        )
        with open(os.path.join(TEST_DATA, "test_data_edge_project.txt")) as f:
            expected = ast.literal_eval(f.read())
        self.assertEqual(expected, resp)
        self.assertEqual(str(expected), str(resp))

    @async_test
    async def test_edge_project_other_kind(self):
        resp = (
            await UserQuery(range(1, 5))
            .edge("reviews")
            .project(["business", "rating", ":id"])
            .take(3)
            .to_json()
        )
        with open(
            os.path.join(TEST_DATA, "test_data_edge_project_other_kind.txt")
        ) as f:
            expected = ast.literal_eval(f.read())
        self.assertEqual(expected, resp)
        self.assertEqual(str(expected), str(resp))

    def test_sync_edge_project(self):
        resp = (
            UserQuery(range(1, 5))
            .edge("friends")
            .project(["name", ":id"])
            .take(3)
            .to_json()
            .send()
        )
        with open(os.path.join(TEST_DATA, "test_data_edge_project.txt")) as f:
            expected = ast.literal_eval(f.read())
        self.assertEqual(expected, resp)

    def test_sync_two_hop_project(self):
        resp = (
            UserQuery([1])
            .edge("friends")
            .edge("friends")
            .project(["name", ":id"])
            .take(3)
            .to_json()
            .send()
        )
        with open(os.path.join(TEST_DATA, "test_data_two_hop_project.txt")) as f:
            expected = ast.literal_eval(f.read())
        self.assertEqual(expected, resp)
        self.assertEqual(str(expected), str(resp))

    def test_sync_two_hop_multiple_project(self):
        resp = (
            UserQuery([1])
            .edge("friends")
            .project(["age", ":id"])
            .parent()
            .edge("friends")
            .project(["name", ":id"])
            .take(3)
            .to_json()
            .send()
        )
        with open(
            os.path.join(TEST_DATA, "test_data_two_hop_multiple_project.txt")
        ) as f:
            expected = ast.literal_eval(f.read())
        self.assertEqual(expected, resp)
        self.assertEqual(str(expected), str(resp))

    def test_sync_edge_union(self):
        resp = (
            UserQuery([1])
            .edge("friends")
            .project(["name"])
            .union(UserQuery([2]).edge("friends").project(["name"]))
            .to_json()
            .send()
        )
        with open(os.path.join(TEST_DATA, "test_data_edge_union.txt")) as f:
            expected = ast.literal_eval(f.read())
        self.assertEqual(expected, resp)

    @async_test
    async def test_nest(self):
        resp = (
            await UserQuery(range(1, 4))
            .project(["name", ":id"])
            .nest("items")
            .to_json()
        )
        expected = [
            {
                "None": [
                    {
                        "items": [
                            {"name": "id1", ":id": 1},
                            {"name": "id2", ":id": 2},
                            {"name": "id3", ":id": 3},
                        ]
                    }
                ]
            }
        ]
        self.assertEqual(expected, resp)

    @async_test
    async def test_let(self):
        resp = (
            await UserQuery(range(1, 5))
            .edge("friends")
            .count()
            .let("count", "friend_count")
            .to_json()
        )
        with open(os.path.join(TEST_DATA, "test_data_edge_let.txt")) as f:
            expected = ast.literal_eval(f.read())
        self.assertEqual(expected, resp)

    @async_test
    async def test_edge_count(self):
        resp = await UserQuery(range(1, 5)).edge("friends").count().to_json()
        with open(os.path.join(TEST_DATA, "test_data_edge_count.txt")) as f:
            expected = ast.literal_eval(f.read())
        self.assertEqual(expected, resp)

    @async_test
    async def test_where(self):
        resp = await UserQuery(range(1, 4)).where(ast.Expr("user.age == 16")).to_json()
        expected = [{"None": [{":id": 1, "name": "id1", ":type": 2, "age": 16}]}]
        self.assertEqual(expected, resp)

    @async_test
    async def test_order_by(self):
        resp = await UserQuery(range(1, 4)).order_by(ast.Expr("user.age")).to_json()
        expected = [
            {
                "None": [
                    {":id": 1, "name": "id1", ":type": 2, "age": 16},
                    {":id": 2, "name": "id2", ":type": 1, "age": 17},
                    {":id": 3, "name": "id3", ":type": 2, "age": 18},
                ]
            }
        ]
        self.assertEqual(expected, resp)

    @async_test
    async def test_group_by(self):
        resp = await UserQuery(range(1, 10)).group_by(ast.Expr("user.age")).to_json()
        expected = [
            {
                "None": [
                    [
                        16,
                        [
                            {":id": 1, "name": "id1", "age": 16, ":type": 1},
                            {":id": 4, "name": "id4", "age": 16, ":type": 1},
                        ],
                    ],
                    [
                        17,
                        [
                            {":id": 2, "name": "id2", "age": 17, ":type": 1},
                            {":id": 3, "name": "id3", "age": 17, ":type": 1},
                            {":id": 6, "name": "id6", "age": 17, ":type": 1},
                            {":id": 8, "name": "id8", "age": 17, ":type": 1},
                            {":id": 9, "name": "id9", "age": 17, ":type": 1},
                        ],
                    ],
                    [
                        18,
                        [
                            {":id": 5, "name": "id5", "age": 18, ":type": 1},
                            {":id": 7, "name": "id7", "age": 18, ":type": 1},
                        ],
                    ],
                ]
            }
        ]
        self.assertEqual(expected, resp)
        # Test the async version
        resp2 = (
            await UserQuery(range(1, 10)).group_by(ast.Expr("user.async_age")).to_json()
        )
        # Can't use expected because new random users are generated for the second query
        expected2 = [
            {
                "None": [
                    [
                        16,
                        [
                            {":id": 2, "name": "id2", "age": 16, ":type": 1},
                            {":id": 4, "name": "id4", "age": 16, ":type": 1},
                            {":id": 5, "name": "id5", "age": 16, ":type": 1},
                            {":id": 9, "name": "id9", "age": 16, ":type": 1},
                        ],
                    ],
                    [
                        17,
                        [
                            {":id": 7, "name": "id7", "age": 17, ":type": 1},
                            {":id": 8, "name": "id8", "age": 17, ":type": 1},
                        ],
                    ],
                    [
                        18,
                        [
                            {":id": 1, "name": "id1", "age": 18, ":type": 1},
                            {":id": 3, "name": "id3", "age": 18, ":type": 1},
                            {":id": 6, "name": "id6", "age": 18, ":type": 1},
                        ],
                    ],
                ]
            }
        ]
        self.assertEqual(expected2, resp2)

    @async_test
    async def test_as_dict(self):
        resp = (
            await UserQuery(range(1, 10))
            .project(["name", ":id"])
            .take(3)
            .to_json()
            .as_dict()
        )
        expected = {
            "1": {"name": "id1", ":id": 1},
            "2": {"name": "id2", ":id": 2},
            "3": {"name": "id3", ":id": 3},
        }
        self.assertEqual(expected, resp)

    @async_test
    async def test_as_list(self):
        resp = (
            await UserQuery(range(1, 10))
            .project(["name", ":id"])
            .take(3)
            .to_json()
            .as_list()
        )
        expected = [
            {"name": "id1", ":id": 1},
            {"name": "id2", ":id": 2},
            {"name": "id3", ":id": 3},
        ]
        self.assertEqual(expected, resp)


if __name__ == "__main__":
    unittest.main()
