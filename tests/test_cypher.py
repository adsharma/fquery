# Copyright (c) Arun Sharma, 2025
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import ast
import random
import textwrap
import unittest

from .mock_user import UserQuery


class CypherTests(unittest.TestCase):
    def setUp(self):
        random.seed(100)
        self.maxDiff = None

    def test_project(self):
        cypher_q = (
            UserQuery(range(1, 10))
            .project([":id", "name"])
            .where(ast.Expr("user.age >= 16"))
            .order_by(ast.Expr("user.age"))
            .take(3)
            .to_cypher()
        )
        expected = textwrap.dedent(
            """\
            MATCH (u:User)
            WHERE u.age >= 16
            RETURN u.id, u.name
            ORDER BY u.age
            LIMIT 3"""
        )
        self.assertEqual(expected, cypher_q)

    def test_sync_edge_project(self):
        cypher_q = (
            UserQuery(range(1, 5))
            .edge("friends")
            .project(["name", ":id"])
            .take(3)
            .to_cypher()
        )
        expected = textwrap.dedent(
            """\
            MATCH (u:User)-[:FRIENDS]->(n1:User)
            RETURN n1.name, n1.id
            LIMIT 3"""
        )
        self.assertEqual(expected, cypher_q)

    def test_sync_two_hop_project(self):
        cypher_q = (
            UserQuery([1])
            .edge("friends")
            .edge("friends")
            .project(["name", ":id"])
            .take(3)
            .to_cypher()
        )
        expected = textwrap.dedent(
            """\
            MATCH (a:User)-[e:FRIENDS*2..2]-(b:User)
            RETURN b.name, b.id
            LIMIT 3"""
        )
        self.assertEqual(expected, cypher_q)

    def test_count(self):
        cypher_q = UserQuery(range(1, 10)).count().to_cypher()
        expected = textwrap.dedent(
            """\
            MATCH (u:User)
            RETURN count(*)"""
        )
        self.assertEqual(expected, cypher_q)


if __name__ == "__main__":
    unittest.main()
