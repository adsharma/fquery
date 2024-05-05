# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import ast
import random
import textwrap
import unittest

from .mock_user import UserQuery


class MalloyTests(unittest.TestCase):
    def setUp(self):
        random.seed(100)
        self.maxDiff = None

    def test_project(self):
        malloy_q = (
            UserQuery(range(1, 10))
            .project([":id", "name"])
            .where(ast.Expr("user.age >= 16"))
            .order_by(ast.Expr("user.age"))
            .take(3)
            .to_malloy()
        )
        expected = textwrap.dedent(
            """\
            run: duckdb.table('user') -> {
              select: id, name
              where: user.age >= 16
              order_by: user.age
              limit: 3
            }"""
        )
        self.assertEqual(expected, malloy_q)


if __name__ == "__main__":
    unittest.main()
