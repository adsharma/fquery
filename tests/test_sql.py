# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import ast
import random
import unittest

from pypika import Query, Tables

from .mock_user import UserQuery


class SQLTests(unittest.TestCase):
    def setUp(self):
        random.seed(100)
        self.maxDiff = None

    def test_project(self):
        sql = (
            UserQuery(range(1, 10))
            .project([":id", "name"])
            .where(ast.Expr("user.age >= 16"))
            .order_by(ast.Expr("user.age"))
            .take(3)
            .to_sql()
        )
        user = Tables("user")[0]
        expected = (
            Query.from_("user")
            .select("id", "name")
            .where(user.age >= 16)
            .orderby(user.age)
            .limit(3)
        )
        self.assertEqual(str(expected), str(sql))


if __name__ == "__main__":
    unittest.main()
