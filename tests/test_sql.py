# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import ast
import random
import unittest

from .mock_user import UserQuery


class SQLTests(unittest.TestCase):
    def setUp(self):
        random.seed(100)
        self.maxDiff = None

    def test_project(self):
        built = (
            UserQuery(range(1, 10))
            .project([":id", "name"])
            .where(ast.Expr("user.age >= 16"))
            .order_by(ast.Expr("user.age"))
            .take(3)
            .to_sql()
        )
        self.assertEqual(
            'SELECT "id", "name" FROM "user" '
            'WHERE "user"."age">=? ORDER BY "user"."age" LIMIT 3',
            built.sql,
        )
        self.assertEqual([16], built.params)

    def test_and_or_null_in_like(self):
        built = (
            UserQuery(range(1, 10))
            .where(ast.Expr("user.age >= 16 and user.name != 'x'"))
            .to_sql()
        )
        self.assertEqual(
            'SELECT * FROM "user" ' 'WHERE ("user"."age">=? AND "user"."name"<>?)',
            built.sql,
        )
        self.assertEqual([16, "x"], built.params)

        built = UserQuery(range(1, 10)).where(ast.Expr("user.age is None")).to_sql()
        self.assertEqual('SELECT * FROM "user" WHERE "user"."age" IS NULL', built.sql)

        built = (
            UserQuery(range(1, 10))
            .where(ast.Expr("user.age in [16, 17] or like(user.name, '%a%')"))
            .to_sql()
        )
        self.assertEqual(
            'SELECT * FROM "user" '
            'WHERE ("user"."age" IN (?,?) OR "user"."name" LIKE ?)',
            built.sql,
        )
        self.assertEqual([16, 17, "%a%"], built.params)

    def test_desc_offset_count(self):
        built = (
            UserQuery(range(1, 10))
            .order_by(ast.Expr("desc(user.age), user.name"))
            .take(3)
            .skip(40)
            .to_sql()
        )
        self.assertEqual(
            'SELECT * FROM "user" ORDER BY "user"."age" DESC,"user"."name"'
            " LIMIT 3 OFFSET 40",
            built.sql,
        )
        built = UserQuery(range(1, 10)).count().to_sql()
        self.assertEqual('SELECT COUNT(*) FROM "user"', built.sql)


if __name__ == "__main__":
    unittest.main()
