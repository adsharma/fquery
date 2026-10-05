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

    def test_project_alias(self):
        built = (
            UserQuery(range(1, 10)).project(["user.id AS uid", "user.name"]).to_sql()
        )
        self.assertEqual(
            'SELECT "user"."id" AS "uid", "user"."name" FROM "user"',
            built.sql,
        )

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

    def test_bind_rows(self):
        import sqlite3

        from fquery import env

        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE user (id INTEGER, name TEXT, age INTEGER)")
        conn.execute("INSERT INTO user VALUES (1, 'amy', 16)")
        with env.connection(conn):
            rows = (
                UserQuery(range(1, 10))
                .project(["user.id", "user.name"])
                .where(ast.Expr('user.age == param("n")'))
                .bind(n=16)
                .rows()
            )
        self.assertEqual([{"id": 1, "name": "amy"}], rows)
        with self.assertRaises(ValueError):
            UserQuery(range(1, 10)).rows()

    def test_params(self):
        built = (
            UserQuery(range(1, 10))
            .where(ast.Expr("user.name == param('who')"))
            .to_sql({"who": "o'brien"})
        )
        self.assertEqual('SELECT * FROM "user" WHERE "user"."name"=?', built.sql)
        self.assertEqual(["o'brien"], built.params)


if __name__ == "__main__":
    unittest.main()
