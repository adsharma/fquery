import ast
import random
import unittest

import polars as pl
from polars.testing import assert_frame_equal

from .mock_user import UserQuery


class PolarsTests(unittest.TestCase):
    def setUp(self):
        random.seed(100)
        self.maxDiff = None

    def test_project(self):
        df = (
            UserQuery(range(1, 10))
            .project([":id", "name", "age"])
            .where(ast.Expr("user.age >= 16"))
            .order_by(ast.Expr("user.age"))
            .take(3)
            .to_polars()
        )
        expected = pl.DataFrame(
            {
                ":id": [1, 4, 2],
                "name": ["id1", "id4", "id2"],
                "age": [16, 16, 17],
            }
        )
        assert_frame_equal(expected, df)


if __name__ == "__main__":
    unittest.main()
