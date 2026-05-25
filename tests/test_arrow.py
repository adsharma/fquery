from __future__ import annotations

from dataclasses import is_dataclass
from typing import Dict, List, Optional

import pyarrow as pa
import pytest

from fquery.arrow import arrow
from fquery.view_model import edge, node


@arrow
class User:
    name: str
    age: int
    is_active: bool = True
    nickname: Optional[str] = None
    scores: List[int] = None
    metadata: Dict[str, str] = None


def test_arrow_schema():
    user = User(name="Jane Doe", age=42)
    assert is_dataclass(user)
    assert User.__arrow__ == pa.schema(
        [
            pa.field("name", pa.string(), nullable=False),
            pa.field("age", pa.int64(), nullable=False),
            pa.field("is_active", pa.bool_(), nullable=False),
            pa.field("nickname", pa.string(), nullable=True),
            pa.field("scores", pa.list_(pa.int64()), nullable=True),
            pa.field("metadata", pa.map_(pa.string(), pa.string()), nullable=True),
        ]
    )


def test_arrow_table():
    table = User.to_arrow(
        [
            User(
                name="Jane Doe",
                age=42,
                scores=[1, 2],
                metadata={"role": "admin"},
            ),
            {
                "name": "John Doe",
                "age": 37,
                "is_active": False,
                "nickname": "jd",
                "scores": [3],
                "metadata": {"role": "user"},
            },
        ]
    )

    assert table.schema == User.__arrow__
    assert table.to_pylist() == [
        {
            "name": "Jane Doe",
            "age": 42,
            "is_active": True,
            "nickname": None,
            "scores": [1, 2],
            "metadata": [("role", "admin")],
        },
        {
            "name": "John Doe",
            "age": 37,
            "is_active": False,
            "nickname": "jd",
            "scores": [3],
            "metadata": [("role", "user")],
        },
    ]


def test_arrow_accepts_pyarrow_types():
    @arrow
    class Event:
        name: pa.string()
        values: pa.list_(pa.float64())

    assert Event.__arrow__ == pa.schema(
        [
            pa.field("name", pa.string(), nullable=False),
            pa.field("values", pa.list_(pa.float64()), nullable=False),
        ]
    )


def test_arrow_unsupported_type():
    with pytest.raises(TypeError, match="Unsupported Arrow field type"):

        @arrow
        class Bad:
            payload: object


@arrow
@node
class ArrowUser:
    name: str
    age: int

    @edge
    async def reviews(self) -> List["ArrowReview"]:
        yield [ArrowReview.get(m) for m in range(10 * self.id, 10 * self.id + 2)]

    @staticmethod
    def get(id: int) -> "ArrowUser":
        return ArrowUser(id=id, name=f"user{id}", age=20 + id)


@arrow
@node
class ArrowReview:
    business: str
    rating: int

    @edge
    async def author(self) -> ArrowUser:
        yield ArrowUser.get(self.id // 10)

    @staticmethod
    def get(id: int) -> "ArrowReview":
        return ArrowReview(id=id, business=f"business{id}", rating=(id % 5) + 1)


ArrowUserQuery = ArrowUser.query()
ArrowReviewQuery = ArrowReview.query()


def test_arrow_backed_graph_schemas():
    assert ArrowUser.__arrow__ == pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
            pa.field("age", pa.int64(), nullable=False),
        ]
    )
    assert ArrowReview.__arrow__ == pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("business", pa.string(), nullable=False),
            pa.field("rating", pa.int64(), nullable=False),
        ]
    )


def test_arrow_backed_graph_edge():
    actual = ArrowUserQuery([1, 2]).edge("reviews").take(1).as_list().send()
    reverse = ArrowReviewQuery([10]).edge("author").as_list().send()

    assert [user.name for user in actual] == ["user1", "user2"]
    assert [user.reviews[0].business for user in actual] == [
        "business10",
        "business20",
    ]
    assert reverse[0].author.name == "user1"

    users = ArrowUser.to_arrow(actual)
    reviews = ArrowReview.to_arrow([user.reviews[0] for user in actual])

    assert users.schema == ArrowUser.__arrow__
    assert reviews.schema == ArrowReview.__arrow__
    assert users.to_pylist() == [
        {"id": 1, "name": "user1", "age": 21},
        {"id": 2, "name": "user2", "age": 22},
    ]
    assert reviews.to_pylist() == [
        {"id": 10, "business": "business10", "rating": 1},
        {"id": 20, "business": "business20", "rating": 1},
    ]
