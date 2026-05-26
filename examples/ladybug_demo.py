from __future__ import annotations

import ast
from typing import List

import ladybug as lb
import pyarrow as pa

from fquery.arrow import arrow
from fquery.ladybug import ladybug, ladybug_graph
from fquery.view_model import edge, node


@ladybug
@arrow
@node
class User:
    name: str
    age: int

    @edge
    async def follow(self) -> List["User"]:
        yield self.follow

    @edge
    async def reviews(self) -> List["Review"]:
        yield self.reviews


@ladybug
@arrow
@node
class Review:
    business: str
    rating: int


UserQuery = User.query()
ReviewQuery = Review.query()


@ladybug_graph
class ReviewGraph:
    User = User
    Review = Review


def save_demo() -> None:
    db = lb.Database(":memory:")
    conn = lb.Connection(db)

    ReviewGraph.create_schema(conn)

    review = Review(id=10, business="Cafe", rating=5)
    user = User(id=1, name="Ada", age=37)
    follow1 = User(id=2, name="Grace", age=42)
    follow2 = User(id=3, name="Linus", age=55)
    user.reviews = [review]
    user.follow = [follow1]
    follow1.follow = [follow2]

    review.save(conn)
    follow2.save(conn, include_edges=False)
    follow1.save(conn)
    user.write(conn)

    reviews_cypher = (
        UserQuery([1])
        .edge("reviews")
        .project(["business", "rating"])
        .where(ast.Expr("review.rating >= 4"))
        .to_cypher()
    )
    assert reviews_cypher == (
        "MATCH (u:User)-[:REVIEWS]->(n1:Review)\n"
        "WHERE n1.rating >= 4\n"
        "RETURN n1.business, n1.rating"
    )
    rows = conn.execute(reviews_cypher).get_all()
    assert rows == [["Cafe", 5]]
    print(reviews_cypher)
    print(rows)

    follow_cypher = (
        UserQuery([1]).edge("follow").edge("follow").project(["name"]).to_cypher()
    )
    assert follow_cypher == "MATCH (a:User)-[e:FOLLOW*2..2]-(b:User)\nRETURN b.name"
    follow_rows = conn.execute(follow_cypher).get_all()
    assert ["Linus"] in follow_rows
    print(follow_cypher)
    print(follow_rows)


def arrow_memory_demo() -> None:
    db = lb.Database(":memory:")
    conn = lb.Connection(db)

    users = User.to_arrow(
        [
            User(id=1, name="Ada", age=37),
            User(id=2, name="Grace", age=42),
            User(id=3, name="Linus", age=55),
        ]
    )
    reviews = Review.to_arrow(
        [
            Review(id=10, business="Cafe", rating=5),
            Review(id=20, business="Deli", rating=4),
        ]
    )
    review_edges = {
        "from": [1, 2],
        "to": [10, 20],
    }

    User.create_arrow_table(conn, users)
    Review.create_arrow_table(conn, reviews)
    User.create_arrow_rel_table(
        conn, "follow", pa.table({"from": [1, 2], "to": [2, 3]}), User
    )
    User.create_arrow_rel_table(conn, "reviews", pa.table(review_edges), Review)

    cypher = (
        UserQuery([1, 2]).edge("reviews").project(["business", "rating"]).to_cypher()
    )
    result = User.query_as_arrow(conn, cypher, chunk_size=1024)
    table = result.get_as_arrow()
    assert table.to_pylist() == [
        {"n1.business": "Cafe", "n1.rating": 5},
        {"n1.business": "Deli", "n1.rating": 4},
    ]
    print(table)

    follow_cypher = (
        UserQuery([1]).edge("follow").edge("follow").project(["name"]).to_cypher()
    )
    assert follow_cypher == "MATCH (a:User)-[e:FOLLOW*2..2]-(b:User)\nRETURN b.name"
    follow_result = User.query_as_arrow(conn, follow_cypher, chunk_size=1024)
    follow_table = follow_result.get_as_arrow()
    assert {"b.name": "Linus"} in follow_table.to_pylist()
    print(follow_cypher)
    print(follow_table)


if __name__ == "__main__":
    save_demo()
    arrow_memory_demo()
