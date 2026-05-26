from __future__ import annotations

import ast
from typing import List

import ladybug as lb
import pyarrow as pa

from fquery.arrow import arrow
from fquery.ladybug import graph, graph_edge, ladybug
from fquery.view_model import edge, node


@graph
class ReviewGraph:
    @ladybug
    @arrow
    @node
    class User:
        name: str
        age: int

        @edge
        async def follows(self) -> List["User"]:
            yield self.follows

        @edge
        async def reviews(self) -> List["Review"]:
            yield self.reviews

    @ladybug
    @arrow
    @node
    class Review:
        business: str
        rating: int


User = ReviewGraph.User
Review = ReviewGraph.Review
UserQuery = User.query()
ReviewQuery = Review.query()


def save_demo() -> None:
    db = lb.Database(":memory:")
    conn = lb.Connection(db)

    ReviewGraph.create_schema(conn)

    u1 = User(id=1, name="Ada", age=37)
    u2 = User(id=2, name="Grace", age=42)
    u3 = User(id=3, name="Linus", age=55)
    r1 = Review(id=10, business="Cafe", rating=5)
    graph = ReviewGraph(
        nodes=[u1, u2, u3, r1],
        edges=[
            graph_edge("reviews", u1, r1),
            graph_edge("follows", u1, u2),
            graph_edge("follows", u2, u3),
        ],
    )
    graph.save(conn)

    reviews_cypher = (
        UserQuery({"name": "Ada"})
        .edge("reviews")
        .project(["business", "rating"])
        .where(ast.Expr("review.rating >= 4"))
        .to_cypher()
    )
    assert reviews_cypher == (
        "MATCH (u:User {name: 'Ada'})-[:REVIEWS]->(n1:Review)\n"
        "WHERE n1.rating >= 4\n"
        "RETURN n1.business, n1.rating"
    )
    rows = conn.execute(reviews_cypher).get_all()
    assert rows == [["Cafe", 5]]
    print(reviews_cypher)
    print(rows)

    follows_cypher = (
        UserQuery({"name": "Ada"})
        .edge("follows")
        .edge("follows")
        .project(["name"])
        .to_cypher()
    )
    assert follows_cypher == (
        "MATCH (a:User {name: 'Ada'})-[e:FOLLOWS*2..2]-(b:User)\nRETURN b.name"
    )
    follows_rows = conn.execute(follows_cypher).get_all()
    assert ["Linus"] in follows_rows
    print(follows_cypher)
    print(follows_rows)


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
        conn, "follows", pa.table({"from": [1, 2], "to": [2, 3]}), User
    )
    User.create_arrow_rel_table(conn, "reviews", pa.table(review_edges), Review)

    cypher = (
        UserQuery({"name": "Ada"})
        .edge("reviews")
        .project(["business", "rating"])
        .to_cypher()
    )
    result = User.query_as_arrow(conn, cypher, chunk_size=1024)
    table = result.get_as_arrow()
    assert table.to_pylist() == [
        {"n1.business": "Cafe", "n1.rating": 5},
    ]
    print(table)

    follows_cypher = (
        UserQuery({"name": "Ada"})
        .edge("follows")
        .edge("follows")
        .project(["name"])
        .to_cypher()
    )
    assert follows_cypher == (
        "MATCH (a:User {name: 'Ada'})-[e:FOLLOWS*2..2]-(b:User)\nRETURN b.name"
    )
    follows_result = User.query_as_arrow(conn, follows_cypher, chunk_size=1024)
    follows_table = follows_result.get_as_arrow()
    assert {"b.name": "Linus"} in follows_table.to_pylist()
    print(follows_cypher)
    print(follows_table)


if __name__ == "__main__":
    save_demo()
    arrow_memory_demo()
