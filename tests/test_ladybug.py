from typing import List

from fquery.ladybug import graph, graph_edge, ladybug
from fquery.view_model import edge, node


class FakeConnection:
    def __init__(self):
        self.executed = []
        self.arrow_tables = []
        self.arrow_rel_tables = []
        self.arrow_queries = []

    def execute(self, query, parameters=None):
        self.executed.append((query, parameters))
        return []

    def create_arrow_table(self, table_name, dataframe):
        self.arrow_tables.append((table_name, dataframe))
        return []

    def create_arrow_rel_table(
        self,
        table_name,
        dataframe,
        src_table_name,
        dst_table_name,
        layout,
        indptr_dataframe,
    ):
        self.arrow_rel_tables.append(
            (
                table_name,
                dataframe,
                src_table_name,
                dst_table_name,
                layout,
                indptr_dataframe,
            )
        )
        return []

    def query_as_arrow(self, query, chunk_size):
        self.arrow_queries.append((query, chunk_size))
        return "arrow-result"


class FakeArrowTable:
    schema = None

    def to_pylist(self):
        return []


@graph
class ReviewGraph:
    @ladybug
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
    @node
    class Review:
        business: str
        rating: int


User = ReviewGraph.User
Review = ReviewGraph.Review


def test_ladybug_schema_and_to_cypher():
    conn = FakeConnection()

    User.create_schema(conn)

    assert conn.executed == [
        (
            "CREATE NODE TABLE User(id INT64 PRIMARY KEY, name STRING, age INT64)",
            None,
        ),
        ("CREATE REL TABLE FOLLOWS(FROM User TO User)", None),
        ("CREATE REL TABLE REVIEWS(FROM User TO Review)", None),
    ]
    Review.query()
    assert (
        User.query()({"name": "Ada"})
        .edge("reviews")
        .project(["business", "rating"])
        .to_cypher()
        == "MATCH (u:User {name: 'Ada'})-[:REVIEWS]->(n1:Review)\n"
        "RETURN n1.business, n1.rating"
    )
    assert (
        User.query()({"name": "Ada"})
        .edge("follows")
        .edge("follows")
        .project(["name"])
        .to_cypher()
        == "MATCH (a:User {name: 'Ada'})-[e:FOLLOWS*2..2]-(b:User)\n"
        "RETURN b.name"
    )


def test_graph_schema_registration():
    conn = FakeConnection()

    ReviewGraph.create_schema(conn)

    assert conn.executed == [
        (
            "CREATE NODE TABLE User(id INT64 PRIMARY KEY, name STRING, age INT64)",
            None,
        ),
        (
            "CREATE NODE TABLE Review(id INT64 PRIMARY KEY, business STRING, rating INT64)",
            None,
        ),
        ("CREATE REL TABLE FOLLOWS(FROM User TO User)", None),
        ("CREATE REL TABLE REVIEWS(FROM User TO Review)", None),
    ]


def test_ladybug_save_writes_node_and_edges():
    conn = FakeConnection()
    review = Review(id=10, business="Cafe", rating=5)
    user = User(id=1, name="Ada", age=37)
    followed = User(id=2, name="Grace", age=42)
    user.follows = [followed]
    user.reviews = [review]

    user.save(conn)

    assert conn.executed == [
        (
            "CREATE (n:User {id: $id, name: $name, age: $age})",
            {"id": 1, "name": "Ada", "age": 37},
        ),
        (
            "MATCH (src:User {id: $src_id}), "
            "(dst:User {id: $dst_id}) "
            "CREATE (src)-[:FOLLOWS]->(dst)",
            {"src_id": 1, "dst_id": 2},
        ),
        (
            "MATCH (src:User {id: $src_id}), "
            "(dst:Review {id: $dst_id}) "
            "CREATE (src)-[:REVIEWS]->(dst)",
            {"src_id": 1, "dst_id": 10},
        ),
    ]


def test_graph_save_writes_nodes_then_edges():
    conn = FakeConnection()
    u1 = User(id=1, name="Ada", age=37)
    u2 = User(id=2, name="Grace", age=42)
    u3 = User(id=3, name="Linus", age=55)
    r1 = Review(id=10, business="Cafe", rating=5)
    graph = ReviewGraph(
        nodes=[u1, u2, u3, r1],
        edges=[
            graph_edge("follows", u1, u2),
            graph_edge("follows", u2, u3),
            graph_edge("reviews", u1, r1),
        ],
    )

    graph.save(conn)

    assert conn.executed == [
        (
            "CREATE (n:User {id: $id, name: $name, age: $age})",
            {"id": 1, "name": "Ada", "age": 37},
        ),
        (
            "CREATE (n:User {id: $id, name: $name, age: $age})",
            {"id": 2, "name": "Grace", "age": 42},
        ),
        (
            "CREATE (n:User {id: $id, name: $name, age: $age})",
            {"id": 3, "name": "Linus", "age": 55},
        ),
        (
            "CREATE (n:Review {id: $id, business: $business, rating: $rating})",
            {"id": 10, "business": "Cafe", "rating": 5},
        ),
        (
            "MATCH (src:User {id: $src_id}), "
            "(dst:User {id: $dst_id}) "
            "CREATE (src)-[:FOLLOWS]->(dst)",
            {"src_id": 1, "dst_id": 2},
        ),
        (
            "MATCH (src:User {id: $src_id}), "
            "(dst:User {id: $dst_id}) "
            "CREATE (src)-[:FOLLOWS]->(dst)",
            {"src_id": 2, "dst_id": 3},
        ),
        (
            "MATCH (src:User {id: $src_id}), "
            "(dst:Review {id: $dst_id}) "
            "CREATE (src)-[:REVIEWS]->(dst)",
            {"src_id": 1, "dst_id": 10},
        ),
    ]


def test_ladybug_arrow_helpers():
    conn = FakeConnection()
    table = FakeArrowTable()
    rel_table = object()
    cypher = "MATCH (u:User) RETURN u.name"

    User.create_arrow_table(conn, table)
    User.create_arrow_rel_table(conn, "reviews", rel_table, Review)
    result = User.query_as_arrow(conn, cypher, chunk_size=64)

    assert conn.arrow_tables == [("User", table)]
    assert conn.arrow_rel_tables == [
        ("REVIEWS", rel_table, "User", "Review", "FLAT", None)
    ]
    assert result == "arrow-result"
    assert conn.arrow_queries == [(cypher, 64)]
