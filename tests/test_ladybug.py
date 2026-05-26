from typing import List

from fquery.ladybug import ladybug, ladybug_graph
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


@ladybug
@node
class LadybugUser:
    name: str
    age: int

    @edge
    async def follow(self) -> List["LadybugUser"]:
        yield self.follow

    @edge
    async def reviews(self) -> List["LadybugReview"]:
        yield self.reviews


@ladybug
@node
class LadybugReview:
    business: str
    rating: int


@ladybug_graph
class LadybugReviewGraph:
    User = LadybugUser
    Review = LadybugReview


def test_ladybug_schema_and_to_cypher():
    conn = FakeConnection()

    LadybugUser.create_schema(conn)

    assert conn.executed == [
        (
            "CREATE NODE TABLE LadybugUser(id INT64 PRIMARY KEY, name STRING, age INT64)",
            None,
        ),
        ("CREATE REL TABLE FOLLOW(FROM LadybugUser TO LadybugUser)", None),
        ("CREATE REL TABLE REVIEWS(FROM LadybugUser TO LadybugReview)", None),
    ]
    LadybugReview.query()
    assert (
        LadybugUser.query()([1])
        .edge("reviews")
        .project(["business", "rating"])
        .to_cypher()
        == "MATCH (u:LadybugUser)-[:REVIEWS]->(n1:LadybugReview)\n"
        "RETURN n1.business, n1.rating"
    )
    assert (
        LadybugUser.query()([1])
        .edge("follow")
        .edge("follow")
        .project(["name"])
        .to_cypher()
        == "MATCH (a:LadybugUser)-[e:FOLLOW*2..2]-(b:LadybugUser)\n"
        "RETURN b.name"
    )


def test_ladybug_graph_schema_registration():
    conn = FakeConnection()

    LadybugReviewGraph.create_schema(conn)

    assert conn.executed == [
        (
            "CREATE NODE TABLE LadybugUser(id INT64 PRIMARY KEY, name STRING, age INT64)",
            None,
        ),
        (
            "CREATE NODE TABLE LadybugReview(id INT64 PRIMARY KEY, business STRING, rating INT64)",
            None,
        ),
        ("CREATE REL TABLE FOLLOW(FROM LadybugUser TO LadybugUser)", None),
        ("CREATE REL TABLE REVIEWS(FROM LadybugUser TO LadybugReview)", None),
    ]


def test_ladybug_save_writes_node_and_edges():
    conn = FakeConnection()
    review = LadybugReview(id=10, business="Cafe", rating=5)
    user = LadybugUser(id=1, name="Ada", age=37)
    followed = LadybugUser(id=2, name="Grace", age=42)
    user.follow = [followed]
    user.reviews = [review]

    user.save(conn)

    assert conn.executed == [
        (
            "CREATE (n:LadybugUser {id: $id, name: $name, age: $age})",
            {"id": 1, "name": "Ada", "age": 37},
        ),
        (
            "MATCH (src:LadybugUser {id: $src_id}), "
            "(dst:LadybugUser {id: $dst_id}) "
            "CREATE (src)-[:FOLLOW]->(dst)",
            {"src_id": 1, "dst_id": 2},
        ),
        (
            "MATCH (src:LadybugUser {id: $src_id}), "
            "(dst:LadybugReview {id: $dst_id}) "
            "CREATE (src)-[:REVIEWS]->(dst)",
            {"src_id": 1, "dst_id": 10},
        ),
    ]


def test_ladybug_arrow_helpers():
    conn = FakeConnection()
    table = FakeArrowTable()
    rel_table = object()
    cypher = "MATCH (u:LadybugUser) RETURN u.name"

    LadybugUser.create_arrow_table(conn, table)
    LadybugUser.create_arrow_rel_table(conn, "reviews", rel_table, LadybugReview)
    result = LadybugUser.query_as_arrow(conn, cypher, chunk_size=64)

    assert conn.arrow_tables == [("LadybugUser", table)]
    assert conn.arrow_rel_tables == [
        ("REVIEWS", rel_table, "LadybugUser", "LadybugReview", "FLAT", None)
    ]
    assert result == "arrow-result"
    assert conn.arrow_queries == [(cypher, 64)]
