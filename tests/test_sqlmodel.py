from dataclasses import field
from datetime import datetime
from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

from fquery.sqlmodel import (
    SQL_PK,
    foreign_key,
    many_to_one,
    one_to_many,
    sqlmodel,
    unique,
)


def test_sqlmodel_namespace_hook():
    # Both classes live in function scope, invisible to module globals,
    # so get_type_hints() can only resolve them via the namespace hook.
    # Mirrors the User/Review pairing: Widget is decorated first with an
    # unresolved forward ref, Gadget's hook completes both sides.
    @sqlmodel(table=False)
    class Widget:
        id: int | None = None
        name: str = ""
        gadgets: List["Gadget"] = one_to_many("widget")

    @sqlmodel(table=False, namespace={"Widget": Widget})
    class Gadget:
        id: int | None = None
        widget: Optional["Widget"] = many_to_one("widgets.id")

    # Relationship registered with back_populates, and the forward ref
    # resolved to the generated SQLModel via the namespace hook.
    assert (
        Gadget.__sqlmodel__.__annotations__["widget"] == Optional[Widget.__sqlmodel__]
    )
    assert Gadget.__sqlmodel__.__sqlmodel_relationships__["widget"].back_populates == (
        "gadgets"
    )


def test_sqlmodel_namespace_hook_callable():
    seen = []

    def provider(cls):
        seen.append(cls.__name__)
        return {}

    @sqlmodel(namespace=provider)
    class Gizmo:
        id: int | None = None

    # Nothing to resolve, so the hook is never consulted and normal
    # decoration (tablename, fields) is unaffected.
    assert seen == []
    assert Gizmo.__sqlmodel__.__tablename__ == "gizmos"


@sqlmodel
class User:
    id: int | None = None
    name: str
    email: str = unique()
    created_at: datetime = None
    updated_at: datetime = None

    friend: Optional["User"] = foreign_key("users.id")
    reviews: List["Review"] = one_to_many("author")
    visits: List["Visit"] = one_to_many()


@sqlmodel
class Review:
    id: int | None = None
    score: int
    author: Optional[User] = many_to_one("users.id")


@sqlmodel
class Visit:
    id: int | None = None
    place: str
    user: Optional[User] = many_to_one("users.id")


@sqlmodel
class Relation:
    src: int | None = field(**SQL_PK)
    type: int = field(**SQL_PK)
    dst: int = field(**SQL_PK)
    created_at: datetime = None
    updated_at: datetime = None


# Create a new user. This should be cheap
user = User(
    name="John Doe",
    email="john@example.com",
    created_at=datetime.now(),
    updated_at=datetime.now(),
)

user1 = User(
    name="Jane Doe",
    email="jane@example.com",
    created_at=datetime.now(),
    updated_at=datetime.now(),
    friend=user.id,
)

# The following is equivalent to: user.sql_model()
# from sqlmodel import Field
# from typing import Optional
# class UserSQLModel(SQLModel, table=True):
#     __tablename__ = "users"
#
#     id: int = Field(primary_key=True)
#     name: str
#     email: str
#     created_at: Optional[datetime] = Field(default_factory=datetime.now)
#     updated_at: Optional[datetime] = Field(default=None)


def test_sqlmodel():
    user_sql = user.sqlmodel()
    assert user_sql.__tablename__ == "users"
    engine = create_engine("duckdb:///:memory:", echo=True)
    SQLModel.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)

    with Session() as session:
        session.add(user.sqlmodel())
        session.add(user1.sqlmodel())
        session.commit()

        relation = Relation(
            src=user.id,
            type=1,
            dst=user1.id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        session.add(relation.sqlmodel())
        session.commit()
        # Read all users from the database
        users = session.query(User.__sqlmodel__).all()
        assert len(users) == 2
        relations = session.query(Relation.__sqlmodel__).all()
        assert len(relations) == 1
