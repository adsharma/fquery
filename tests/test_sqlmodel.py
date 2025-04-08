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
