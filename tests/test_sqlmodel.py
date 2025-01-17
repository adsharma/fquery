from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

from fquery.sqlmodel import SQL_PK, model


@model(global_id=True)
@dataclass(kw_only=True)
class User:
    id: int | None = None
    name: str
    email: str
    created_at: datetime = None
    updated_at: datetime = None
    friend_id: Optional[int] = field(
        default=None, metadata={"SQL": {"foreign_key": "users.id"}}
    )
    friend: Optional["User"] = field(
        default=None, metadata={"SQL": {"relationship": True, "back_populates": False}}
    )
    reviews: List["Review"] = field(
        default=None, metadata={"SQL": {"relationship": True}}
    )


@model(global_id=True)
@dataclass(kw_only=True)
class Review:
    id: int | None = None
    score: int
    user_id: Optional[int] = field(
        default=None, metadata={"SQL": {"foreign_key": "users.id"}}
    )
    user: Optional[User] = field(
        default=None, metadata={"SQL": {"relationship": True, "many_to_one": True}}
    )


@model(global_id=True)
@dataclass
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

        relation = Relation(user.id, 1, user1.id, datetime.now(), datetime.now())
        session.add(relation.sqlmodel())
        session.commit()
        # Read all users from the database
        users = session.query(User.__sqlmodel__).all()
        assert len(users) == 2
        relations = session.query(Relation.__sqlmodel__).all()
        assert len(relations) == 1
