from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

from fquery.sqlmodel import SQL_PK, model


@model(global_id=True)
@dataclass
class User:
    name: str
    email: str
    id: int | None = None
    created_at: datetime = None
    updated_at: datetime = None


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
