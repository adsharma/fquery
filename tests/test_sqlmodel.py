from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from fquery.sqlmodel import model


@model()
@dataclass
class User:
    id: int
    name: str
    email: str
    created_at: datetime = None
    updated_at: datetime = None


# Create a new user. This should be cheap
user = User(
    id=1,
    name="John Doe",
    email="john@example.com",
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
    engine = create_engine("sqlite:///:memory:", echo=True)
    # Having this at the top of the file will cause a circular import
    from sqlmodel import SQLModel

    SQLModel.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)

    with Session() as session:
        session.add(user_sql)
        session.commit()
        # Read all users from the database
        users = session.query(User.__sqlmodel__).all()
        assert len(users) == 1
        assert users[0].__dict__ == user_sql.__dict__
