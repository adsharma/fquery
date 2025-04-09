from dataclasses import field
from typing import List, Optional

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

from fquery.sqlmodel import SQL_PK, many_to_one, one_to_many, sqlmodel


@sqlmodel
class Topic:
    id: Optional[int] = field(default=None, **SQL_PK)
    name: str
    description: Optional[str] = None
    wikidata_id: Optional[str] = None
    probability: Optional[float] = None
    level: int
    combined_prob: Optional[float] = None
    parent: Optional["Topic"] = many_to_one(
        "TopicSQLModel.id", back_populates="children"
    )
    children: List["Topic"] = one_to_many(back_populates="parent")


def test_self_reference():
    engine = create_engine("duckdb:///:memory:", echo=False)
    SQLModel.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        session.query(Topic.__sqlmodel__).all()
