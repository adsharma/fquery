from fquery.django import model
from dataclasses import dataclass
from fquery.view_model import node


@model
@dataclass
@node
class Person:
    first_name: str
    last_name: str
    age: int
