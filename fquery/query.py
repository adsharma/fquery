# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import ast
import itertools
import traceback
from enum import IntEnum
from types import FunctionType
from typing import Dict, List, Optional, Tuple, Type, Union

from .async_utils import wait_for
from .execute import AbstractSyntaxTreeVisitor
from .malloy_builder import MalloyBuilderVisitor
from .sql_builder import SQLBuilderVisitor
from .view_model import ViewModel, get_edges, get_return_type
from .walk import (
    EdgeContext,
    PrintASTVisitor,
    Tree,
    materialize_walk,
    materialize_walk_obj,
    print_walk,
)


class QueryableOp(IntEnum):
    INVALID = 0
    PROJECT = 1
    WHERE = 2
    TAKE = 3
    COUNT = 4
    LEAF = 5
    COND = 6
    EDGE = 7
    UNION = 8
    BRANCHED_UNION = 9
    NEST = 10
    LET = 11
    ORDER_BY = 12
    GROUP_BY = 13


SwitchType = Union[Tuple, Tuple[int, "Query"]]


class Query:
    """
    A Query doesn't have an iterator attached to it
    """

    OP: QueryableOp = QueryableOp.INVALID
    EDGE_NAME_TO_RETURN_TYPE: Dict[str, Type["Query"]] = {}
    ALL_QUERIES = []
    CLASS_TO_QUERIES = {}
    _TEMP_COUNTER = 0

    def __init__(
        self,
        child: "Query" = None,
        ids: Optional[List[int]] = None,
        items: Optional[List[ViewModel]] = None,
    ) -> None:
        # Boiler plate code that bloats subclasses
        self.parent_edge: Optional[EdgeQueryable] = None
        self._unbound_class = self.__class__
        # At least one of ids or items should be true, but not both.
        # One exception is unbound queries, where the ids/items come
        # from a child query.
        assert (bool(items) ^ bool(ids)) or child
        self._items = items
        if self._items:
            # pyre-fixme[16]: `ViewModel` has no attribute `id`.
            self.ids = [item.id for item in self._items]
        else:
            self.ids = ids or []

        if child:
            self.child: "Query" = child
            self._unbound_class: Type[Query] = child._unbound_class
        self.edges: List["Query"] = []
        self.visited = False
        # Only one of the two below can be true
        self._as_dict = False
        self._as_list = False
        self._to_json = False

    def __str__(self) -> str:
        # black and flake8 don't agree on formatting the next line
        query_name = str(self.OP)[len("QueryableOp.") :]  # noqa: E203
        if self.OP == QueryableOp.LEAF:
            return f"{query_name} ({self.__class__.__name__})"
        else:
            return query_name

    def leaf_type(self) -> str:
        "Transforms UserQuery -> user"
        if self.OP != QueryableOp.LEAF:
            return "x"
        return self.__class__.__name__[: len("Query") - 1].lower()

    @classmethod
    def _get_temp(cls) -> int:
        cls._TEMP_COUNTER += 1
        return cls._TEMP_COUNTER

    # TODO: @run_once
    @staticmethod
    def _populate_class_to_queries():
        for kls in Query.ALL_QUERIES:
            Query.CLASS_TO_QUERIES[kls.TYPE.__name__] = kls

    @classmethod
    def _edge_name_to_query_type(cls, edge_name: str) -> type:
        if not Query.CLASS_TO_QUERIES:
            Query._populate_class_to_queries()
        try:
            return Query.CLASS_TO_QUERIES[cls.EDGE_NAME_TO_RETURN_TYPE[edge_name]]
        except KeyError:
            traceback.print_exc()
            return None

    # The meaning of child vs parent depends on your perspective
    # Query Author writes: a.b().c() where a is the parent of b etc
    # AST construction: a.b().c() where b is the child of c etc
    #
    # Since this API is exposed to Query Author, we use their perspective
    def parent(self) -> "Query":
        if len(self.edges) == 0:
            self.child.edges.append(self)
        else:
            # TODO: Handle the case where len(self.edges) > 1
            self.child.edges.append(self.edges[0])
        return self.child

    def project(self, projector) -> "ProjectQueryable":
        return ProjectQueryable(self, projector)

    def where(self, predicate: ast.Expr) -> "WhereQueryable":
        return WhereQueryable(self, predicate)

    def take(self, count: int = 1) -> "TakeQueryable":
        return TakeQueryable(self, count)

    def count(self) -> "CountQueryable":
        return CountQueryable(self)

    def nest(self, key: str) -> "NestQueryable":
        return NestQueryable(self, key)

    def let(self, old: str, new: str) -> "LetQueryable":
        return LetQueryable(self, old, new)

    def order_by(self, key: ast.Expr) -> "OrderbyQueryable":
        return OrderbyQueryable(self, key)

    def group_by(self, key: ast.Expr) -> "OrderedGroupbyQueryable":
        return OrderedGroupbyQueryable(OrderbyQueryable(self, key), key)

    def cond(self, key=":type", switch: SwitchType = ()) -> "CondQueryable":
        return CondQueryable(self, key, switch)

    def edge(self, edge_name: str, edge_ctx: EdgeContext = None) -> "EdgeQueryable":
        cur_query = self
        while cur_query.OP != QueryableOp.LEAF:
            if hasattr(cur_query, "child"):
                cur_query = cur_query.child
            else:
                break
        self._unbound_class = cur_query._edge_name_to_query_type(edge_name)
        return EdgeQueryable(self, edge_name, self._unbound_class(self), edge_ctx)

    def union(self, *queries) -> "UnionQueryable":
        return UnionQueryable(self, *queries)

    def as_dict(self) -> "Query":
        self._as_dict = True
        return self

    def as_list(self) -> "Query":
        self._as_list = True
        return self

    def to_json(self) -> "Query":
        self._to_json = True
        return self

    def get_ids(self) -> List[int]:
        if self.ids:
            return self.ids
        if self.child:
            return self.child.get_ids()
        return []

    def get_keys(self) -> List[str]:
        return [str(x) for x in self.get_ids()]

    def debug(self, indent=1):
        visitor = AbstractSyntaxTreeVisitor([])
        wait_for(visitor.visit_child(self))
        print_walk(visitor.root, indent)

    def send(self) -> Tree:
        visitor = AbstractSyntaxTreeVisitor([])
        wait_for(visitor.visit_child(self))
        if self._to_json:
            r = wait_for(materialize_walk(visitor.root))
        else:
            r = wait_for(materialize_walk_obj(visitor.root))
        if not self._as_list and not self._as_dict:
            return r
        # r is a list of dicts, but static checkers don't know that
        r = next(itertools.islice(r[0].values(), 0, 1))
        if self._as_dict:
            return {k: v for k, v in zip(self.get_keys(), r)}
        else:
            return r

    async def async_debug(self, indent=1):
        visitor = AbstractSyntaxTreeVisitor([])
        await visitor.visit_child(self)
        print_walk(visitor.root, indent)

    async def async_send(self) -> Tree:
        visitor = AbstractSyntaxTreeVisitor([])
        await visitor.visit_child(self)
        if self._to_json:
            r = await materialize_walk(visitor.root)
        else:
            r = await materialize_walk_obj(visitor.root)
        if not self._as_list and not self._as_dict:
            return r
        # r is a list of dicts, but static checkers don't know that
        # pyre-fixme[16]: Optional type has no attribute `__getitem__`.
        r = next(itertools.islice(r[0].values(), 0, 1))
        if self._as_dict:
            return {k: v for k, v in zip(self.get_keys(), r)}
        else:
            return r

    def __await__(self) -> Tree:
        return self.async_send().__await__()

    def dump(self) -> str:
        visitor = PrintASTVisitor([])
        wait_for(visitor.visit(self))
        return visitor.tree

    def to_sql(self) -> str:
        visitor = SQLBuilderVisitor([])
        wait_for(visitor.visit(self))
        return visitor.sql

    def to_malloy(self) -> str:
        visitor = MalloyBuilderVisitor([])
        wait_for(visitor.visit(self))
        return visitor.malloy

    def batch_resolve_objs(self) -> List[Dict[str, List[ViewModel]]]:
        return [{str(None): [o for o in (self.resolve_obj(i) for i in self.ids) if o]}]

    @staticmethod
    def resolve_obj(_id: int, edge: str = "") -> Optional[ViewModel]:
        return None

    async def iter(self) -> Union[List[ViewModel], Dict[str, List[ViewModel]]]:
        if not self.parent_edge:
            yield self.batch_resolve_objs()[0]
        else:
            # TODO: Use asyncio.gather or similar to paralellize
            async for item in self._items:
                async for i in self.resolve(
                    item, self.parent_edge.edge_name, self.parent_edge._ctx
                ):
                    yield i

    async def resolve(self, item, key, edge_ctx):
        if isinstance(item, ViewModel):
            async for i in item.resolve_edge(key, edge_ctx):
                yield i
        else:
            yield item


def query(cls):
    def constructor(self, ids=None, items=None):
        Query.__init__(self, None, ids, items)

    @staticmethod
    def resolve_obj(_id: int, edge: str = "") -> Optional[ViewModel]:
        return cls.TYPE.get(_id)

    cls = type(cls.__name__, (Query,), dict(cls.__dict__))
    node_cls = cls.TYPE
    edges = get_edges(node_cls)
    cls.EDGE_NAME_TO_RETURN_TYPE = {
        name: get_return_type(func._old) for name, func in edges.items()
    }
    Query.ALL_QUERIES.append(cls)
    cls.OP = QueryableOp.LEAF
    cls.__init__ = constructor
    cls.resolve_obj = resolve_obj
    return cls


class ProjectQueryable(Query):
    OP = QueryableOp.PROJECT

    def __init__(self, child: Query, projector) -> None:
        super(ProjectQueryable, self).__init__(child)
        self.projector = projector

    def __str__(self) -> str:
        return Query.__str__(self) + " " + str(self.projector)


class WhereQueryable(Query):
    OP = QueryableOp.WHERE

    def __init__(self, child: Query, predicate: ast.Expr) -> None:
        super(WhereQueryable, self).__init__(child)
        self._expr = predicate
        # Create a lambda from the predicate. This can be dangerous due
        # to predicates trying to exploit. Need validation of input
        # TODO: Look at pandas.eval
        leaf_name = child.leaf_type()
        code = compile(f"lambda {leaf_name}: " + predicate.value, "<string>", "exec")
        self.predicate = FunctionType(code.co_consts[0], {}, None)

    def __str__(self) -> str:
        return Query.__str__(self) + " " + str(self.predicate)


class TakeQueryable(Query):
    OP = QueryableOp.TAKE

    def __init__(self, child: Query, count: int) -> None:
        super(TakeQueryable, self).__init__(child)
        self._count: int = count

    def __str__(self) -> str:
        return Query.__str__(self) + " " + str(self._count)


class CountQueryable(Query):
    OP = QueryableOp.COUNT

    def __init__(self, child: Query) -> None:
        super().__init__(child)


class CondQueryable(Query):
    OP = QueryableOp.COND

    def __init__(self, child: Query, key: str, switch: SwitchType) -> None:
        super(CondQueryable, self).__init__(child)
        self.key = key
        # pair of <type, Query>
        self.switch = switch

    def __str__(self) -> str:
        return Query.__str__(self) + " " + self.key + " " + str(self.switch)


class UnionQueryable(Query):
    OP = QueryableOp.UNION

    def __init__(self, child, *queries):
        super(UnionQueryable, self).__init__(child)
        self.queries = queries


class BranchedUnionQueryable(Query):
    OP = QueryableOp.BRANCHED_UNION

    def __init__(self, child, *queries):
        super(BranchedUnionQueryable, self).__init__(child)
        self.queries = queries


class EdgeQueryable(Query):
    OP = QueryableOp.EDGE

    # q1.edge('foo').q2
    #
    # constructs the following AST
    # q2 -- (parent_edge) --> EdgeQueryable('foo') -- (child) --> q1
    def __init__(
        self, child: Query, edge_name: str, unbound: Query, edge_ctx: EdgeContext
    ) -> None:
        super(EdgeQueryable, self).__init__(child)
        self._unbound = unbound
        unbound.parent_edge = self
        self.edge_name = edge_name
        self._ctx = edge_ctx

    def __str__(self) -> str:
        return Query.__str__(self) + " " + self.edge_name + " " + str(self._unbound)


class NestQueryable(Query):
    OP = QueryableOp.NEST

    def __init__(self, child: Query, key: str) -> None:
        super().__init__(child)
        self.key = key


class LetQueryable(Query):
    OP = QueryableOp.LET

    def __init__(self, child: Query, old: str, new: str) -> None:
        super().__init__(child)
        self.old = old
        self.new = new


class OrderbyQueryable(Query):
    OP = QueryableOp.ORDER_BY

    def __init__(self, child: Query, key: ast.Expr) -> None:
        super(OrderbyQueryable, self).__init__(child)
        self._expr = key
        # Create a lambda from the key. This can be dangerous due
        # to predicates trying to exploit. Need validation of input
        # TODO: Look at pandas.eval
        leaf_name = child.leaf_type()
        # TODO: use something more robust instead of naming convention
        if "async" in key.value:
            number = self._get_temp()
            code = compile(
                f"async def _func{number}({leaf_name}): return {key.value}",
                "<string>",
                "exec",
                flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
            )
            self.key = FunctionType(code.co_consts[0], {}, None)
        else:
            code = compile(f"lambda {leaf_name}: {key.value}", "<string>", "exec")
            self.key = FunctionType(code.co_consts[0], {}, None)

    def __str__(self) -> str:
        return Query.__str__(self) + " " + str(self.key)


class OrderedGroupbyQueryable(Query):
    OP = QueryableOp.GROUP_BY

    def __init__(self, child: Query, key: ast.Expr) -> None:
        super(OrderedGroupbyQueryable, self).__init__(child)
        self._expr = key
        # Create a lambda from the key. This can be dangerous due
        # to predicates trying to exploit. Need validation of input
        # TODO: Look at pandas.eval
        # Handle the case of many nested operators
        while child and child.OP != QueryableOp.LEAF:
            child = child.child
        leaf_name = child.leaf_type()
        # TODO: use something more robust instead of naming convention
        if "async" in key.value:
            number = self._get_temp()
            code = compile(
                f"async def _func{number}({leaf_name}): return {key.value}",
                "<string>",
                "exec",
                flags=ast.PyCF_ALLOW_TOP_LEVEL_AWAIT,
            )
            self.key = FunctionType(code.co_consts[0], {}, None)
        else:
            code = compile(f"lambda {leaf_name}: {key.value}", "<string>", "exec")
            self.key = FunctionType(code.co_consts[0], {}, None)

    def __str__(self) -> str:
        return Query.__str__(self) + " " + str(self.key)
