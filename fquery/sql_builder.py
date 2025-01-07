# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import ast
import operator

from pypika import Query, Tables

from .visitor import Visitor

# inspired from pandas.core.computation.ops
_cmp_ops_syms = (">", "<", ">=", "<=", "==", "!=")
_cmp_ops_funcs = (
    operator.gt,
    operator.lt,
    operator.ge,
    operator.le,
    operator.eq,
    operator.ne,
)
_cmp_ops_dict = dict(zip(_cmp_ops_syms, _cmp_ops_funcs))


class SQLBuilderVisitor(Visitor):
    def __init__(self, id1s):
        self.sql = None
        self.sql_stack = []
        self.visited = set()

    @staticmethod
    def table_from_query(query):
        query_name = query.__class__.__name__.lower()
        # UserQuery -> user
        query_name = query_name.split("query")[0]
        return Query.from_(query_name)

    async def visit_leaf(self, query):
        self.sql = self.table_from_query(query)
        while self.sql_stack:
            func = self.sql_stack.pop()
            self.sql = func(self.sql)

        if query in self.visited:
            # Prevent infinite recursion
            return
        else:
            self.visited.add(query)
        for q in query.edges:
            await self.visit(q)

    async def visit_project(self, query):
        proj = [x if x != ":id" else "id" for x in query.projector]
        self.sql_stack.append(lambda x: x.select(*proj))
        await self.visit(query.child)

    async def visit_take(self, query):
        self.sql_stack.append(lambda x: x.limit(query._count))
        await self.visit(query.child)

    async def visit_where(self, query):
        # TODO: more general lazy expression evaluator
        left, op, right = query._expr.value.split()
        right = ast.literal_eval(right)
        table, field = left.split(".") if "." in left else (self.sql, left)
        if type(table) is str:
            table = Tables(table)[0]
        binary_op = _cmp_ops_dict[op]
        self.sql_stack.append(
            lambda x: x.where(binary_op(table.__getattr__(field), right))
        )
        await self.visit(query.child)

    async def visit_order_by(self, query):
        key = query._expr.value
        table, field = key.split(".") if "." in key else (self.sql, key)
        if type(table) is str:
            table = Tables(table)[0]
        self.sql_stack.append(lambda x: x.orderby(table.__getattr__(field)))
        await self.visit(query.child)
