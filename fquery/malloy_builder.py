# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import ast
import operator

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


class MalloyBuilderVisitor(Visitor):
    INDENT = 2

    def __init__(self, id1s):
        self.malloy = None
        self.malloy_stack = []
        self.visited = set()

    @staticmethod
    def table_from_query(query):
        query_name = query.__class__.__name__.lower()
        # UserQuery -> user
        query_name = query_name.split("query")[0]
        return query_name

    @staticmethod
    def _indent():
        return " " * MalloyBuilderVisitor.INDENT

    async def visit_leaf(self, query):
        table = self.table_from_query(query)
        qstr = f"run: duckdb.table('{table}') -> {{\n"
        while self.malloy_stack:
            func = self.malloy_stack.pop()
            qstr = func(qstr)

        if query in self.visited:
            # Prevent infinite recursion
            return
        else:
            self.visited.add(query)
        for q in query.edges:
            await self.visit(q)
        qstr += "}"
        self.malloy = qstr

    async def visit_project(self, query):
        proj = ", ".join([x if x != ":id" else "id" for x in query.projector])
        self.malloy_stack.append(lambda x: x + self._indent() + f"select: {proj}\n")
        await self.visit(query.child)

    async def visit_take(self, query):
        self.malloy_stack.append(
            lambda x: x + self._indent() + f"limit: {query._count}\n"
        )
        await self.visit(query.child)

    async def visit_where(self, query):
        # TODO: more general lazy expression evaluator
        left, op, right = query._expr.value.split()
        right = ast.literal_eval(right)
        table, field = left.split(".") if "." in left else (self.malloy, left)
        self.malloy_stack.append(
            lambda x: x + self._indent() + f"where: {table}.{field} {op} {right}\n"
        )
        await self.visit(query.child)

    async def visit_order_by(self, query):
        key = query._expr.value
        table, field = key.split(".") if "." in key else (self.malloy, key)
        self.malloy_stack.append(
            lambda x: x + self._indent() + f"order_by: {table}.{field}\n"
        )
        await self.visit(query.child)
