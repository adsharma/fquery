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


class CypherBuilderVisitor(Visitor):
    def __init__(self, id1s):
        self.cypher = None
        self.cypher_clauses = []
        self.visited = set()

    @staticmethod
    def table_from_query(query):
        query_name = query.__class__.__name__.lower()
        # UserQuery -> user -> User
        query_name = query_name.split("query")[0]
        return query_name.capitalize()

    async def visit_leaf(self, query):
        label = self.table_from_query(query)
        sorted_clauses = sorted(self.cypher_clauses, key=lambda x: x[0])
        clauses = [c for _, c in sorted_clauses]
        qstr = f"MATCH (u:{label})"
        if clauses:
            qstr += "\n" + "\n".join(clauses)
        if query in self.visited:
            # Prevent infinite recursion
            return
        else:
            self.visited.add(query)
        for q in query.edges:
            await self.visit(q)
        self.cypher = qstr

    async def visit_project(self, query):
        proj = ", ".join([f"u.{x}" if x != ":id" else "u.id" for x in query.projector])
        self.cypher_clauses.append((2, f"RETURN {proj}"))
        await self.visit(query.child)

    async def visit_take(self, query):
        self.cypher_clauses.append((4, f"LIMIT {query._count}"))
        await self.visit(query.child)

    async def visit_where(self, query):
        # TODO: more general lazy expression evaluator
        left, op, right = query._expr.value.split()
        right = ast.literal_eval(right)
        table, field = left.split(".") if "." in left else (self.cypher, left)
        self.cypher_clauses.append((1, f"WHERE u.{field} {op} {right}"))
        await self.visit(query.child)

    async def visit_order_by(self, query):
        key = query._expr.value
        table, field = key.split(".") if "." in key else (self.cypher, key)
        self.cypher_clauses.append((3, f"ORDER BY u.{field}"))
        await self.visit(query.child)