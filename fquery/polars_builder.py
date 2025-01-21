import ast
import operator

import polars as pl

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


class PolarsBuilderVisitor(Visitor):

    def __init__(self, id1s):
        self.polars = None
        self.polars_stack = []
        self.visited = set()

    async def visit_leaf(self, query):
        # TODO: make this columnar and real lazy instead of faking laziness
        self.polars = pl.DataFrame(await query.as_list()).lazy()
        while self.polars_stack:
            func, params = self.polars_stack.pop()
            self.polars = getattr(self.polars, func)(params)

    async def visit_project(self, query):
        self.polars_stack.append(("select", query.projector))
        await self.visit(query.child)

    async def visit_take(self, query):
        self.polars_stack.append(("limit", query._count))
        await self.visit(query.child)

    async def visit_where(self, query):
        left, op, right = query._expr.value.split()
        right = ast.literal_eval(right)
        table, field = left.split(".") if "." in left else (self.malloy, left)
        self.polars_stack.append(("filter", (_cmp_ops_dict[op](pl.col(field), right))))
        await self.visit(query.child)

    async def visit_order_by(self, query):
        table, field = query._expr.value.split(".")
        self.polars_stack.append(("sort", field))
        await self.visit(query.child)
