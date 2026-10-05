# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import ast
import operator

from pypika import Order, Query, Tables, functions
from pypika.terms import Criterion

from .visitor import Visitor
from .walk import JoinOn

# inspired from pandas.core.computation.ops
_cmp_ops = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}

_order_funcs = {
    "lower": functions.Lower,
}


class MatchCriterion(Criterion):
    """Full-text MATCH (SQLite FTS5): "table"."col" MATCH 'query'."""

    def __init__(self, field, query):
        super().__init__(None)
        self.field = field
        self.query = query

    @property
    def tables_(self):
        return {self.field.table}

    def get_sql(self, **kwargs):
        return "{field} MATCH {query}".format(
            field=self.field.get_sql(**kwargs),
            query=self.query.get_sql(**kwargs),
        )


class SQLBuilderVisitor(Visitor):
    def __init__(self, id1s):
        self.sql = None
        self.tables = {}
        self.current_table = None
        self.visited = set()

    @staticmethod
    def table_name_for(query_cls):
        # Explicit TABLE wins ("memberships"); otherwise the historical
        # derivation (UserQuery -> user).
        table = getattr(query_cls, "TABLE", None)
        if table:
            return table
        query_name = query_cls.__name__.lower()
        return query_name.split("query")[0]

    @staticmethod
    def alias_for(query_cls):
        # Predicate prefix ("membership.user_id"): explicit ALIAS wins,
        # otherwise the singular table stem (memberships -> membership).
        alias = getattr(query_cls, "ALIAS", None)
        if alias:
            return alias
        name = SQLBuilderVisitor.table_name_for(query_cls)
        if name.endswith("s"):
            return name[:-1]
        return name

    def _table_for(self, query_cls):
        alias = self.alias_for(query_cls)
        if alias not in self.tables:
            table = Tables(self.table_name_for(query_cls))[0]
            # Alias only when the predicate prefix differs from the real
            # table name, so single-table SQL renders exactly as before.
            if alias != self.table_name_for(query_cls):
                table = table.as_(alias)
            self.tables[alias] = table
        return self.tables[alias]

    def _field(self, table_alias, col):
        try:
            table = self.tables[table_alias]
        except KeyError:
            raise ValueError("unknown table alias: " + table_alias)
        return table.__getattr__(col)

    def _compile_field_ref(self, node):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            return self._field(node.value.id, node.attr)
        if isinstance(node, ast.Name):
            return self._field(self._alias_of_current(), node.id)
        raise ValueError("unsupported field: " + ast.dump(node))

    def _compile_compare(self, node):
        if len(node.ops) != 1:
            raise ValueError("unsupported predicate: " + ast.dump(node))
        op_type = type(node.ops[0])
        if op_type in (ast.Is, ast.IsNot):
            field = self._compile_field_ref(node.left)
            if not (
                isinstance(node.comparators[0], ast.Constant)
                and node.comparators[0].value is None
            ):
                raise ValueError("unsupported predicate: " + ast.dump(node))
            return field.isnull() if op_type is ast.Is else field.notnull()
        if op_type in (ast.In, ast.NotIn):
            field = self._compile_field_ref(node.left)
            right = node.comparators[0]
            if not isinstance(right, (ast.List, ast.Tuple)):
                raise ValueError("unsupported predicate: " + ast.dump(node))
            vals = [e.value for e in right.elts]
            if not all(isinstance(e, ast.Constant) for e in right.elts):
                raise ValueError("unsupported predicate: " + ast.dump(node))
            if not vals:
                # IN () is invalid SQL; an empty set matches nothing.
                if op_type is ast.In:
                    return field.isnull() & field.notnull()
                return field.isnull() | field.notnull()
            cond = field.isin(vals)
            return cond if op_type is ast.In else cond.negate()
        field = self._compile_field_ref(node.left)
        op = _cmp_ops.get(op_type)
        if op is None:
            raise ValueError("unsupported predicate: " + ast.dump(node))
        if not isinstance(node.comparators[0], ast.Constant):
            raise ValueError("unsupported predicate: " + ast.dump(node))
        return op(field, node.comparators[0].value)

    def _compile_call_predicate(self, node):
        # like(table.col, '%pat%') / match(table.col, 'query')
        if not isinstance(node.func, ast.Name) or len(node.args) != 2:
            raise ValueError("unsupported predicate: " + ast.dump(node))
        field = self._compile_field_ref(node.args[0])
        if not isinstance(node.args[1], ast.Constant):
            raise ValueError("unsupported predicate: " + ast.dump(node))
        pat = node.args[1].value
        if node.func.id == "like":
            return field.like(pat)
        if node.func.id == "match":
            from pypika.terms import ValueWrapper

            return MatchCriterion(field, ValueWrapper(pat))
        raise ValueError("unsupported predicate: " + ast.dump(node))

    def _alias_of_current(self):
        for alias, table in self.tables.items():
            if table is self.current_table:
                return alias
        raise ValueError("no current table")

    def _compile_criterion(self, node):
        if isinstance(node, ast.BoolOp):
            parts = [self._compile_criterion(p) for p in node.values]
            if isinstance(node.op, ast.And):
                crit = parts[0]
                for part in parts[1:]:
                    crit = crit & part
                return crit
            if isinstance(node.op, ast.Or):
                crit = parts[0]
                for part in parts[1:]:
                    crit = crit | part
                return crit
            raise ValueError("unsupported predicate: " + ast.dump(node))
        if isinstance(node, ast.Call):
            return self._compile_call_predicate(node)
        if isinstance(node, ast.Compare):
            return self._compile_compare(node)
        raise ValueError("unsupported predicate: " + ast.dump(node))

    def _compile_order_key(self, node):
        order = None
        while isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            name = node.func.id
            if name == "desc":
                order = Order.desc
            elif name in _order_funcs:
                inner = node.args[0]
                field = self._compile_order_field(inner)
                return _order_funcs[name](field), order
            else:
                raise ValueError("unsupported order key: " + ast.dump(node))
            if len(node.args) != 1:
                raise ValueError("unsupported order key: " + ast.dump(node))
            node = node.args[0]
        field = self._compile_order_field(node)
        return field, order

    def _compile_order_field(self, node):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            return self._field(node.value.id, node.attr)
        if isinstance(node, ast.Name):
            return self._field(self._alias_of_current(), node.id)
        raise ValueError("unsupported order key: " + ast.dump(node))

    async def visit_leaf(self, query):
        if self.sql is None:
            table = self._table_for(query.__class__)
            self.current_table = table
            self.sql = Query.from_(table)
        if query in self.visited:
            # Prevent infinite recursion
            return
        else:
            self.visited.add(query)
        for q in query.edges:
            await self.visit(q)

    async def visit_project(self, query):
        await self.visit(query.child)
        cols = []
        for name in query.projector:
            if name == ":id":
                name = "id"
            if "." in name:
                alias, col = name.split(".")
                cols.append(self._field(alias, col))
            else:
                cols.append(name)
        self.sql = self.sql.select(*cols)

    async def visit_take(self, query):
        await self.visit(query.child)
        self.sql = self.sql.limit(query._count)

    async def visit_skip(self, query):
        await self.visit(query.child)
        self.sql = self.sql.offset(query._count)

    async def visit_count(self, query):
        await self.visit(query.child)
        from pypika.terms import Star

        self.sql = self.sql.select(functions.Count(Star()))

    async def visit_where(self, query):
        await self.visit(query.child)
        # Predicates arrive as unparsed source text in ast.Expr.
        body = query._expr.value if isinstance(query._expr, ast.Expr) else query._expr
        if isinstance(body, str):
            body = ast.parse(body, mode="eval").body
        self.sql = self.sql.where(self._compile_criterion(body))

    async def visit_order_by(self, query):
        await self.visit(query.child)
        body = query._expr.value if isinstance(query._expr, ast.Expr) else query._expr
        if isinstance(body, str):
            body = ast.parse(body, mode="eval").body
        keys = body.elts if isinstance(body, ast.Tuple) else [body]
        for key in keys:
            field, order = self._compile_order_key(key)
            if order is None:
                self.sql = self.sql.orderby(field)
            else:
                self.sql = self.sql.orderby(field, order=order)

    async def visit_edge(self, query):
        await self.visit(query.child)
        target = self._table_for(query._unbound.__class__)
        ctx = query._ctx
        if not isinstance(ctx, JoinOn):
            raise ValueError("edge needs a JoinOn context for SQL")
        self.sql = self.sql.join(target).on(
            self.current_table.__getattr__(ctx.left) == target.__getattr__(ctx.right)
        )
