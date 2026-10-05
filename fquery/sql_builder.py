# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import ast

from . import sql_render
from .visitor import Visitor
from .walk import JoinOn

_cmp_ops = {
    ast.Eq: "=",
    ast.NotEq: "<>",
    ast.Lt: "<",
    ast.LtE: "<=",
    ast.Gt: ">",
    ast.GtE: ">=",
}

_order_funcs = ("lower",)


class SQLBuilderVisitor(Visitor):
    def __init__(self, id1s):
        self.select = None
        self.tables = {}
        self.current_alias = ""
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
            self.tables[alias] = sql_render.TableRef(
                self.table_name_for(query_cls), alias
            )
        return self.tables[alias]

    def _check_field(self, alias, col):
        if alias not in self.tables:
            raise ValueError("unknown table alias: " + alias)
        return alias

    def _compile_field_ref(self, node):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            return (self._check_field(node.value.id, node.attr), node.attr)
        if isinstance(node, ast.Name):
            if not self.current_alias:
                raise ValueError("no current table")
            return (self.current_alias, node.id)
        raise ValueError("unsupported field: " + ast.dump(node))

    def _compile_value(self, node):
        # Literal or param("name") reference.
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "param"
            and len(node.args) == 1
            and isinstance(node.args[0], ast.Constant)
        ):
            return ("param", node.args[0].value)
        if isinstance(node, ast.Constant):
            return node.value
        raise ValueError("unsupported predicate: " + ast.dump(node))

    def _compile_compare(self, node):
        if len(node.ops) != 1:
            raise ValueError("unsupported predicate: " + ast.dump(node))
        op_type = type(node.ops[0])
        if op_type in (ast.Is, ast.IsNot):
            alias, col = self._compile_field_ref(node.left)
            if not (
                isinstance(node.comparators[0], ast.Constant)
                and node.comparators[0].value is None
            ):
                raise ValueError("unsupported predicate: " + ast.dump(node))
            return ("null", alias, col, op_type is ast.Is)
        if op_type in (ast.In, ast.NotIn):
            alias, col = self._compile_field_ref(node.left)
            right = node.comparators[0]
            if not isinstance(right, (ast.List, ast.Tuple)):
                raise ValueError("unsupported predicate: " + ast.dump(node))
            if not all(isinstance(e, ast.Constant) for e in right.elts):
                raise ValueError("unsupported predicate: " + ast.dump(node))
            vals = [e.value for e in right.elts]
            return ("in", alias, col, vals, op_type is ast.NotIn)
        alias, col = self._compile_field_ref(node.left)
        op = _cmp_ops.get(op_type)
        if op is None:
            raise ValueError("unsupported predicate: " + ast.dump(node))
        return ("cmp", op, alias, col, self._compile_value(node.comparators[0]))

    def _compile_call_predicate(self, node):
        # like(table.col, '%pat%') / match(table.col, 'query'); the
        # field may be wrapped: like(lower(table.col), '%pat%').
        if not isinstance(node.func, ast.Name) or len(node.args) != 2:
            raise ValueError("unsupported predicate: " + ast.dump(node))
        target = node.args[0]
        func = ""
        if (
            isinstance(target, ast.Call)
            and isinstance(target.func, ast.Name)
            and target.func.id in _order_funcs
            and len(target.args) == 1
        ):
            func = target.func.id
            target = target.args[0]
        alias, col = self._compile_field_ref(target)
        pat = self._compile_value(node.args[1])
        if node.func.id == "like":
            return ("like", alias, col, pat, func)
        if node.func.id == "match":
            if func:
                raise ValueError("unsupported predicate: " + ast.dump(node))
            return ("match", alias, col, pat)
        raise ValueError("unsupported predicate: " + ast.dump(node))

    def _compile_criterion(self, node):
        if isinstance(node, ast.BoolOp):
            parts = [self._compile_criterion(p) for p in node.values]
            if isinstance(node.op, ast.And):
                return ("and", parts)
            if isinstance(node.op, ast.Or):
                return ("or", parts)
            raise ValueError("unsupported predicate: " + ast.dump(node))
        if isinstance(node, ast.Call):
            return self._compile_call_predicate(node)
        if isinstance(node, ast.Compare):
            return self._compile_compare(node)
        raise ValueError("unsupported predicate: " + ast.dump(node))

    def _compile_order_key(self, node):
        desc = False
        while isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            name = node.func.id
            if len(node.args) != 1:
                raise ValueError("unsupported order key: " + ast.dump(node))
            if name == "desc":
                desc = True
            elif name != "lower":
                raise ValueError("unsupported order key: " + ast.dump(node))
            if name == "lower":
                alias, col = self._compile_field_ref(node.args[0])
                return (alias, col, "lower", desc)
            node = node.args[0]
        alias, col = self._compile_field_ref(node)
        return (alias, col, "", desc)

    async def visit_leaf(self, query):
        if self.select is None:
            table = self._table_for(query.__class__)
            self.current_alias = self.alias_for(query.__class__)
            self.select = sql_render.Select(table)
        if query in self.visited:
            # Prevent infinite recursion
            return
        else:
            self.visited.add(query)
        for q in query.edges:
            await self.visit(q)

    async def visit_project(self, query):
        await self.visit(query.child)
        for name in query.projector:
            if name == ":id":
                name = "id"
            out = ""
            if " AS " in name.upper():
                # "room.id AS rid" (case-insensitive AS)
                at = name.upper().rindex(" AS ")
                name, out = name[:at], name[at + 4:].strip()  # noqa: E203
            if "." in name:
                alias, col = name.split(".")
                self._check_field(alias, col)
                if out:
                    self.select.columns.append((alias, col, out))
                else:
                    self.select.columns.append((alias, col))
            else:
                self.select.columns.append(name)

    async def visit_take(self, query):
        await self.visit(query.child)
        self.select.limit = query._count

    async def visit_skip(self, query):
        await self.visit(query.child)
        self.select.offset = query._count

    async def visit_count(self, query):
        await self.visit(query.child)
        self.select.columns.append(("count",))

    async def visit_where(self, query):
        await self.visit(query.child)
        # Predicates arrive as unparsed source text in ast.Expr.
        body = query._expr.value if isinstance(query._expr, ast.Expr) else query._expr
        if isinstance(body, str):
            body = ast.parse(body, mode="eval").body
        crit = self._compile_criterion(body)
        if self.select.where is None:
            self.select.where = crit
        else:
            self.select.where = ("and", [self.select.where, crit])

    async def visit_order_by(self, query):
        await self.visit(query.child)
        body = query._expr.value if isinstance(query._expr, ast.Expr) else query._expr
        if isinstance(body, str):
            body = ast.parse(body, mode="eval").body
        keys = body.elts if isinstance(body, ast.Tuple) else [body]
        for key in keys:
            self.select.order.append(self._compile_order_key(key))

    async def visit_edge(self, query):
        await self.visit(query.child)
        target = self._table_for(query._unbound.__class__)
        ctx = query._ctx
        if not isinstance(ctx, JoinOn):
            raise ValueError("edge needs a JoinOn context for SQL")
        self.select.joins.append(
            sql_render.Join(target, self.current_alias, ctx.left, ctx.right)
        )

    def built(self, bound=None):
        return sql_render.render(self.select, bound)
