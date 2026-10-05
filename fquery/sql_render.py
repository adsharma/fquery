# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
"""Tiny SQL renderer: structured clauses in, (sql, params) out.

Replaces pypika for the query-tree backend. Identifiers render
double-quoted; every literal becomes a ? placeholder with the value
appended to params (safe quoting, stable query plans).
"""

from dataclasses import dataclass, field


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@dataclass
class TableRef:
    name: str
    alias: str = ""

    def render_from(self) -> str:
        if self.alias and self.alias != self.name:
            return quote_ident(self.name) + " " + quote_ident(self.alias)
        return quote_ident(self.name)

    def render_ref(self) -> str:
        return quote_ident(self.alias or self.name)


@dataclass
class Join:
    table: TableRef
    left_alias: str
    left_col: str
    right_col: str


@dataclass
class Select:
    table: TableRef
    joins: list = field(default_factory=list)
    columns: list = field(default_factory=list)
    where: object = None
    order: list = field(default_factory=list)
    limit: object = None
    offset: object = None


@dataclass
class BuiltSQL:
    sql: str
    params: list

    def __str__(self) -> str:
        return self.sql


def render_field(alias: str, col: str, func: str = "") -> str:
    ref = quote_ident(alias) + "." + quote_ident(col)
    if func == "lower":
        return "LOWER(" + ref + ")"
    return ref


def render_cond(node, params: list) -> str:
    kind = node[0]
    if kind == "cmp":
        _, op, alias, col, value = node
        params.append(value)
        return render_field(alias, col) + op + "?"
    if kind == "null":
        _, alias, col, is_null = node
        return render_field(alias, col) + (" IS NULL" if is_null else " IS NOT NULL")
    if kind == "in":
        _, alias, col, values, negate = node
        if not values:
            return "1=0" if not negate else "1=1"
        marks = ",".join(["?"] * len(values))
        params.extend(values)
        out = render_field(alias, col) + " IN (" + marks + ")"
        return ("NOT " + out) if negate else out
    if kind == "like":
        _, alias, col, pattern = node
        params.append(pattern)
        return render_field(alias, col) + " LIKE ?"
    if kind == "match":
        _, alias, col, query = node
        params.append(query)
        return render_field(alias, col) + " MATCH ?"
    if kind in ("and", "or"):
        glue = " AND " if kind == "and" else " OR "
        return "(" + glue.join(render_cond(p, params) for p in node[1]) + ")"
    if kind == "true":
        return "1=1"
    if kind == "false":
        return "1=0"
    raise ValueError("cannot render: " + repr(node))


def render(select: Select) -> BuiltSQL:
    params: list = []
    cols = []
    for proj in select.columns:
        if proj == ("count",):
            cols.append("COUNT(*)")
        elif isinstance(proj, tuple):
            cols.append(render_field(proj[0], proj[1]))
        else:
            cols.append(quote_ident(proj))
    sql = "SELECT " + (", ".join(cols) if cols else "*")
    sql += " FROM " + select.table.render_from()
    for join in select.joins:
        sql += (
            " JOIN "
            + join.table.render_from()
            + " ON "
            + quote_ident(join.left_alias)
            + "."
            + quote_ident(join.left_col)
            + "="
            + join.table.render_ref()
            + "."
            + quote_ident(join.right_col)
        )
    if select.where is not None:
        sql += " WHERE " + render_cond(select.where, params)
    if select.order:
        parts = []
        for alias, col, func, desc in select.order:
            part = render_field(alias, col, func)
            if desc:
                part += " DESC"
            parts.append(part)
        sql += " ORDER BY " + ",".join(parts)
    if select.limit is not None:
        sql += " LIMIT " + str(select.limit)
    if select.offset is not None:
        sql += " OFFSET " + str(select.offset)
    return BuiltSQL(sql, params)
