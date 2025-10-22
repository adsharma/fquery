# Copyright (c) Arun Sharma, 2025
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
        self.match_parts = []
        self.current_node = "u"
        self.where_clauses = []
        self.return_clause = ""
        self.order_by_clause = ""
        self.limit_clause = ""
        self.visited = set()
        self.node_counter = 0
        self.root_label = None

    @staticmethod
    def table_from_query(query):
        query_name = query.__class__.__name__.lower()
        # UserQuery -> user -> User
        query_name = query_name.split("query")[0]
        return query_name.capitalize()

    def _get_next_node_var(self):
        self.node_counter += 1
        return f"n{self.node_counter}"

    async def visit_leaf(self, query):
        if not self.root_label:
            self.root_label = self.table_from_query(query)
            self.match_parts = [f"({self.current_node}:{self.root_label})"]

        if query in self.visited:
            # Prevent infinite recursion
            return
        else:
            self.visited.add(query)
        for q in query.edges:
            await self.visit(q)

    async def visit_project(self, query):
        await self.visit(query.child)
        proj = ", ".join(
            [
                f"{self.current_node}.{x}" if x != ":id" else f"{self.current_node}.id"
                for x in query.projector
            ]
        )
        self.return_clause = f"RETURN {proj}"

    async def visit_take(self, query):
        await self.visit(query.child)
        self.limit_clause = f"LIMIT {query._count}"

    async def visit_where(self, query):
        await self.visit(query.child)
        # TODO: more general lazy expression evaluator
        left, op, right = query._expr.value.split()
        right = ast.literal_eval(right)
        table, field = left.split(".") if "." in left else (self.cypher, left)
        self.where_clauses.append(f"{self.current_node}.{field} {op} {right}")

    async def visit_order_by(self, query):
        await self.visit(query.child)
        key = query._expr.value
        table, field = key.split(".") if "." in key else (self.cypher, key)
        self.order_by_clause = f"ORDER BY {self.current_node}.{field}"

    async def visit_edge(self, query):
        # Ensure we have the root label and initial match part
        if not self.root_label:
            # Find the root query
            root_query = query
            while hasattr(root_query, "child") and root_query.child:
                root_query = root_query.child
            if hasattr(root_query, "__class__"):
                self.root_label = self.table_from_query(root_query)
                self.match_parts = [f"({self.current_node}:{self.root_label})"]

        edge_name = query.edge_name

        # Check if this is part of a multi-hop pattern of the same edge type
        # Look ahead to see if the child has another edge of the same type
        has_same_edge_child = (
            hasattr(query, "child")
            and hasattr(query.child, "OP")
            and query.child.OP.name == "EDGE"
            and query.child.edge_name == edge_name
        )

        if has_same_edge_child:
            # Count the total number of consecutive edges of the same type
            hops = 1  # current edge
            current_query = query.child
            while (
                hasattr(current_query, "OP")
                and current_query.OP.name == "EDGE"
                and current_query.edge_name == edge_name
            ):
                hops += 1
                current_query = current_query.child

            # This is the start of a multi-hop pattern (e.g., friend-of-friend, etc.)
            self.match_parts = [
                f"(a:{self.root_label})",
                f"[e:{edge_name.upper()}*{hops}..{hops}]",
                f"(b:{self.root_label})",
            ]
            self.current_node = "b"
            # Skip visiting the intermediate edges and visit the query after the chain
            await self.visit(current_query)
        else:
            # Regular edge traversal
            child_label = self.table_from_query(query._unbound)
            next_node = self._get_next_node_var()
            relationship = f"[:{edge_name.upper()}]"
            self.match_parts.append(f"{relationship}->({next_node}:{child_label})")
            self.current_node = next_node
            await self.visit(query.child)

    async def visit_union(self, query):
        # UNION in Cypher - this is complex, would need to handle multiple queries
        # For now, just visit the child
        await self.visit(query.child)

    async def visit_count(self, query):
        await self.visit(query.child)
        self.return_clause = "RETURN count(*)"

    async def visit_nest(self, query):
        # Nesting - not directly supported in Cypher
        await self.visit(query.child)

    async def visit_let(self, query):
        # LET - for renaming, could be handled with AS in RETURN
        await self.visit(query.child)
