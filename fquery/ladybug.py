import re
import types
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import date, datetime, time
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Type,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from .arrow import table as arrow_table
from .view_model import get_edges, get_return_type

LADYBUG_TYPEMAP = {
    bool: "BOOL",
    int: "INT64",
    float: "DOUBLE",
    str: "STRING",
    bytes: "BLOB",
    datetime: "TIMESTAMP",
    date: "DATE",
    time: "TIME",
}

PARAM_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


@dataclass(frozen=True)
class GraphEdge:
    name: str
    src: Any
    dst: Any


UNION_TYPES = (Union,)
if hasattr(types, "UnionType"):
    UNION_TYPES = UNION_TYPES + (types.UnionType,)


def ladybug(cls):
    """
    Decorator that adds Ladybug persistence helpers to an fquery node.
    """
    if not is_dataclass(cls) or "__dataclass_fields__" not in cls.__dict__:
        cls = dataclass(kw_only=True)(cls)
    return model(cls)


def graph(cls):
    """
    Decorator that registers a group of Ladybug-backed fquery nodes.
    """
    if not is_dataclass(cls):
        annotations = dict(getattr(cls, "__annotations__", {}))
        annotations.setdefault("nodes", List[Any])
        annotations.setdefault("edges", List[GraphEdge])
        cls.__annotations__ = annotations
        cls.nodes = field(default_factory=list)
        cls.edges = field(default_factory=list)
        cls = dataclass(kw_only=True)(cls)

    def create_schema(graph_cls, conn) -> None:
        models = _graph_models(graph_cls)
        for model_cls in models:
            model_cls.create_schema(conn, include_edges=False)
        for model_cls in models:
            model_cls.create_edge_schema(conn)

    def save(self, conn) -> None:
        nodes, edges = _collect_graph(self)
        for node in nodes:
            node.save(conn, include_edges=False)
        for src, edge_name, dst in edges:
            _execute(
                conn,
                _edge_create(type(src), edge_name, type(dst)),
                {"src_id": src.id, "dst_id": dst.id},
            )

    cls.create_schema = classmethod(create_schema)
    cls.save = save
    cls.write = save
    return cls


def graph_edge(name: str, src: Any, dst: Any) -> GraphEdge:
    return GraphEdge(name, src, dst)


def _graph_models(cls: Type) -> List[Type]:
    return [
        value
        for value in cls.__dict__.values()
        if isinstance(value, type) and hasattr(value, "__ladybug_node_ddl__")
    ]


def _is_ladybug_node(value) -> bool:
    return hasattr(type(value), "__ladybug_node_ddl__")


def _is_graph_edge(value) -> bool:
    return isinstance(value, GraphEdge)


def _iter_values(value):
    if value is None:
        return
    if isinstance(value, dict):
        for item in value.values():
            yield item
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            yield item
        return
    yield value


def _is_graph_container(value) -> bool:
    return isinstance(value, (dict, list, tuple, set))


def _graph_roots(graph):
    if is_dataclass(graph):
        for graph_field in fields(graph):
            yield getattr(graph, graph_field.name)
        return
    for name, value in vars(graph).items():
        if not name.startswith("_"):
            yield value


def _collect_graph(graph):
    nodes = []
    edges = []
    seen_nodes = set()
    seen_edges = set()

    def add_edge(edge_name, src, dst):
        edge_key = (type(src), src.id, edge_name, type(dst), dst.id)
        if edge_key in seen_edges:
            return
        seen_edges.add(edge_key)
        edges.append((src, edge_name, dst))

    def visit(value):
        if _is_graph_edge(value):
            visit(value.src)
            visit(value.dst)
            add_edge(value.name, value.src, value.dst)
            return
        if _is_ladybug_node(value):
            visit_node(value)
            return
        for item in _iter_values(value):
            if _is_graph_edge(item):
                visit(item)
            elif _is_ladybug_node(item):
                visit_node(item)
            elif _is_graph_container(item):
                visit(item)

    def visit_node(item):
        node_key = (type(item), item.id)
        if node_key in seen_nodes:
            return
        seen_nodes.add(node_key)
        nodes.append(item)

        for edge_name in get_edges(type(item)):
            if edge_name not in item:
                continue
            for target in _iter_values(item[edge_name]):
                visit(target)
                add_edge(edge_name, item, target)

    for root in _graph_roots(graph):
        visit(root)
    return nodes, edges


def _table_name(cls: Type) -> str:
    return getattr(cls, "__ladybug_table__", cls.__name__)


def _rel_table_name(edge_name: str) -> str:
    return edge_name.upper()


def _unwrap_optional(annotation):
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in UNION_TYPES and type(None) in args:
        non_none = [arg for arg in args if arg is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
    return annotation


def _ladybug_type(annotation) -> str:
    annotation = _unwrap_optional(annotation)
    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin in (list, List):
        if len(args) != 1:
            raise TypeError(f"List fields must specify one item type: {annotation!r}")
        return f"{_ladybug_type(args[0])}[]"

    if origin in (dict, Dict):
        return "MAP"

    try:
        return LADYBUG_TYPEMAP[annotation]
    except KeyError:
        raise TypeError(f"Unsupported Ladybug field type {annotation!r}") from None


def _node_fields(cls: Type):
    return [model_field for model_field, _ in _node_field_types(cls)]


def _node_field_types(cls: Type):
    type_hints = get_type_hints(cls)
    pairs = []
    for model_field in fields(cls):
        annotation = type_hints.get(model_field.name, model_field.type)
        if get_origin(annotation) is ClassVar or model_field.name.startswith("_"):
            continue
        pairs.append((model_field, annotation))
    return pairs


def _node_ddl(cls: Type) -> str:
    columns = []
    for model_field, annotation in _node_field_types(cls):
        col = f"{model_field.name} {_ladybug_type(annotation)}"
        if model_field.name == "id":
            col += " PRIMARY KEY"
        columns.append(col)
    if not any(model_field.name == "id" for model_field in _node_fields(cls)):
        columns.insert(0, "id INT64 PRIMARY KEY")
    return f"CREATE NODE TABLE {_table_name(cls)}({', '.join(columns)})"


def _edge_ddl(cls: Type, edge_name: str, dst_table_name: str) -> str:
    return (
        f"CREATE REL TABLE {_rel_table_name(edge_name)}"
        f"(FROM {_table_name(cls)} TO {dst_table_name})"
    )


def _node_params(obj) -> Dict[str, Any]:
    return {
        model_field.name: getattr(obj, model_field.name)
        for model_field in _node_fields(type(obj))
    }


def _node_create(cls: Type) -> str:
    props = ", ".join(
        f"{model_field.name}: ${model_field.name}" for model_field in _node_fields(cls)
    )
    return f"CREATE (n:{_table_name(cls)} {{{props}}})"


def _edge_create(src_cls: Type, edge_name: str, dst_cls: Type) -> str:
    return (
        f"MATCH (src:{_table_name(src_cls)} {{id: $src_id}}), "
        f"(dst:{_table_name(dst_cls)} {{id: $dst_id}}) "
        f"CREATE (src)-[:{_rel_table_name(edge_name)}]->(dst)"
    )


def _cypher_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_cypher_literal(item) for item in value) + "]"
    raise TypeError(f"Unsupported Ladybug parameter value {value!r}")


def _inline_parameters(query: str, parameters: Dict[str, Any]) -> str:
    def replace(match):
        name = match.group(1)
        if name not in parameters:
            return match.group(0)
        return _cypher_literal(parameters[name])

    return PARAM_RE.sub(replace, query)


def _execute(conn, query: str, parameters: Dict[str, Any] = None):
    if parameters is None:
        return conn.execute(query)
    try:
        return conn.execute(query, parameters)
    except ModuleNotFoundError as exc:
        if exc.name != "numpy":
            raise
        return conn.execute(_inline_parameters(query, parameters))


def _as_arrow_table(cls: Type, rows_or_table):
    if hasattr(rows_or_table, "schema") and hasattr(rows_or_table, "to_pylist"):
        return rows_or_table
    if hasattr(cls, "to_arrow"):
        return cls.to_arrow(rows_or_table)
    return arrow_table(cls, rows_or_table)


def model(cls: Type) -> Type:
    def create_schema(model_cls, conn, *, include_edges: bool = True) -> None:
        conn.execute(_node_ddl(model_cls))
        if not include_edges:
            return
        model_cls.create_edge_schema(conn)

    def create_edge_schema(model_cls, conn) -> None:
        for edge_name, edge_func in get_edges(model_cls).items():
            conn.execute(
                _edge_ddl(model_cls, edge_name, get_return_type(edge_func._old))
            )

    def create_arrow_table(model_cls, conn, rows_or_table, table_name: str = None):
        table_name = table_name or _table_name(model_cls)
        return conn.create_arrow_table(
            table_name, _as_arrow_table(model_cls, rows_or_table)
        )

    def create_arrow_rel_table(
        model_cls,
        conn,
        edge_name: str,
        rows_or_table,
        dst_cls: Type,
        *,
        layout="FLAT",
        indptr_dataframe=None,
    ):
        return conn.create_arrow_rel_table(
            _rel_table_name(edge_name),
            rows_or_table,
            _table_name(model_cls),
            _table_name(dst_cls),
            layout,
            indptr_dataframe,
        )

    def query_as_arrow(model_cls, conn, query: str, chunk_size: int = 1024):
        return conn.query_as_arrow(query, chunk_size)

    def save(self, conn, *, include_edges: bool = True) -> None:
        _execute(conn, _node_create(type(self)), _node_params(self))
        if not include_edges:
            return
        for edge_name in get_edges(type(self)):
            if edge_name not in self:
                continue
            targets = self[edge_name]
            if targets is None:
                continue
            targets = targets if isinstance(targets, list) else [targets]
            for target in targets:
                _execute(
                    conn,
                    _edge_create(type(self), edge_name, type(target)),
                    {"src_id": self.id, "dst_id": target.id},
                )

    cls.__ladybug_table__ = _table_name(cls)
    cls.__ladybug_node_ddl__ = _node_ddl(cls)
    cls.create_schema = classmethod(create_schema)
    cls.create_edge_schema = classmethod(create_edge_schema)
    cls.create_arrow_table = classmethod(create_arrow_table)
    cls.create_arrow_rel_table = classmethod(create_arrow_rel_table)
    cls.query_as_arrow = classmethod(query_as_arrow)
    cls.save = save
    cls.write = save
    return cls
