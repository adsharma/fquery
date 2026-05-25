def query_type_name(cls) -> str:
    query_name = getattr(cls, "QUERY_NAME", cls.__name__)
    if query_name.endswith("Query"):
        query_name = query_name[: -len("Query")]
    return query_name
