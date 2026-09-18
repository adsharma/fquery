import dataclasses
import re
from dataclasses import dataclass, fields
from typing import Callable, Mapping, Type

from pydantic import BaseModel, ConfigDict, Field

# Matches "name 'Foo' is not defined" raised for unresolvable forward refs.
_MISSING = re.compile(r"name '(\w+)' is not defined")

# A namespace is either a static mapping or a callable taking the decorated
# class and returning a mapping. Callables allow consumers (e.g. generated
# packages with hundreds of lazily-imported models) to supply forward-ref
# targets on demand instead of importing everything up front.
Namespace = Mapping | Callable[[type], Mapping]


def pydantic(cls=None, *, namespace: Namespace | None = None):
    """Decorate a class as an fquery pydantic dataclass.

    Usable bare (``@pydantic``) or parametrized
    (``@pydantic(namespace=...)``). ``namespace`` supplies extra names for
    resolving forward references when ``validator()`` rebuilds the model;
    it is inherited by subclasses unless they pass their own.
    """
    if cls is None:
        return lambda c: model(dataclass(kw_only=True)(c), namespace=namespace)
    return model(dataclass(kw_only=True)(cls), namespace=namespace)


def _seed_namespace(cls: type) -> dict:
    """Names always available: the MRO classes plus their module globals."""
    import sys

    ns = {c.__name__: c for c in cls.__mro__}
    for c in cls.__mro__:
        mod = sys.modules.get(getattr(c, "__module__", ""))
        if mod is not None:
            for k, v in vars(mod).items():
                if isinstance(v, type) and k not in ns:
                    ns[k] = v
    return ns


def _missing_name(ex: NameError) -> str | None:
    name = getattr(ex, "name", None)
    if name:
        return name
    m = _MISSING.search(str(ex))
    return m.group(1) if m else None


def _call_provider(provider, cls):
    try:
        return provider(cls)
    except TypeError:
        return provider()


def _rebuild(cls: type, m: type[BaseModel], tries: int = 5) -> None:
    provider = getattr(cls, "__pydantic_namespace__", None)
    ns = _seed_namespace(cls)
    if isinstance(provider, Mapping):
        ns.update(provider)
        provider = None  # static: single rebuild attempt below suffices
    for _ in range(tries):
        if provider is not None:
            extra = _call_provider(provider, cls)
            if extra:
                ns.update(extra)
        before = len(ns)
        try:
            m.model_rebuild(_types_namespace=dict(ns))
            return
        except NameError as ex:
            if provider is None:
                raise
            name = _missing_name(ex)
            # No progress possible: unknown name, or provider is exhausted.
            if name is None or name in ns or len(ns) == before:
                raise
    raise RuntimeError(f"could not resolve forward refs for {cls.__name__}")


def validator(self) -> BaseModel:
    m = self.__pydantic__
    if not getattr(m, "__pydantic_complete__", True):
        _rebuild(type(self), m)
    attrs = {name: getattr(self, name) for name in m.model_fields}
    return m(**attrs)


def get_field_def(cls, field):
    # if the dataclass has a default_factory, or a default value, use it in pydantic Field
    kwargs = {}
    if not isinstance(field.default, dataclasses._MISSING_TYPE):
        kwargs["default"] = field.default
    if not isinstance(field.default_factory, dataclasses._MISSING_TYPE):
        kwargs["default_factory"] = field.default_factory
    return Field(**kwargs)


def model(cls: Type, namespace: Namespace | None = None) -> Type:
    """
    Decorator to convert a dataclass to a Pydantic model.
    """
    # Generate the SQLModel class
    pydantic_cls = type(
        cls.__name__ + "Model",
        (BaseModel,),
        {
            # Add type annotations to the generated fields
            "__annotations__": {**{field.name: field.type for field in fields(cls)}},
            # Actual field defs
            **{field.name: get_field_def(cls, field) for field in fields(cls)},
        },
    )
    cls.__pydantic__ = pydantic_cls
    if namespace is not None:
        cls.__pydantic_namespace__ = namespace
    cls.model_config = ConfigDict(extra="ignore")
    cls.validator = validator

    return cls
