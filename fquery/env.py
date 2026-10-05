# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
"""Ambient execution environment for query chains.

A chain carries its predicates, projections and bound params; the
DBAPI connection it runs on rides here instead of in the call
signature, so execution reads naturally: chain.bind(...).rows().
Thread- and async-safe (ContextVar); set once per request/test.
"""

from contextlib import contextmanager
from contextvars import ContextVar

_conn: ContextVar = ContextVar("fquery_connection", default=None)


def use(conn) -> None:
    """Set (None clears) the ambient connection used by Query.rows()."""
    _conn.set(conn)


def current():
    """The ambient connection, or None when unset."""
    return _conn.get()


@contextmanager
def connection(conn):
    """Scope an ambient connection (tests, scripts, REPL)."""
    token = _conn.set(conn)
    try:
        yield conn
    finally:
        _conn.reset(token)
