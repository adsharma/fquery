# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
from collections.abc import AsyncGenerator

from async_test import wait_for
from asyncio import iscoroutinefunction, wait_for


VISITED_EDGES_KEY = "__visited_edges__"


def resolve_field(val):
    try:
        if callable(val):
            val = val()
        if iscoroutinefunction(val):
            return wait_for(val)
        else:
            return val
    except Exception as ex:
        log_exception()
        return None


async def async_resolve_field(val, edge_ctx=None):
    try:
        if isinstance(val, AsyncGenerator):
            return [x async for x in val]
        if callable(val):
            if edge_ctx is None:
                val = val()
            else:
                val = val(edge_ctx=edge_ctx)
        if iscoroutinefunction(val):
            return await val
        else:
            return val
    except Exception as ex:
        # log_exception()
        return None
