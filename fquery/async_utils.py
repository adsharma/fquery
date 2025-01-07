# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
import asyncio
from contextlib import contextmanager


@contextmanager
def _get_event_loop():
    loop = asyncio.get_event_loop()
    if not loop.is_running():
        yield loop
    else:
        yield asyncio.new_event_loop()


def wait_for(coro):
    with _get_event_loop() as loop:
        return loop.run_until_complete(coro)
