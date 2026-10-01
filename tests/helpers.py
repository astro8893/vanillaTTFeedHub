from __future__ import annotations

import asyncio
from collections.abc import Callable

TOKENS = {
    "ui": "tok-ui-0123456789abcdef",
    "recorder": "tok-recorder-0123456789abcdef",
}


def auth(name: str = "ui") -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKENS[name]}"}


async def eventually(pred: Callable[[], object], timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not pred():
        if loop.time() > deadline:
            raise AssertionError(f"condition not met within {timeout:g}s")
        await asyncio.sleep(0.01)
