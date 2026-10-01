import asyncio
import time

import aiohttp
from aiohttp.test_utils import TestServer

from tests.helpers import auth, eventually
from tests.integration.test_stream_endpoint import hello
from ttfeedhub.hub import Hub
from ttfeedhub.server.app import build_app


async def test_shutdown_closes_clients_with_1001_quickly(
    hub: Hub, http: aiohttp.ClientSession
) -> None:
    server = TestServer(build_app(hub))
    await server.start_server()
    ws = await http.ws_connect(server.make_url("/v1/stream"), headers=auth())
    await hello(ws)
    await eventually(lambda: len(hub.sessions) == 1)

    async def drain() -> None:  # a real client keeps reading, so it answers the close
        while not ws.closed:
            await ws.receive()

    reader = asyncio.create_task(drain())
    t0 = time.monotonic()
    await server.close()
    elapsed = time.monotonic() - t0
    await asyncio.wait_for(reader, 2)
    assert ws.close_code == 1001
    assert elapsed < 5, f"shutdown took {elapsed:.1f}s"
