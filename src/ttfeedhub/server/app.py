"""aiohttp application factory."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from aiohttp import WSCloseCode, web

from ..hub import Hub
from .auth import Authenticator, RateLimiter
from .http import candles, health, snapshot, snapshot_post, stats
from .keys import AUTH, HUB, LIMITER
from .stream import stream_handler

SHUTDOWN_CLOSE_S = 2.0


def build_app(hub: Hub) -> web.Application:
    app = web.Application(client_max_size=64 * 1024)
    app[HUB] = hub
    app[AUTH] = Authenticator(hub.clients)
    app[LIMITER] = RateLimiter()
    app.router.add_get("/v1/stream", stream_handler)
    app.router.add_get("/health", health)
    app.router.add_get("/v1/stats", stats)
    app.router.add_get("/v1/snapshot", snapshot)
    app.router.add_post("/v1/snapshot", snapshot_post)
    app.router.add_get("/v1/candles", candles)

    async def _going_away(_: web.Application) -> None:
        """Close every client websocket (1001) so aiohttp need not wait out its
        per-handler shutdown timeout, and Docker's stop window is enough."""
        sessions = list(hub.sessions)
        if not sessions:
            return
        try:
            async with asyncio.timeout(SHUTDOWN_CLOSE_S):
                await asyncio.gather(
                    *(
                        s.ws.close(code=WSCloseCode.GOING_AWAY, message=b"server shutdown")
                        for s in sessions
                    ),
                    return_exceptions=True,
                )
        except TimeoutError:
            pass

    async def _hub(_: web.Application) -> AsyncIterator[None]:
        # A cleanup context: contexts registered before it (the entry point's
        # HTTP session) are unwound even when hub.start() fails, and after it
        # on a normal stop.
        await hub.start()
        try:
            yield
        finally:
            await hub.stop()

    app.cleanup_ctx.append(_hub)
    app.on_shutdown.append(_going_away)
    return app
