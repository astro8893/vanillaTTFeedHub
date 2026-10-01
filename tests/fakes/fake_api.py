"""Fake tastytrade REST: /oauth/token and /api-quote-tokens."""

from __future__ import annotations

from typing import Any

from aiohttp import web
from aiohttp.test_utils import TestServer


class FakeApi:
    def __init__(self) -> None:
        self.refresh_status = 200
        self.scope: Any = "read"
        self.quote_url = "ws://127.0.0.1:1/dx"
        self.expires_in: Any = 900
        self.refresh_body_override: str | None = None  # raw text/html 200 body
        self.refresh_calls = 0
        self.quote_calls = 0
        self.last_refresh_body: dict[str, Any] | None = None
        self.last_auth: str | None = None
        self._server: TestServer | None = None

    @property
    def url(self) -> str:
        assert self._server is not None
        return str(self._server.make_url("")).rstrip("/")

    async def start(self) -> None:
        app = web.Application()
        app.router.add_post("/oauth/token", self._oauth)
        app.router.add_get("/api-quote-tokens", self._quote)
        self._server = TestServer(app)
        await self._server.start_server()

    async def close(self) -> None:
        if self._server is not None:
            await self._server.close()

    async def _oauth(self, request: web.Request) -> web.Response:
        self.refresh_calls += 1
        self.last_refresh_body = await request.json()
        if self.refresh_status != 200:
            return web.json_response({"error": "invalid_grant"}, status=self.refresh_status)
        if self.refresh_body_override is not None:
            return web.Response(text=self.refresh_body_override, content_type="text/html")
        body: dict[str, Any] = {
            "access_token": f"access-{self.refresh_calls}-0123456789",
            "expires_in": self.expires_in,
        }
        if self.scope is not None:
            body["scope"] = self.scope
        return web.json_response(body)

    async def _quote(self, request: web.Request) -> web.Response:
        self.quote_calls += 1
        self.last_auth = request.headers.get("Authorization")
        return web.json_response(
            {
                "data": {
                    "token": f"quote-{self.quote_calls}-0123456789",
                    "dxlink-url": self.quote_url,
                }
            }
        )
