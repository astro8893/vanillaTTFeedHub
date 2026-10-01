"""In-process DXLink server that speaks the real protocol, with failure knobs."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import orjson
from aiohttp import WSMsgType, web
from aiohttp.test_utils import TestServer

from ttfeedhub.types import Key
from ttfeedhub.upstream.protocol import FIELDS

SESSION_LIMIT_MSG = "The number of user sessions has exceeded the configured limit"
# Observed live (KI-002): what real DXLink sent first on the live socket when the
# quote token expired, then on every AUTH that reused the expired token.
TOKEN_EXPIRED_MSG = "Your authentication token has expired, reauthentication is required"
AUTH_FAILED_MSG = "Authentication failed"
# Real dxFeed drops a period multiplier of 1: SPX{=1m,tho=true} -> SPX{=m,tho=true}.
_ONE_PERIOD_RE = re.compile(r"(?<=[{,]=)1(?=(?:mo|[smhdwy])[,}])")


def dx_normalize(etype: str, symbol: str) -> str:
    """The candle symbol as real dxFeed labels its events (deliberately independent
    of the hub's own canonicalization)."""
    return _ONE_PERIOD_RE.sub("", symbol) if etype == "Candle" else symbol


def row(etype: str, symbol: str, **values: Any) -> list[Any]:
    vals: dict[str, Any] = {"eventSymbol": symbol, **values}
    return [vals.get(f, 0) for f in FIELDS[etype]]


def quote_row(symbol: str, bid: float, ask: float) -> list[Any]:
    return row("Quote", symbol, bidPrice=bid, askPrice=ask, bidSize=1, askSize=1)


def trade_row(symbol: str, price: float, size: float = 1) -> list[Any]:
    return row("Trade", symbol, price=price, size=size)


def candle_row(csym: str, t: int, flags: int, o: Any, h: Any, lo: Any, c: Any, v: Any) -> list[Any]:
    return row(
        "Candle",
        csym,
        eventFlags=flags,
        index=t,
        time=t,
        open=o,
        high=h,
        low=lo,
        close=c,
        volume=v,
        vwap="NaN",
        impVolatility="NaN",
    )


@dataclass(eq=False)
class FakeConn:
    ws: web.WebSocketResponse
    authorized: bool = False
    token: str | None = None  # the token of the last AUTH
    subs: dict[Key, int | None] = field(default_factory=dict)
    received: list[dict[str, Any]] = field(default_factory=list)
    keepalives: int = 0


class FakeDxLink:
    def __init__(self) -> None:
        self.valid_tokens: set[str] | None = None  # None: any non-empty token is valid
        self.max_sockets: int | None = None
        self.session_limit = False
        self.stall_handshake = False
        self.silent = False
        self.max_sub_entries = 50
        # tokens DXLink no longer accepts: AUTH with one gets an ERROR (not AUTH_STATE)
        self.rejected_tokens: set[str] = set()
        self.reject_all_tokens = False
        self.auth_error_code = "UNAUTHORIZED"  # the ERROR frame's "error" field
        # candle symbol (e.g. "SPX{=1d}"; matched as dxFeed normalizes it) -> bars
        self.candles: dict[str, list[tuple[int, float, float, float, float, float]]] = {}
        self.auto_rows: dict[Key, list[Any]] = {}  # emitted immediately on subscribe
        self.conns: list[FakeConn] = []
        self.connect_attempts = 0
        self.peak_open = 0
        self.candle_requests: list[tuple[str, int]] = []
        self._server: TestServer | None = None

    @property
    def url(self) -> str:
        assert self._server is not None
        return str(self._server.make_url("/dx")).replace("http://", "ws://")

    @property
    def open_count(self) -> int:
        return len(self.conns)

    def all_subs(self) -> set[Key]:
        return {k for c in self.conns for k in c.subs}

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/dx", self._handler)
        self._server = TestServer(app)
        await self._server.start_server()

    async def close(self) -> None:
        for c in list(self.conns):
            await c.ws.close()
        if self._server is not None:
            await self._server.close()

    async def emit(self, etype: str, rows: list[list[Any]]) -> None:
        rows = [[dx_normalize(etype, r[0]), *r[1:]] for r in rows]
        for c in list(self.conns):
            mine = [r for r in rows if (etype, r[0]) in c.subs]
            if mine and c.authorized:
                await self._send(
                    c,
                    {
                        "type": "FEED_DATA",
                        "channel": 1,
                        "data": [etype, [v for r in mine for v in r]],
                    },
                )

    async def drop_all(self, code: int = 1011) -> None:
        for c in list(self.conns):
            await c.ws.close(code=code)

    async def unauthorize(self) -> None:
        for c in list(self.conns):
            c.authorized = False
            await self._send(c, {"type": "AUTH_STATE", "channel": 0, "state": "UNAUTHORIZED"})

    async def expire_token(self, token: str) -> None:
        """The token expires: every socket authed with it gets DXLink's ERROR, and
        any later AUTH with it is refused with an ERROR as well."""
        self.rejected_tokens.add(token)
        for c in list(self.conns):
            if c.token == token:
                c.authorized = False
                await self._send(c, self._auth_error(TOKEN_EXPIRED_MSG))

    def _auth_error(self, message: str) -> dict[str, Any]:
        return {"type": "ERROR", "channel": 0, "error": self.auth_error_code, "message": message}

    async def _send(self, c: FakeConn, msg: dict[str, Any]) -> None:
        if not self.silent and not c.ws.closed:
            await c.ws.send_str(orjson.dumps(msg).decode())

    async def _handler(self, request: web.Request) -> web.WebSocketResponse:
        self.connect_attempts += 1
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        c = FakeConn(ws)
        self.conns.append(c)
        self.peak_open = max(self.peak_open, len(self.conns))
        try:
            if self.session_limit or (
                self.max_sockets is not None and len(self.conns) > self.max_sockets
            ):
                await ws.send_str(
                    orjson.dumps(
                        {
                            "type": "ERROR",
                            "channel": 0,
                            "error": "UNAUTHORIZED",
                            "message": SESSION_LIMIT_MSG,
                        }
                    ).decode()
                )
                await ws.close()
                return ws
            async for m in ws:
                if m.type is not WSMsgType.TEXT:
                    continue
                msg = orjson.loads(m.data)
                c.received.append(msg)
                await self._on_msg(c, msg)
        finally:
            self.conns.remove(c)
        return ws

    async def _on_msg(self, c: FakeConn, msg: dict[str, Any]) -> None:
        t = msg.get("type")
        if t == "KEEPALIVE":
            c.keepalives += 1
            return
        if self.stall_handshake:
            return
        if t == "SETUP":
            await self._send(
                c,
                {
                    "type": "SETUP",
                    "channel": 0,
                    "version": "fake",
                    "keepaliveTimeout": 60,
                    "acceptKeepaliveTimeout": 60,
                },
            )
            await self._send(c, {"type": "AUTH_STATE", "channel": 0, "state": "UNAUTHORIZED"})
        elif t == "AUTH":
            tok = msg.get("token")
            c.token = tok
            if self.reject_all_tokens or tok in self.rejected_tokens:
                c.authorized = False
                await self._send(c, self._auth_error(AUTH_FAILED_MSG))
                return
            ok = bool(tok) and (self.valid_tokens is None or tok in self.valid_tokens)
            c.authorized = ok
            await self._send(
                c,
                {
                    "type": "AUTH_STATE",
                    "channel": 0,
                    "state": "AUTHORIZED" if ok else "UNAUTHORIZED",
                },
            )
        elif t == "CHANNEL_REQUEST":
            await self._send(
                c,
                {
                    "type": "CHANNEL_OPENED",
                    "channel": msg["channel"],
                    "service": "FEED",
                    "parameters": {"contract": "AUTO"},
                },
            )
        elif t == "FEED_SETUP":
            await self._send(
                c,
                {
                    "type": "FEED_CONFIG",
                    "channel": 1,
                    "dataFormat": "COMPACT",
                    "eventFields": msg.get("acceptEventFields", {}),
                },
            )
        elif t == "FEED_SUBSCRIPTION":
            add = msg.get("add") or []
            rem = msg.get("remove") or []
            if len(add) > self.max_sub_entries or len(rem) > self.max_sub_entries:
                await c.ws.close(code=1009)
                return
            if msg.get("reset"):
                c.subs.clear()
            for e in rem:
                c.subs.pop((e["type"], dx_normalize(e["type"], e["symbol"])), None)
            for e in add:
                key = (e["type"], dx_normalize(e["type"], e["symbol"]))
                c.subs[key] = e.get("fromTime")
                auto = next(
                    (r for (t, s), r in self.auto_rows.items() if (t, dx_normalize(t, s)) == key),
                    None,
                )
                if auto is not None:
                    await self._send(
                        c,
                        {
                            "type": "FEED_DATA",
                            "channel": 1,
                            "data": [key[0], [key[1], *auto[1:]]],
                        },
                    )
                if key[0] == "Candle" and e.get("fromTime") is not None:
                    self.candle_requests.append((e["symbol"], e["fromTime"]))  # as sent
                    await self._send_candles(c, key[1], e["fromTime"])

    async def _send_candles(self, c: FakeConn, csym: str, from_ms: int) -> None:
        known = {dx_normalize("Candle", k): v for k, v in self.candles.items()}
        if csym not in known:
            return  # unknown symbol: silence (lets tests exercise timeouts)
        bars = sorted((b for b in known[csym] if b[0] >= from_ms), reverse=True)
        rows = [
            candle_row(csym, t, 0x04 if i == 0 else 0, o, h, lo, cl, v)
            for i, (t, o, h, lo, cl, v) in enumerate(bars)
        ]
        rows.append(candle_row(csym, from_ms, 0x08 | 0x02, "NaN", "NaN", "NaN", "NaN", "NaN"))
        await self._send(
            c, {"type": "FEED_DATA", "channel": 1, "data": ["Candle", [v for r in rows for v in r]]}
        )
