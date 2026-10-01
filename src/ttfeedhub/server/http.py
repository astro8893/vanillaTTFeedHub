"""HTTP endpoints: health, stats, snapshot, candles."""

from __future__ import annotations

import time
from typing import Any

import orjson
from aiohttp import web

from ..config import ClientSpec
from ..tokens import TokenError
from ..upstream.connection import UpstreamError
from ..upstream.history import INTERVALS, HistoryTimeout
from .keys import AUTH, HUB, LIMITER
from .validate import BASE_SYMBOL_RE, parse_key_strings, reject

MAX_SNAPSHOT_KEYS = 2000
MAX_HISTORY_MS = 5 * 365 * 86_400_000


def _json(obj: Any, status: int = 200) -> web.Response:
    return web.Response(body=orjson.dumps(obj), status=status, content_type="application/json")


def _require(request: web.Request) -> ClientSpec:
    spec = request.app[AUTH].authenticate(request)
    if spec is None:
        raise web.HTTPUnauthorized(text="unauthorized")
    return spec


async def health(request: web.Request) -> web.Response:
    state = request.app[HUB].state
    ok = state != "auth_failed"
    return _json({"ok": ok, "state": state}, 200 if ok else 503)


async def stats(request: web.Request) -> web.Response:
    _require(request)
    return _json(request.app[HUB].stats())


async def snapshot(request: web.Request) -> web.Response:
    """GET form: `?key=Type:Symbol` repeated. Long lists hit the request-line
    limit well before MAX_SNAPSHOT_KEYS; use POST for those."""
    spec = _require(request)
    return await _snapshot(request, spec, request.query.getall("key", []))


async def snapshot_post(request: web.Request) -> web.Response:
    """POST form: JSON body `{"keys": ["Type:Symbol", ...]}` (64 KB body cap)."""
    spec = _require(request)
    try:
        body = orjson.loads(await request.read())
    except orjson.JSONDecodeError:
        raise web.HTTPBadRequest(text="body must be JSON") from None
    raw = body.get("keys") if isinstance(body, dict) else None
    if not isinstance(raw, list) or not all(isinstance(k, str) for k in raw):
        raise web.HTTPBadRequest(text='body must be {"keys": ["Type:Symbol", ...]}')
    return await _snapshot(request, spec, raw)


async def _snapshot(request: web.Request, spec: ClientSpec, raw: list[str]) -> web.Response:
    hub = request.app[HUB]
    if not raw or len(raw) > MAX_SNAPSHOT_KEYS:
        raise web.HTTPBadRequest(text=f"give 1..{MAX_SNAPSHOT_KEYS} keys")
    if not request.app[LIMITER].allow(f"{spec.name}:snap", spec.snapshots_per_min):
        raise web.HTTPTooManyRequests(text="snapshot rate limit")
    keys, rejected = parse_key_strings(raw, spec.event_types)
    have = {(e["type"], e["symbol"]): e for e in hub.cache.get_many(keys)}
    missing = [k for k in keys if k not in have]
    if missing:
        # One-shot fetches count against the caller's max_keys headroom.
        name = spec.name
        room = max(0, spec.max_keys - hub.keys_by_name[name])
        fetch = missing[:room]
        rejected.extend(reject(t, s, "max_keys") for t, s in missing[room:])
        if fetch:
            hub.keys_by_name[name] += len(fetch)
            try:
                for e in await hub.one_shot(fetch, hub.settings.one_shot_wait_s):
                    have[(e["type"], e["symbol"])] = e
            finally:
                hub.keys_by_name[name] -= len(fetch)
    return _json(
        {
            "d": [have[k] for k in keys if k in have],
            "missing": [f"{t}:{s}" for t, s in keys if (t, s) not in have],
            "rejected": rejected,
        }
    )


async def candles(request: web.Request) -> web.Response:
    spec = _require(request)
    hub = request.app[HUB]
    q = request.query
    if "Candle" not in spec.event_types:
        raise web.HTTPForbidden(text="Candle is not allowed for this client")
    symbol = q.get("symbol", "")
    interval = q.get("interval", "1m")
    if not BASE_SYMBOL_RE.fullmatch(symbol):
        raise web.HTTPBadRequest(text="bad symbol")
    if interval not in INTERVALS:
        raise web.HTTPBadRequest(text=f"interval must be one of {sorted(INTERVALS)}")
    try:
        from_ms = int(q["from"])
        to_ms = int(q["to"]) if "to" in q else None
    except (KeyError, ValueError):
        raise web.HTTPBadRequest(text="from (epoch ms) is required; to is optional") from None
    now_ms = int(time.time() * 1000)
    if from_ms < now_ms - MAX_HISTORY_MS or (to_ms is not None and to_ms < from_ms):
        raise web.HTTPBadRequest(text="bad time range")
    if not request.app[LIMITER].allow(spec.name, spec.candles_per_min):
        raise web.HTTPTooManyRequests(text="candle rate limit")
    try:
        bars = await hub.history.candles(
            symbol, interval, from_ms, to_ms, tho=q.get("tho") in ("1", "true")
        )
    except HistoryTimeout:
        raise web.HTTPGatewayTimeout(text="history request timed out") from None
    except (UpstreamError, TokenError):
        raise web.HTTPBadGateway(text="upstream unavailable") from None
    return _json({"d": bars})
