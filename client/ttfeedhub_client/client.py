"""TTfeedhub client: depends only on aiohttp and orjson.

    async with FeedClient("http://ttfeedhub:8700", token, name="my-dashboard") as feed:
        await feed.subscribe([("Quote", "SPX")], mode="latest")
        async for batch in feed.events():
            ...

- Reconnects forever (0.5 s → 10 s backoff) and replays its own subscriptions.
- `state` is "live" only while the hub reports a live feed (or an idle one, when
  this client has no subscriptions) AND frames keep arriving. Otherwise it is
  "stale" or "reconnecting". Reading `state` re-checks frame freshness, so it
  never reads "live" on a silent connection.
  Trading code must not act on data unless the state is "live" AND
  `staleness(key)` is small enough for it: `last()` keeps returning the
  pre-disconnect value while the client is stale or reconnecting.
- A connection that receives no frame for `dead_after` seconds (default
  max(3 * stale_after, 10 s)) is torn down and re-established.
- `on_gap(reason)` fires whenever events may have been missed. Reasons:
  "reconnected", "upstream_reconnect", "evicted_slow", "client_overflow".
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from collections.abc import AsyncIterator, Callable, Iterable
from typing import Any, Literal

import aiohttp
import orjson

VERSION = "0.1.0"
PROTO = 1
Key = tuple[str, str]
State = Literal["connecting", "live", "reconnecting", "stale", "closed"]

log = logging.getLogger("ttfeedhub_client")

_WELCOME_TIMEOUT_S = 5.0
_REQUEST_TIMEOUT_S = 10.0
_MAX_SUBS_PER_MSG = 1000
_CLOSE_SLOW_CONSUMER = 4008
_WS_CLOSE_TIMEOUT_S = 2.0
_DEAD_AFTER_FACTOR = 3
_DEAD_AFTER_MIN_S = 10.0


def _split_key(k: str) -> Key:
    etype, _, symbol = k.partition(":")
    return etype, symbol


class FeedError(Exception):
    pass


class _HubRejected(FeedError):
    """The hub answered a request with an error frame (as opposed to a lost connection)."""


class FeedClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        name: str,
        stale_after: float = 5.0,
        max_pending_batches: int = 10_000,
        backoff_cap: float = 10.0,
        session: aiohttp.ClientSession | None = None,
        dead_after: float | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {token}"}
        self._name = name
        self._stale_after = stale_after
        self._backoff_cap = backoff_cap
        self._dead_after = (
            dead_after
            if dead_after is not None
            else max(_DEAD_AFTER_FACTOR * stale_after, _DEAD_AFTER_MIN_S)
        )
        self._started = False
        self._delay = 0.5
        self._http = session
        self._own_http = session is None
        self._subs: dict[Key, str] = {}
        self._latest: dict[Key, dict[str, Any]] = {}
        self._events: asyncio.Queue[list[dict[str, Any]]] = asyncio.Queue(max_pending_batches)
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._ids = itertools.count(1)
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._last_frame = 0.0
        self._connected_once = False
        self._state_cbs: list[Callable[[State], None]] = []
        self._gap_cbs: list[Callable[[str], None]] = []
        self._state: State = "connecting"
        self.hub_state = "unknown"
        self.generation = 0

    @property
    def state(self) -> State:
        st = self._state
        if st == "live" and (
            time.monotonic() - self._last_frame >= self._stale_after or not self._hub_live()
        ):
            return "stale"  # even if the watchdog has not caught up (or died)
        return st

    async def __aenter__(self) -> FeedClient:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def start(self) -> None:
        if self._started or self._state == "closed":
            raise RuntimeError("FeedClient can only be started once and not after close()")
        self._started = True
        if self._http is None:
            self._http = aiohttp.ClientSession()
        self._tasks = [
            asyncio.create_task(self._run(), name=f"feed-{self._name}"),
            asyncio.create_task(self._watchdog(), name=f"feed-{self._name}-watchdog"),
        ]

    async def close(self) -> None:
        self._set_state("closed")
        tasks, self._tasks = self._tasks, []
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._fail_pending()
        ws, self._ws = self._ws, None
        if ws is not None:
            await ws.close()
        if self._own_http and self._http is not None:
            await self._http.close()
            self._http = None

    def on_state(self, cb: Callable[[State], None]) -> None:
        self._state_cbs.append(cb)

    def on_gap(self, cb: Callable[[str], None]) -> None:
        self._gap_cbs.append(cb)

    async def subscribe(self, keys: Iterable[Key], mode: str = "all") -> list[dict[str, Any]]:
        """Returns the hub's rejections. Keys are remembered and replayed after reconnects."""
        if mode not in ("all", "latest"):
            raise ValueError("mode must be 'all' or 'latest'")
        ks = list(dict.fromkeys(keys))
        for k in ks:
            self._subs[k] = mode
        self._refresh_state()
        rejected: list[dict[str, Any]] = []
        if self._ws is None:
            return rejected  # sent on (re)connect
        try:
            for i in range(0, len(ks), _MAX_SUBS_PER_MSG):
                chunk = ks[i : i + _MAX_SUBS_PER_MSG]
                ack = await self._request(
                    {
                        "op": "sub",
                        "mode": mode,
                        "subs": [{"type": t, "symbol": s} for t, s in chunk],
                    }
                )
                rejected.extend(ack.get("rejected") or [])
        except _HubRejected:
            for k in ks[i:]:
                self._subs.pop(k, None)
            self._refresh_state()
            raise
        except FeedError as e:
            log.warning("subscribe interrupted (%s); will replay on reconnect", e)
        for r in rejected:
            self._subs.pop((str(r.get("type")), str(r.get("symbol"))), None)
        self._refresh_state()
        return rejected

    async def unsubscribe(self, keys: Iterable[Key]) -> None:
        ks = [k for k in dict.fromkeys(keys) if self._subs.pop(k, None) is not None]
        for k in ks:
            self._latest.pop(k, None)
        self._refresh_state()
        if self._ws is None or not ks:
            return
        try:
            for i in range(0, len(ks), _MAX_SUBS_PER_MSG):
                chunk = ks[i : i + _MAX_SUBS_PER_MSG]
                await self._request(
                    {"op": "unsub", "subs": [{"type": t, "symbol": s} for t, s in chunk]}
                )
        except FeedError as e:
            log.warning("unsubscribe interrupted (%s)", e)

    async def events(self) -> AsyncIterator[list[dict[str, Any]]]:
        while True:
            yield await self._events.get()

    def last(self, key: Key) -> dict[str, Any] | None:
        return self._latest.get(key)

    def staleness(self, key: Key) -> float | None:
        ev = self._latest.get(key)
        return None if ev is None else max(0.0, time.time() - float(ev["rt"]))

    async def snapshot(self, keys: Iterable[Key]) -> dict[Key, dict[str, Any]]:
        return (await self.snapshot_detail(keys))[0]

    async def snapshot_detail(
        self, keys: Iterable[Key]
    ) -> tuple[dict[Key, dict[str, Any]], list[Key], list[Key]]:
        """(events, missing, rejected). `missing` has no data after the hub's one-shot
        fetch; `rejected` was refused (type, symbol or max_keys headroom). A key refused
        for max_keys headroom is in both lists."""
        # POST: 2,000 keys don't fit in a GET request line.
        body = await self._http_json(
            "POST", "/v1/snapshot", json={"keys": [f"{t}:{s}" for t, s in keys]}
        )
        events = {(e["type"], e["symbol"]): e for e in body.get("d") or []}
        # "Type:Symbol": types never contain ':', symbols may ("/ESZ26:XCME").
        missing = [_split_key(k) for k in body.get("missing") or [] if isinstance(k, str)]
        rejected = [
            (str(r.get("type") or ""), str(r.get("symbol") or ""))
            for r in body.get("rejected") or []
            if isinstance(r, dict)
        ]
        return events, missing, rejected

    async def candles(
        self,
        symbol: str,
        interval: str = "1m",
        *,
        from_ms: int,
        to_ms: int | None = None,
        tho: bool = False,
    ) -> list[dict[str, Any]]:
        params = [("symbol", symbol), ("interval", interval), ("from", str(from_ms))]
        if to_ms is not None:
            params.append(("to", str(to_ms)))
        if tho:
            params.append(("tho", "1"))
        return list((await self._http_json("GET", "/v1/candles", params=params)).get("d", []))

    # ---- internals -------------------------------------------------------

    async def _http_json(
        self,
        method: str,
        path: str,
        *,
        params: list[tuple[str, str]] | None = None,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        http = self._http
        if http is None:
            raise FeedError("client not started")
        async with http.request(
            method,
            self._base + path,
            params=params,
            data=None if json is None else orjson.dumps(json),
            headers=self._headers
            if json is None
            else {**self._headers, "Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as r:
            if r.status != 200:
                raise FeedError(f"{method} {path}: HTTP {r.status} {(await r.text())[:200]}")
            data = orjson.loads(await r.read())
            return data if isinstance(data, dict) else {}

    async def _run(self) -> None:
        while True:
            try:
                await self._connect_once()
            except asyncio.CancelledError:
                raise
            except aiohttp.WSServerHandshakeError as e:
                log.error("ttfeedhub refused the connection: HTTP %s", e.status)
            except (aiohttp.ClientError, OSError, TimeoutError, FeedError, ValueError) as e:
                log.warning("ttfeedhub connection lost: %s", e)
            except Exception:
                log.exception("ttfeedhub client hit an unexpected error; reconnecting")
            self._ws = None
            self._fail_pending()
            if self._state == "closed":
                return
            self._set_state("reconnecting")
            await asyncio.sleep(self._delay)
            self._delay = min(self._delay * 2, self._backoff_cap)

    async def _connect_once(self) -> None:
        http = self._http
        if http is None:
            raise FeedError("client not started")
        async with http.ws_connect(
            f"{self._base}/v1/stream",
            headers=self._headers,
            autoping=True,
            heartbeat=None,
            compress=0,
            max_msg_size=64 * 1024 * 1024,
            timeout=aiohttp.ClientWSTimeout(ws_close=_WS_CLOSE_TIMEOUT_S),
        ) as ws:
            await ws.send_str(
                orjson.dumps({"op": "hello", "client": self._name, "proto": PROTO}).decode()
            )
            async with asyncio.timeout(_WELCOME_TIMEOUT_S):
                welcome = self._decode(await ws.receive())
            if welcome.get("t") != "welcome":
                raise FeedError(f"expected welcome, got {welcome.get('t')!r}")
            self._delay = 0.5
            self._last_frame = time.monotonic()
            if self._connected_once:
                self._gap("reconnected")
            self._connected_once = True
            self._ws = ws
            self._on_hub_status(welcome)
            try:
                await self._replay(ws)
                await self._reader(ws)
            finally:
                # Leave "live" the moment the reader stops, before the `async with`
                # exit awaits the close handshake.
                self._detach(ws)

    async def _replay(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        for mode in ("all", "latest"):
            keys = [k for k, m in self._subs.items() if m == mode]
            for i in range(0, len(keys), _MAX_SUBS_PER_MSG):
                chunk = keys[i : i + _MAX_SUBS_PER_MSG]
                msg = {
                    "op": "sub",
                    "id": next(self._ids),
                    "mode": mode,
                    "subs": [{"type": t, "symbol": s} for t, s in chunk],
                }
                await ws.send_str(orjson.dumps(msg).decode())

    async def _reader(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        async for m in ws:
            if m.type is not aiohttp.WSMsgType.TEXT:
                continue
            self._last_frame = time.monotonic()
            msg = self._decode(m)
            t = msg.get("t")
            if t in ("ev", "snap"):
                batch = msg.get("d") or []
                latest = self._latest
                for ev in batch:
                    latest[(ev["type"], ev["symbol"])] = ev
                try:
                    self._events.put_nowait(batch)
                except asyncio.QueueFull:
                    # The consumer isn't draining events(): drop the backlog so the next
                    # connection starts with room, and never read "live" while closing.
                    self._gap("client_overflow")
                    while not self._events.empty():
                        self._events.get_nowait()
                    await self._drop_ws(ws)
                    return
            elif t == "status":
                self._on_hub_status(msg)
            elif t in ("ack", "pong", "error"):
                self._resolve(msg)
            self._refresh_state()
        if ws.close_code == _CLOSE_SLOW_CONSUMER:
            self._gap("evicted_slow")

    def _on_hub_status(self, msg: dict[str, Any]) -> None:
        self.hub_state = str(msg.get("state", "unknown"))
        gen = msg.get("gen")
        if isinstance(gen, int) and gen:
            if self.generation and gen != self.generation:
                self._gap("upstream_reconnect")
            self.generation = gen
        self._refresh_state()

    def _hub_live(self) -> bool:
        # After a hub restart the welcome says idle/gen 0 while we still hold
        # subscriptions: that is not live until the hub says so.
        return self.hub_state == "live" or (self.hub_state == "idle" and not self._subs)

    def _refresh_state(self) -> None:
        if self._state == "closed" or self._ws is None:
            return
        fresh = time.monotonic() - self._last_frame < self._stale_after
        self._set_state("live" if fresh and self._hub_live() else "stale")

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(min(0.25, self._stale_after / 4))
            try:
                self._refresh_state()
                ws = self._ws
                if ws is not None and time.monotonic() - self._last_frame > self._dead_after:
                    log.warning("no frames for %.1fs; dropping the connection", self._dead_after)
                    await self._drop_ws(ws)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("ttfeedhub client watchdog iteration failed")

    def _detach(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        if self._ws is ws:
            self._ws = None
        if self._state != "closed":
            self._set_state("reconnecting")

    async def _drop_ws(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Detach and close `ws` so the run loop reconnects; state leaves "live" first."""
        self._detach(ws)
        await ws.close()

    def _resolve(self, msg: dict[str, Any]) -> None:
        mid = msg.get("id")
        fut = self._pending.pop(mid, None) if isinstance(mid, int) else None
        if fut is not None and not fut.done():
            fut.set_result(msg)
        elif msg.get("t") == "ack":
            for r in msg.get("rejected") or []:
                log.warning(
                    "ttfeedhub rejected %s:%s (%s)", r.get("type"), r.get("symbol"), r.get("reason")
                )
                self._subs.pop((str(r.get("type")), str(r.get("symbol"))), None)
        elif msg.get("t") == "error":
            log.warning("ttfeedhub error: %s", msg.get("reason"))

    async def _request(self, msg: dict[str, Any]) -> dict[str, Any]:
        ws = self._ws
        if ws is None:
            raise FeedError("not connected")
        mid = next(self._ids)
        msg["id"] = mid
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[mid] = fut
        try:
            await ws.send_str(orjson.dumps(msg).decode())
            async with asyncio.timeout(_REQUEST_TIMEOUT_S):
                reply = await fut
        except TimeoutError:
            raise FeedError("request timed out") from None
        except (aiohttp.ClientError, OSError):
            raise FeedError("disconnected") from None
        finally:
            self._pending.pop(mid, None)
        if reply.get("t") == "error":
            raise _HubRejected(str(reply.get("reason")))
        return reply

    def _fail_pending(self) -> None:
        pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(FeedError("disconnected"))

    @staticmethod
    def _decode(m: aiohttp.WSMessage) -> dict[str, Any]:
        if m.type is not aiohttp.WSMsgType.TEXT:
            raise FeedError(f"connection closed ({m.type.name})")
        msg = orjson.loads(m.data)
        if not isinstance(msg, dict):
            raise FeedError("non-object frame")
        return msg

    def _gap(self, reason: str) -> None:
        for cb in self._gap_cbs:
            try:
                cb(reason)
            except Exception:
                log.exception("on_gap callback failed")

    def _set_state(self, state: State) -> None:
        if state == self._state:
            return
        self._state = state
        for cb in self._state_cbs:
            try:
                cb(state)
            except Exception:
                log.exception("on_state callback failed")
