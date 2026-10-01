"""Candle history over a small, capped pool of DXLink sockets.

A request waits for a socket, and each socket serves one request at a time.
Idle sockets are closed after `idle_close_s`. Past bars are cached (LRU,
sized by approximate bytes), so a repeat request only fetches the gap since
the newest cached bar.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import aiohttp

from ..symbols import canonical_candle
from ..tokens import TokenManager
from ..types import Event
from . import protocol
from .connection import QuoteTokenRejected, SessionBudget, UpstreamConnection, UpstreamError

log = logging.getLogger(__name__)

INTERVALS: dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "1h": 3_600_000,
    "1d": 86_400_000,
}
_REMOVE, _SNAP_END, _SNAP_SNIP = 0x02, 0x08, 0x10
# Measured with tracemalloc: one cached bar dict (7 keys, float values) plus its
# dict-entry and int key costs ~480 B in CPython; round up so the cap is honest.
_BAR_BYTES = 512
_FAR_FUTURE = 2**62


class HistoryTimeout(Exception):
    pass


def candle_symbol(symbol: str, interval: str, tho: bool = False) -> str:
    """The canonical candle symbol: dxFeed labels `SPX{=1m}` bars as `SPX{=m}`."""
    return canonical_candle(f"{symbol}{{={interval}{',tho=true' if tho else ''}}}")


@dataclass
class _Series:
    bars: dict[int, dict[str, Any]] = field(default_factory=dict)
    covered_from: int = _FAR_FUTURE  # earliest time fetched completely


class _Route:
    """Routes a connection's events to whichever request is using it."""

    def __init__(self) -> None:
        self.collect: Callable[[list[Event]], None] | None = None

    def __call__(self, events: list[Event]) -> None:
        if self.collect is not None:
            self.collect(events)


class HistoryPool:
    def __init__(
        self,
        *,
        tokens: TokenManager,
        http: aiohttp.ClientSession,
        budget: SessionBudget,
        max_sockets: int = 2,
        timeout_s: float = 20.0,
        idle_close_s: float = 60.0,
        cache_bytes: int = 256 * 1024 * 1024,
        handshake_timeout_s: float = 30.0,
        silence_timeout_s: float = 60.0,
        quiet_s: float = 2.5,
    ) -> None:
        self._tokens = tokens
        self._http = http
        self._budget = budget
        self._slots = asyncio.Semaphore(max_sockets)
        self._timeout_s = timeout_s
        self._idle_close_s = idle_close_s
        self._cache_bytes = cache_bytes
        self._handshake_s = handshake_timeout_s
        self._silence_s = silence_timeout_s
        self._quiet_s = quiet_s
        self._idle: list[tuple[UpstreamConnection, _Route, float]] = []
        self._cache: OrderedDict[str, _Series] = OrderedDict()
        self._names = itertools.count(1)
        self._reaper: asyncio.Task[None] | None = None
        self._closed = False
        self._inflight: set[UpstreamConnection] = set()
        self._closing: set[UpstreamConnection] = set()
        self.requests = 0
        self.gap_fetches = 0

    async def candles(
        self,
        symbol: str,
        interval: str,
        from_ms: int,
        to_ms: int | None = None,
        *,
        tho: bool = False,
    ) -> list[dict[str, Any]]:
        if self._closed:
            raise UpstreamError("history pool is closed")
        step = INTERVALS[interval]
        csym = candle_symbol(symbol, interval, tho)
        self.requests += 1
        series = self._cache.get(csym)
        if series is not None and series.bars and series.covered_from <= from_ms:
            fetch_from = max(from_ms, max(series.bars) - step)
            self.gap_fetches += 1
        else:
            series = _Series()
            fetch_from = from_ms
        bars, complete = await self._fetch(csym, fetch_from)
        series.bars.update(bars)
        if complete:
            series.covered_from = min(series.covered_from, fetch_from)
        elif bars:
            series.covered_from = min(series.covered_from, min(bars))
        self._cache[csym] = series
        self._cache.move_to_end(csym)
        self._evict()
        hi = to_ms if to_ms is not None else _FAR_FUTURE
        return [series.bars[t] for t in sorted(series.bars) if from_ms <= t <= hi]

    async def close(self) -> None:
        self._closed = True
        if self._reaper is not None:
            self._reaper.cancel()
            await asyncio.gather(self._reaper, return_exceptions=True)
            self._reaper = None
        idle, self._idle = self._idle, []
        await self._close_conns(
            [c for c, _, _ in idle] + list(self._inflight) + list(self._closing)
        )

    def stats(self) -> dict[str, Any]:
        return {
            "idle_sockets": len(self._idle),
            "series": len(self._cache),
            "requests": self.requests,
            "gap_fetches": self.gap_fetches,
        }

    # ---- internals -------------------------------------------------------

    async def _fetch(self, csym: str, from_ms: int) -> tuple[dict[int, dict[str, Any]], bool]:
        # One deadline covers queueing, budget wait, handshake and the data wait.
        cm = asyncio.timeout(self._timeout_s)
        try:
            async with cm:
                return await self._fetch_locked(csym, from_ms)
        except TimeoutError:
            if cm.expired():
                raise HistoryTimeout(f"no candles for {csym} within {self._timeout_s:g}s") from None
            raise

    async def _fetch_locked(
        self, csym: str, from_ms: int
    ) -> tuple[dict[int, dict[str, Any]], bool]:
        async with self._slots:
            conn, route = await self._checkout()
            self._inflight.add(conn)
            if self._closed:  # close() ran while we were opening
                self._inflight.discard(conn)
                await conn.close()
                raise UpstreamError("history pool is closed")
            loop = asyncio.get_running_loop()
            bars: dict[int, dict[str, Any]] = {}
            finished = asyncio.Event()
            complete = False
            last = loop.time()

            def collect(events: list[Event]) -> None:
                nonlocal complete, last
                for ev in events:
                    sym = ev.get("symbol")
                    if (
                        ev.get("type") != "Candle"
                        or not isinstance(sym, str)
                        or canonical_candle(sym) != csym
                    ):
                        continue
                    last = loop.time()
                    flags = int(ev.get("eventFlags") or 0)
                    if flags & (_SNAP_END | _SNAP_SNIP):
                        complete = bool(flags & _SNAP_END) and not flags & _SNAP_SNIP
                        finished.set()
                    t = ev.get("time")
                    if (
                        flags & _REMOVE
                        or t is None
                        or ev.get("open") is None
                        or ev.get("close") is None
                    ):
                        continue
                    bars[int(t)] = {
                        "t": int(t),
                        "o": ev["open"],
                        "h": ev.get("high"),
                        "l": ev.get("low"),
                        "c": ev["close"],
                        "v": ev.get("volume") or 0,
                        "vwap": ev.get("vwap"),
                    }

            route.collect = collect
            reusable = False
            try:
                await conn.subscribe(add=[protocol.sub_entry(("Candle", csym), from_ms)])
                while not finished.is_set():
                    try:
                        async with asyncio.timeout(self._quiet_s):
                            await finished.wait()
                    except TimeoutError:
                        if not conn.live:  # pool closed or socket died mid-request
                            raise UpstreamError(
                                f"history socket lost while fetching {csym}"
                            ) from None
                        if bars and loop.time() - last >= self._quiet_s:
                            break  # data stopped arriving without an end flag
                # The overall deadline (see _fetch) ends a wait that yields no bars.
                try:
                    await conn.subscribe(remove=[protocol.sub_entry(("Candle", csym))])
                    reusable = conn.live and not self._closed
                except (UpstreamError, aiohttp.ClientError, OSError) as e:
                    log.debug(
                        "history: unsubscribe failed: %r", e
                    )  # bars are good; drop the socket
                return bars, complete
            finally:
                route.collect = None
                try:
                    if reusable:
                        self._checkin(conn, route)
                    else:
                        await conn.close()
                finally:
                    self._inflight.discard(conn)

    async def _checkout(self) -> tuple[UpstreamConnection, _Route]:
        while self._idle:
            conn, route, _ = self._idle.pop()
            if conn.live:
                return conn, route
            await conn.close()
        route = _Route()
        # A refused quote token is dropped by the connection, so one retry fetches a
        # fresh token (KI-002). Only one: a fresh token refused too is a real failure.
        for attempt in range(2):
            conn = UpstreamConnection(
                name=f"history{next(self._names)}",
                generation=0,
                tokens=self._tokens,
                http=self._http,
                budget=self._budget,
                on_events=route,
                handshake_timeout_s=self._handshake_s,
                silence_timeout_s=self._silence_s,
            )
            try:
                await conn.open()
            except QuoteTokenRejected as e:
                if attempt:
                    raise
                log.info("history: quote token refused (%s); retrying with a fresh one", e)
                continue
            return conn, route
        raise AssertionError("unreachable")  # pragma: no cover

    def _checkin(self, conn: UpstreamConnection, route: _Route) -> None:
        self._idle.append((conn, route, time.monotonic()))
        if self._reaper is None or self._reaper.done():
            self._reaper = asyncio.create_task(self._reap(), name="history-reaper")

    async def _reap(self) -> None:
        while self._idle:
            await asyncio.sleep(self._idle_close_s / 2)
            now = time.monotonic()
            expired = [i for i in self._idle if now - i[2] >= self._idle_close_s]
            self._idle = [i for i in self._idle if now - i[2] < self._idle_close_s]
            await self._close_conns([c for c, _, _ in expired])

    async def _close_conns(self, conns: list[UpstreamConnection]) -> None:
        """Close a batch at once. Until every close returns, the conns stay in
        `_closing`, so a cancelled caller cannot orphan them (close() sweeps it)."""
        self._closing.update(conns)
        await asyncio.gather(*(c.close() for c in conns), return_exceptions=True)
        self._closing.difference_update(conns)

    def _evict(self) -> None:
        total = sum(len(s.bars) for s in self._cache.values()) * _BAR_BYTES
        while total > self._cache_bytes and len(self._cache) > 1:
            _, old = self._cache.popitem(last=False)
            total -= len(old.bars) * _BAR_BYTES
