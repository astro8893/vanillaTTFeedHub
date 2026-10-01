"""The stream sockets (0..max_sockets).

Keys are placed first-fit and stay on their socket until they are removed,
so a reconnect never moves them. Each shard runs its own reconnect loop and
resubscribes its whole key set from the master list. A shard with no keys
holds no socket.
"""

from __future__ import annotations

import asyncio
import functools
import itertools
import logging
import random
import re
import time
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Literal

import aiohttp

from ..tokens import AuthFailed, TokenError, TokenManager
from ..types import Event, Key
from . import protocol
from .connection import (
    EventCallback,
    SessionBudget,
    SessionLimitError,
    UpstreamConnection,
    UpstreamError,
)

log = logging.getLogger(__name__)

State = Literal["idle", "connecting", "live", "reconnecting", "auth_failed"]
HEALTHY_RESET_S = 60.0

_DAY_MS = 86_400_000
_UNIT_MS = {"s": 1000, "m": 60_000, "h": 3_600_000, "d": _DAY_MS, "w": 7 * _DAY_MS}
# The aggregation period attribute of a candle symbol: SPX{=5m,tho=true}, SPX{=d}
_PERIOD_RE = re.compile(r"[{,]=(\d*)([smhdw])(?=[,}])")


def candle_period_ms(symbol: str) -> int:
    """Bar length from the `{=N<unit>}` attribute; 1 day when absent or unparseable."""
    m = _PERIOD_RE.search(symbol)
    if m is None:
        return _DAY_MS
    n = int(m.group(1)) if m.group(1) else 1
    return n * _UNIT_MS[m.group(2)] if n > 0 else _DAY_MS


def candle_from_time(symbol: str, now_ms: int) -> int:
    """Start of the bar in progress: a bar's `time` is its start, so fromTime=now
    would leave the current bar out."""
    period = candle_period_ms(symbol)
    return now_ms // period * period


def _now_ms() -> int:
    return int(time.time() * 1000)


class Backoff:
    def __init__(
        self,
        base: float = 1.0,
        cap: float = 30.0,
        jitter: float = 0.2,
        rng: Callable[[], float] = random.random,  # nosec B311 - jitter, not security
    ) -> None:
        self._base, self._cap, self._jitter, self._rng = base, cap, jitter, rng
        self._n = 0

    def next(self) -> float:
        delay = min(self._cap, self._base * (2.0 ** min(self._n, 20)))
        self._n += 1
        return delay * (1.0 + self._jitter * (2.0 * self._rng() - 1.0))

    def reset(self) -> None:
        self._n = 0


def _limit_backoff() -> Backoff:
    return Backoff(base=30.0, cap=300.0)


class _Shard:
    def __init__(self, idx: int) -> None:
        self.idx = idx
        self.keys: dict[Key, int | None] = {}  # key -> fromTime (Candle only)
        self.conn: UpstreamConnection | None = None
        self.task: asyncio.Task[None] | None = None
        self.state: State = "idle"
        self.generation = 0
        self.live = False  # resubscribed and delivering
        self.stopping: asyncio.Event | None = None  # set while _stop is tearing down
        # Serialises subscription sends (each may be several chunks) on this shard.
        self.send_lock = asyncio.Lock()

    def entries(self, keys: Iterable[Key]) -> list[dict[str, Any]]:
        return [protocol.sub_entry(k, self.keys.get(k)) for k in keys]


class StreamPool:
    def __init__(
        self,
        *,
        tokens: TokenManager,
        http: aiohttp.ClientSession,
        budget: SessionBudget,
        on_events: EventCallback,
        on_state: Callable[[State, int], None],
        per_socket: int,
        max_sockets: int,
        handshake_timeout_s: float,
        silence_timeout_s: float,
        keepalive_s: float = 30.0,
        auth_retry_s: float = 300.0,
        backoff_factory: Callable[[], Backoff] = Backoff,
        limit_backoff_factory: Callable[[], Backoff] = _limit_backoff,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._tokens = tokens
        self._http = http
        self._budget = budget
        self._on_events = on_events
        self._on_state = on_state
        self._per_socket = per_socket
        self._max_sockets = max_sockets
        self._handshake_s = handshake_timeout_s
        self._silence_s = silence_timeout_s
        self._keepalive_s = keepalive_s
        self._auth_retry_s = auth_retry_s
        self._backoff_factory = backoff_factory
        self._limit_backoff_factory = limit_backoff_factory
        self._sleep = sleep
        self._clock = clock
        self._shards: list[_Shard] = []
        self._where: dict[Key, _Shard] = {}
        self._gen = itertools.count(1)
        self.generation = 0
        self._state: State = "idle"
        self._closed = False

    @property
    def state(self) -> State:
        return self._state

    def keys(self) -> set[Key]:
        return set(self._where)

    def stats(self) -> list[dict[str, Any]]:
        return [
            {
                "socket": sh.idx,
                "state": sh.state,
                "generation": sh.generation,
                "keys": len(sh.keys),
                "events_in": sh.conn.events_in if sh.conn else 0,
            }
            for sh in self._shards
        ]

    async def add(self, keys: Iterable[Key]) -> list[Key]:
        """Subscribe upstream. Returns the keys rejected because every socket is full,
        or all of them once the pool is closed (nothing is opened after close())."""
        if self._closed:
            return list(dict.fromkeys(keys))
        rejected: list[Key] = []
        fresh: dict[_Shard, list[Key]] = {}
        now_ms = _now_ms()
        for key in keys:
            if key in self._where:
                continue
            sh = self._shard_with_room()
            if sh is None:
                rejected.append(key)
                continue
            sh.keys[key] = candle_from_time(key[1], now_ms) if key[0] == "Candle" else None
            self._where[key] = sh
            fresh.setdefault(sh, []).append(key)
        for sh in fresh:  # start tasks before awaiting any subscribe
            # a shard that is being stopped is restarted by _stop itself
            if sh.stopping is None and not sh.live and (sh.task is None or sh.task.done()):
                sh.task = asyncio.create_task(self._run(sh), name=f"stream{sh.idx}")
        self._aggregate()
        for sh, added in fresh.items():
            async with sh.send_lock:
                conn = sh.conn
                still = [k for k in added if self._where.get(k) is sh]  # not removed meanwhile
                if sh.live and conn is not None and still:
                    try:
                        await conn.subscribe(add=sh.entries(still))
                    except (UpstreamError, aiohttp.ClientError, OSError):
                        pass  # the shard's reconnect resubscribes everything
        if rejected:
            log.warning("stream pool full: rejected %d keys", len(rejected))
        return rejected

    async def remove(self, keys: Iterable[Key]) -> None:
        gone: dict[_Shard, list[Key]] = {}
        for key in keys:
            sh = self._where.pop(key, None)
            if sh is None:
                continue
            sh.keys.pop(key, None)
            gone.setdefault(sh, []).append(key)
        for sh, removed in gone.items():
            if not sh.keys:
                await self._stop(sh)
                continue
            async with sh.send_lock:
                conn = sh.conn
                still = [k for k in removed if k not in sh.keys]  # not re-added meanwhile
                if sh.live and conn is not None and still:
                    try:
                        await conn.subscribe(remove=[protocol.sub_entry(k) for k in still])
                    except (UpstreamError, aiohttp.ClientError, OSError):
                        pass

    async def close(self) -> None:
        self._closed = True
        while True:  # a concurrent _stop may own a shard; loop until nothing is left
            for sh in self._shards:
                await self._stop(sh)
            if all(
                (sh.task is None or sh.task.done()) and sh.conn is None and sh.stopping is None
                for sh in self._shards
            ):
                return

    # ---- internals -------------------------------------------------------

    def _shard_with_room(self) -> _Shard | None:
        for sh in self._shards:
            if len(sh.keys) < self._per_socket:
                return sh
        if len(self._shards) < self._max_sockets:
            sh = _Shard(len(self._shards))
            self._shards.append(sh)
            return sh
        return None

    def _deliver(self, sh: _Shard, gen: int, events: list[Event]) -> None:
        if gen == sh.generation:  # generation guard: a zombie socket can't write
            self._on_events(events)

    def _set(self, sh: _Shard, state: State) -> None:
        sh.state = state
        self._aggregate()

    def _aggregate(self) -> None:
        # a shard that has keys but has not started yet counts as connecting
        active = [
            "connecting" if s.state == "idle" and s.keys else s.state
            for s in self._shards
            if s.state != "idle" or s.keys
        ]
        new: State
        if not active:
            new = "idle"
        elif "auth_failed" in active:
            new = "auth_failed"
        elif all(s == "live" for s in active):
            new = "live"
        elif "reconnecting" in active:
            new = "reconnecting"
        else:
            new = "connecting"
        if new != self._state:
            self._state = new
            self._on_state(new, self.generation)

    async def _stop(self, sh: _Shard) -> None:
        if sh.stopping is not None:  # someone else is already stopping it
            await sh.stopping.wait()
            return
        done = sh.stopping = asyncio.Event()
        try:
            task, sh.task = sh.task, None
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            sh.live = False
            if sh.conn is not None:  # idempotent; waits for the shared teardown
                await sh.conn.close()
                sh.conn = None
            self._set(sh, "idle")
        finally:
            sh.stopping = None
            done.set()
            # keys added while we were stopping: start again, unless closing
            if sh.keys and not self._closed and sh.task is None:
                sh.task = asyncio.create_task(self._run(sh), name=f"stream{sh.idx}")
                self._aggregate()

    async def _resubscribe(self, sh: _Shard, conn: UpstreamConnection) -> None:
        async with sh.send_lock:
            await self._resubscribe_locked(sh, conn)

    async def _resubscribe_locked(self, sh: _Shard, conn: UpstreamConnection) -> None:
        now_ms = _now_ms()
        for k in sh.keys:  # stream candles carry the current bar onward, not old history
            if k[0] == "Candle":
                sh.keys[k] = candle_from_time(k[1], now_ms)
        sent = set(sh.keys)
        await conn.subscribe(add=sh.entries(sent), reset=True)
        while True:  # keys added or removed while we were sending
            current = set(sh.keys)
            missing, stale = current - sent, sent - current
            if not missing and not stale:
                return
            if missing:
                await conn.subscribe(add=sh.entries(missing))
            if stale:
                await conn.subscribe(remove=[protocol.sub_entry(k) for k in stale])
            sent = current

    async def _run(self, sh: _Shard) -> None:
        backoff = self._backoff_factory()
        limit_backoff = self._limit_backoff_factory()
        first = True
        while sh.keys:
            sh.generation = self.generation = next(self._gen)
            self._set(sh, "connecting" if first else "reconnecting")
            conn = UpstreamConnection(
                name=f"stream{sh.idx}",
                generation=sh.generation,
                tokens=self._tokens,
                http=self._http,
                budget=self._budget,
                on_events=functools.partial(self._deliver, sh, sh.generation),
                handshake_timeout_s=self._handshake_s,
                silence_timeout_s=self._silence_s,
                keepalive_s=self._keepalive_s,
            )
            sh.conn = conn
            live_since: float | None = None
            delay = 0.0
            next_state: State = "reconnecting"
            try:
                await conn.open()
                await self._resubscribe(sh, conn)
                sh.live = True
                live_since = self._clock()
                self._set(sh, "live")
                cause = await conn.wait_closed()
                raise cause or UpstreamError(f"stream{sh.idx}: closed")
            except AuthFailed as e:
                delay, next_state = self._auth_retry_s, "auth_failed"
                log.error("stream%d: %s; retrying in %.0fs", sh.idx, e, delay)
            except SessionLimitError as e:
                delay = limit_backoff.next()
                log.error("stream%d: %s; retrying in %.0fs", sh.idx, e, delay)
            except (UpstreamError, TokenError, aiohttp.ClientError, OSError, TimeoutError) as e:
                if live_since is not None and self._clock() - live_since >= HEALTHY_RESET_S:
                    backoff.reset()
                    limit_backoff.reset()
                delay = backoff.next()
                log.warning("stream%d down: %s; retry in %.1fs", sh.idx, e, delay)
            except Exception as e:  # never let an unexpected error kill the loop
                delay = backoff.next()
                log.exception("stream%d: unexpected %r; retry in %.1fs", sh.idx, e, delay)
            finally:
                if sh.conn is conn:
                    sh.live = False
                await conn.close()
            first = False
            if not sh.keys:
                break
            self._set(sh, next_state)
            await self._sleep(delay)
        self._set(sh, "idle")
