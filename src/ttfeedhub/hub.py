"""Wires upstream, core and clients together, and owns the background tasks."""

from __future__ import annotations

import asyncio
import gc
import itertools
import logging
import time
from collections import Counter
from collections.abc import Callable
from typing import Any

import aiohttp

from .config import ClientSpec, Settings
from .core.cache import LatestCache
from .core.dispatch import Dispatcher
from .core.registry import Registry
from .symbols import canonical_key
from .tokens import AuthFailed, ScopeError, TokenError, TokenManager
from .types import Event, Key
from .upstream.connection import SessionBudget
from .upstream.history import HistoryPool
from .upstream.stream_pool import Backoff, State, StreamPool

log = logging.getLogger(__name__)


class Hub:
    def __init__(
        self,
        settings: Settings,
        clients: dict[str, ClientSpec],
        tokens: TokenManager,
        http: aiohttp.ClientSession,
        *,
        backoff_factory: Callable[[], Backoff] = Backoff,
    ) -> None:
        self.settings = settings
        self.clients = clients
        self.tokens = tokens
        self.budget = SessionBudget(settings.session_budget)
        self.cache = LatestCache()
        self.dispatcher = Dispatcher(self.cache)
        self.registry = Registry(settings.linger_s)
        self.pool = StreamPool(
            tokens=tokens,
            http=http,
            budget=self.budget,
            on_events=self.dispatcher.on_events,
            on_state=self._on_state,
            per_socket=settings.stream_symbols_per_socket,
            max_sockets=settings.max_stream_sockets,
            handshake_timeout_s=settings.handshake_timeout_s,
            silence_timeout_s=settings.silence_timeout_s,
            keepalive_s=settings.keepalive_s,
            auth_retry_s=settings.auth_retry_s,
            backoff_factory=backoff_factory,
        )
        self.history = HistoryPool(
            tokens=tokens,
            http=http,
            budget=self.budget,
            max_sockets=settings.max_history_sockets,
            timeout_s=settings.history_timeout_s,
            idle_close_s=settings.history_idle_close_s,
            cache_bytes=settings.candle_cache_bytes,
            handshake_timeout_s=settings.handshake_timeout_s,
            silence_timeout_s=settings.silence_timeout_s,
        )
        self.sessions: set[Any] = set()  # server.stream.ClientSession
        self.conn_count: Counter[str] = Counter()
        self.keys_by_name: Counter[str] = Counter()
        self.slow_evictions = 0
        self.started_at = time.time()
        self._auth_ok = True
        self._seq = itertools.count(1)
        self._tasks: list[asyncio.Task[None]] = []
        self._since_state = self.state
        self._since = time.time()  # epoch seconds when the current state began
        self._gc_prev: tuple[int, int, int] | None = None

    @property
    def state(self) -> str:
        st: State = self.pool.state
        if st == "idle" and not self._auth_ok:
            return "auth_failed"
        return st

    async def start(self) -> None:
        if self.settings.gc_threshold0 and self._gc_prev is None:
            self._gc_prev = gc.get_threshold()
            gc.set_threshold(self.settings.gc_threshold0, *self._gc_prev[1:])
        self._tasks = [
            asyncio.create_task(self._reaper(), name="hub-reaper"),
            asyncio.create_task(self._heartbeat(), name="hub-heartbeat"),
        ]
        try:
            await self.tokens.access_token()
        except ScopeError:
            await self.stop()
            raise  # refuse to run with a trade-scoped grant
        except AuthFailed as e:
            log.error("tastytrade rejected the grant: %s", e)
            self._auth_ok = False
            self._note_state()
            self._tasks.append(asyncio.create_task(self._auth_retry(), name="hub-auth-retry"))
        except TokenError as e:
            log.warning("startup token check failed (retried on demand): %s", e)

    async def stop(self) -> None:
        tasks, self._tasks = self._tasks, []
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.pool.close()
        await self.history.close()
        if self._gc_prev is not None:
            gc.set_threshold(*self._gc_prev)
            self._gc_prev = None

    def _note_state(self) -> str:
        st = self.state
        if st != self._since_state:
            self._since_state, self._since = st, time.time()
        return st

    def status_msg(self) -> dict[str, Any]:
        st = self._note_state()
        return {"t": "status", "state": st, "gen": self.pool.generation, "since": self._since}

    def new_client_id(self, name: str) -> str:
        return f"{name}#{next(self._seq)}"

    async def subscribe(self, client_id: str, keys: list[Key]) -> list[Key]:
        """Returns the keys (as given) rejected because the stream pool is full.
        Keys may be candle aliases; upstream only ever sees canonical keys."""
        new = self.registry.add(client_id, keys)
        full = set(await self.pool.add(new)) if new else set()
        if not full:
            return []
        rejected = [k for k in keys if canonical_key(k) in full]
        self.registry.discard(client_id, rejected)
        return rejected

    def unsubscribe(self, client_id: str, keys: list[Key]) -> None:
        self.registry.remove(client_id, keys)

    def drop_client(self, client_id: str) -> None:
        self.registry.remove_client(client_id)

    async def one_shot(self, keys: list[Key], wait_s: float) -> list[Event]:
        """Subscribe briefly, wait for a first value, then release into linger."""
        cid = self.new_client_id("_oneshot")
        try:
            rejected = set(await self.subscribe(cid, keys))
            got = await asyncio.gather(
                *(self.cache.wait_for(k, wait_s) for k in keys if k not in rejected)
            )
            return [e for e in got if e is not None]
        finally:
            self.registry.remove_client(cid)

    def stats(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "generation": self.pool.generation,
            "uptime_s": round(time.time() - self.started_at, 1),
            "sockets": {
                "in_use": self.budget.in_use,
                "peak": self.budget.peak,
                "limit": self.budget.limit,
            },
            "stream": self.pool.stats(),
            "history": self.history.stats(),
            "keys": {
                "upstream": len(self.pool.keys()),
                "registry": len(self.registry),
                "cached": len(self.cache),
            },
            "events_in": self.dispatcher.events_in,
            "clients": [s.stats() for s in self.sessions],
            "slow_evictions": self.slow_evictions,
        }

    # ---- background ------------------------------------------------------

    def _on_state(self, state: State, generation: int) -> None:
        self.dispatcher.broadcast(self.status_msg())

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self.settings.heartbeat_s)
            try:
                self.dispatcher.broadcast(self.status_msg())
            except Exception:
                log.exception("heartbeat failed")

    async def _reaper(self) -> None:
        while True:
            await asyncio.sleep(self.settings.reaper_interval_s)
            try:
                expired = self.registry.expired()
                if expired:
                    await self.pool.remove(expired)
                    # A key re-subscribed during the await keeps its cached value.
                    live = self.registry.active()
                    self.cache.drop([k for k in expired if k not in live])
            except Exception:
                log.exception("reaper iteration failed")

    async def _auth_retry(self) -> None:
        while not self._auth_ok:
            await asyncio.sleep(self.settings.auth_retry_s)
            try:
                await self.tokens.access_token()
            except ScopeError:
                log.critical("grant now has trade scope; refusing")
                return  # stay auth_failed
            except TokenError as e:
                log.error("tastytrade grant still rejected: %s", e)
                continue
            except Exception:
                log.exception("auth retry failed")
                continue
            self._auth_ok = True
            log.info("tastytrade grant accepted")
            self.dispatcher.broadcast(self.status_msg())
