"""Latest event per (type, symbol), plus one-shot waiters.

Events stored here are shared with client queues and must never be mutated
after dispatch.

Entries are stored under the canonical key (see `symbols`). Lookups accept any
alias of it and return the event labelled with the symbol that was asked for.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

from ..symbols import canonical_key
from ..types import Event, Key


def as_symbol(ev: Event, symbol: str) -> Event:
    """`ev` itself, or a copy labelled `symbol` (events are shared: never mutate)."""
    return ev if ev["symbol"] == symbol else {**ev, "symbol": symbol}


class LatestCache:
    def __init__(self) -> None:
        self._d: dict[Key, Event] = {}
        self._waiters: dict[Key, list[asyncio.Future[Event]]] = {}

    def update(self, key: Key, ev: Event) -> None:
        """`key` must be canonical."""
        self._d[key] = ev
        if self._waiters:
            waiting = self._waiters.pop(key, None)
            if waiting:
                for fut in waiting:
                    if not fut.done():
                        fut.set_result(ev)

    def get(self, key: Key) -> Event | None:
        ev = self._d.get(canonical_key(key))
        return None if ev is None else as_symbol(ev, key[1])

    def get_many(self, keys: Iterable[Key]) -> list[Event]:
        d = self._d
        return [as_symbol(ev, k[1]) for k in keys if (ev := d.get(canonical_key(k))) is not None]

    def drop(self, keys: Iterable[Key]) -> None:
        for k in keys:
            self._d.pop(canonical_key(k), None)

    async def wait_for(self, key: Key, wait_s: float) -> Event | None:
        ckey = canonical_key(key)
        existing = self._d.get(ckey)
        if existing is not None:
            return as_symbol(existing, key[1])
        fut: asyncio.Future[Event] = asyncio.get_running_loop().create_future()
        self._waiters.setdefault(ckey, []).append(fut)
        try:
            async with asyncio.timeout(wait_s):
                return as_symbol(await fut, key[1])
        except TimeoutError:
            return None
        finally:
            waiting = self._waiters.get(ckey)
            if waiting is not None and fut in waiting:
                waiting.remove(fut)
                if not waiting:
                    del self._waiters[ckey]

    def waiter_count(self) -> int:
        return sum(len(v) for v in self._waiters.values())

    def __len__(self) -> int:
        return len(self._d)
