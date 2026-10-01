"""Hot path: one call per upstream frame.

Stamps the receive time, updates the latest-value cache and appends to each
subscriber's queue. It never awaits, so a slow client can't delay the
upstream reader or any other client.

Candle symbols have aliases (`SPX{=1m}` is dxFeed's `SPX{=m}`): Candle events
are matched by canonical key and delivered to each subscriber under the symbol
it subscribed. Other types take the exact-match path.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from typing import Any, Protocol

from ..symbols import canonical_candle
from ..types import Event, Key
from .cache import LatestCache


class Sink(Protocol):
    def push(self, ev: Event) -> None: ...

    def push_control(self, msg: dict[str, Any]) -> None: ...


class Dispatcher:
    def __init__(self, cache: LatestCache, clock: Callable[[], float] = time.time) -> None:
        self._cache = cache
        self._clock = clock
        self._subs: dict[Key, set[Sink]] = {}
        # canonical Candle key -> subscribed symbol (alias) -> sinks
        self._candles: dict[Key, dict[str, set[Sink]]] = {}
        self._sinks: set[Sink] = set()
        self.events_in = 0

    def register(self, sink: Sink) -> None:
        self._sinks.add(sink)

    def unregister(self, sink: Sink) -> None:
        self._sinks.discard(sink)

    def attach(self, keys: Iterable[Key], sink: Sink) -> None:
        for key in keys:
            if key[0] == "Candle":
                aliases = self._candles.setdefault((key[0], canonical_candle(key[1])), {})
                aliases.setdefault(key[1], set()).add(sink)
            else:
                self._subs.setdefault(key, set()).add(sink)

    def detach(self, keys: Iterable[Key], sink: Sink) -> None:
        for key in keys:
            if key[0] == "Candle":
                ckey = (key[0], canonical_candle(key[1]))
                aliases = self._candles.get(ckey)
                sinks = aliases.get(key[1]) if aliases is not None else None
                if aliases is None or sinks is None:
                    continue
                sinks.discard(sink)
                if not sinks:
                    del aliases[key[1]]
                    if not aliases:
                        del self._candles[ckey]
                continue
            targets = self._subs.get(key)
            if targets is not None:
                targets.discard(sink)
                if not targets:
                    del self._subs[key]

    def on_events(self, events: list[Event]) -> None:
        rt = self._clock()
        subs = self._subs
        update = self._cache.update
        for ev in events:
            ev["rt"] = rt
            etype = ev["type"]
            if etype == "Candle":
                self._on_candle(ev)
                continue
            key = (etype, ev["symbol"])
            update(key, ev)
            targets = subs.get(key)
            if targets:
                for sink in targets:
                    sink.push(ev)
        self.events_in += len(events)

    def _on_candle(self, ev: Event) -> None:
        symbol = ev["symbol"]
        key = ("Candle", canonical_candle(symbol))
        self._cache.update(key, ev)
        aliases = self._candles.get(key)
        if aliases:
            for alias, sinks in aliases.items():
                out = ev if alias == symbol else {**ev, "symbol": alias}
                for sink in sinks:
                    sink.push(out)

    def broadcast(self, msg: dict[str, Any]) -> None:
        for sink in list(self._sinks):
            sink.push_control(msg)
