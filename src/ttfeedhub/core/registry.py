"""Declarative master subscription list.

Tracks which (type, symbol) keys must be subscribed upstream, refcounted per
client connection. An unsubscribe linger keeps a key subscribed for a while
after its last holder leaves, so page flips and client restarts don't churn
DXLink.

Clients hold keys as they asked for them; upstream keys are canonical (see
`symbols`), so candle aliases such as `SPX{=1m}` and `SPX{=m}` share one
upstream key, which stays subscribed while any client holds any alias of it.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable

from ..symbols import canonical_key
from ..types import Key


class Registry:
    def __init__(self, linger_s: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._linger_s = linger_s
        self._clock = clock
        # canonical key -> client -> how many of its aliases that client holds
        self._by_key: dict[Key, dict[str, int]] = {}
        self._by_client: dict[str, set[Key]] = {}  # keys as the client gave them
        self._linger: dict[Key, float] = {}  # key -> expiry time

    def add(self, client_id: str, keys: Iterable[Key]) -> list[Key]:
        """Returns the canonical keys that were not subscribed upstream before."""
        new: list[Key] = []
        mine = self._by_client.setdefault(client_id, set())
        for key in keys:
            if key in mine:
                continue
            mine.add(key)
            ckey = canonical_key(key)
            holders = self._by_key.get(ckey)
            if holders is not None:
                holders[client_id] = holders.get(client_id, 0) + 1
                continue
            self._by_key[ckey] = {client_id: 1}
            if self._linger.pop(ckey, None) is None:
                new.append(ckey)
        if not mine:
            del self._by_client[client_id]
        return new

    def remove(self, client_id: str, keys: Iterable[Key]) -> None:
        self._release(client_id, keys, linger=True)

    def discard(self, client_id: str, keys: Iterable[Key]) -> None:
        """Release without linger: for keys that never made it upstream."""
        self._release(client_id, keys, linger=False)

    def remove_client(self, client_id: str) -> None:
        self.remove(client_id, list(self._by_client.get(client_id, ())))

    def expired(self) -> list[Key]:
        now = self._clock()
        out = [k for k, t in self._linger.items() if t <= now]
        for k in out:
            del self._linger[k]
        return out

    def active(self) -> set[Key]:
        return set(self._by_key) | set(self._linger)

    def client_keys(self, client_id: str) -> frozenset[Key]:
        return frozenset(self._by_client.get(client_id, ()))

    def refcount(self, key: Key) -> int:
        """Number of clients holding any alias of `key`."""
        return len(self._by_key.get(canonical_key(key), ()))

    def __len__(self) -> int:
        return len(self._by_key) + len(self._linger)

    def _release(self, client_id: str, keys: Iterable[Key], *, linger: bool) -> None:
        mine = self._by_client.get(client_id)
        if not mine:
            return
        expiry = self._clock() + self._linger_s
        for key in list(keys):
            if key not in mine:
                continue
            mine.discard(key)
            ckey = canonical_key(key)
            holders = self._by_key[ckey]
            left = holders[client_id] - 1
            if left:
                holders[client_id] = left
                continue
            del holders[client_id]
            if not holders:
                del self._by_key[ckey]
                if linger:
                    self._linger[ckey] = expiry
        if not mine:
            del self._by_client[client_id]
