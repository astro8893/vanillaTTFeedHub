"""Per-client bearer tokens (hashed at rest) and per-client rate limits."""

from __future__ import annotations

import hmac
import time
from collections.abc import Callable

from aiohttp import web

from ..config import ClientSpec, hash_token

_MAX_TOKEN_LEN = 256


class Authenticator:
    def __init__(self, clients: dict[str, ClientSpec]) -> None:
        self._clients = clients

    def authenticate(self, request: web.Request) -> ClientSpec | None:
        header = request.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return None
        token = header[7:].strip()
        if not token or len(token) > _MAX_TOKEN_LEN:
            return None
        digest = hash_token(token)
        found: ClientSpec | None = None
        for known, spec in self._clients.items():  # constant time across entries
            if hmac.compare_digest(known, digest):
                found = spec
        return found


class RateLimiter:
    """Token bucket per client name: `per_min` requests per minute, burst = per_min."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._buckets: dict[str, tuple[float, float]] = {}

    def allow(self, name: str, per_min: int) -> bool:
        now = self._clock()
        tokens, at = self._buckets.get(name, (float(per_min), now))
        tokens = min(float(per_min), tokens + (now - at) * per_min / 60.0)
        if tokens < 1.0:
            self._buckets[name] = (tokens, now)
            return False
        self._buckets[name] = (tokens - 1.0, now)
        return True
