"""tastytrade OAuth refresh grant → access token → DXLink quote token."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import aiohttp

from .logsafe import register_secret

log = logging.getLogger(__name__)

USER_AGENT = "ttfeedhub/0.1"
ACCESS_SKEW_S = 60.0
QUOTE_TOKEN_TTL_S = 12 * 3600.0  # observed lifetime ~24h; refresh at half-life


class TokenError(Exception):
    """Transient failure: network, 5xx, malformed response."""


class AuthFailed(TokenError):
    """tastytrade rejected the grant itself; retrying soon won't help."""


class ScopeError(AuthFailed):
    """The grant carries more than read scope."""


@dataclass(frozen=True, slots=True)
class QuoteToken:
    token: str
    url: str
    fetched_at: float


class TokenManager:
    def __init__(
        self,
        http: aiohttp.ClientSession,
        *,
        api_base: str,
        client_secret: str,
        refresh_token: str,
        clock: Callable[[], float] = time.monotonic,
        allow_insecure: bool = False,
        timeout_s: float = 15.0,
    ) -> None:
        self._http = http
        self._base = api_base.rstrip("/")
        self._secret = client_secret
        self._refresh = refresh_token
        self._clock = clock
        self._insecure = allow_insecure
        self._timeout = aiohttp.ClientTimeout(total=timeout_s)
        self._headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        self._access: str | None = None
        self._access_exp = 0.0
        self._quote: QuoteToken | None = None
        self._quote_rejected = False  # DXLink refused the cached token (KI-002)
        self._lock = asyncio.Lock()
        self.scopes: frozenset[str] | None = None

    async def access_token(self) -> str:
        async with self._lock:
            return await self._access_locked()

    async def quote_token(self, *, force: bool = False) -> QuoteToken:
        async with self._lock:
            q = self._quote
            if q is not None and not force and self._clock() - q.fetched_at < QUOTE_TOKEN_TTL_S:
                return q
            bearer = await self._access_locked()
            body = await self._request(
                "GET", "/api-quote-tokens", headers={"Authorization": f"Bearer {bearer}"}
            )
            data = body.get("data") if isinstance(body, dict) else None
            token = data.get("token") if isinstance(data, dict) else None
            url = data.get("dxlink-url") if isinstance(data, dict) else None
            if not isinstance(token, str) or not token or not isinstance(url, str):
                raise TokenError("quote token response malformed")
            if not (url.startswith("wss://") or (self._insecure and url.startswith("ws://"))):
                raise TokenError("refusing non-wss dxlink-url")
            register_secret(token)
            self._quote = QuoteToken(token, url, self._clock())
            if self._quote_rejected:
                self._quote_rejected = False
                log.info("quote token refreshed after DXLink auth error")
            else:
                log.info("fetched DXLink quote token")
            return self._quote

    def invalidate_quote_token(self, *, rejected: QuoteToken | None = None) -> None:
        """Drop the cached quote token. With `rejected` (DXLink refused that token),
        only drop it if it is still the cached one: a late report from a socket on
        an old token must not throw away the fresh token another socket fetched."""
        if rejected is None:
            self._quote = None
        elif self._quote is rejected:
            self._quote = None
            self._quote_rejected = True

    async def _access_locked(self) -> str:
        if self._access is not None and self._clock() < self._access_exp - ACCESS_SKEW_S:
            return self._access
        body = await self._request(
            "POST",
            "/oauth/token",
            json={
                "grant_type": "refresh_token",
                "client_secret": self._secret,
                "refresh_token": self._refresh,
            },
        )
        token = body.get("access_token") if isinstance(body, dict) else None
        if not isinstance(token, str) or not token:
            raise TokenError("token response missing access_token")
        scope = body.get("scope")
        if scope is not None and not isinstance(scope, str):
            raise ScopeError("unrecognised scope format; refusing (read-only grant required)")
        if isinstance(scope, str):
            self.scopes = frozenset(scope.split())
            if "trade" in self.scopes:
                raise ScopeError("grant has 'trade' scope; TTfeedhub requires a read-only grant")
        try:
            expires_in = float(body.get("expires_in", 900))
        except (TypeError, ValueError):
            raise TokenError("token response has bad expires_in") from None
        register_secret(token)
        self._access = token
        self._access_exp = self._clock() + expires_in
        return token

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = {**self._headers, **kwargs.pop("headers", {})}
        try:
            async with self._http.request(
                method, self._base + path, headers=headers, timeout=self._timeout, **kwargs
            ) as r:
                if path == "/oauth/token" and r.status in (400, 401, 403):
                    raise AuthFailed(f"token refresh rejected: HTTP {r.status}")
                if r.status == 401:
                    self._access = None
                    raise TokenError(f"{path}: access token rejected")
                if r.status == 403:
                    raise AuthFailed(f"{path}: forbidden (market-data entitlement?)")
                if r.status != 200:
                    raise TokenError(f"{path}: HTTP {r.status}")
                return await r.json(content_type=None)
        except ValueError:
            raise TokenError(f"{path}: malformed JSON") from None
        except aiohttp.ClientError as e:
            raise TokenError(f"{path}: {type(e).__name__}") from None
        except TimeoutError:
            raise TokenError(f"{path}: timed out") from None
