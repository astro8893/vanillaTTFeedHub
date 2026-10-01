"""One DXLink socket.

- Takes a slot from the account-wide SessionBudget before connecting, and
  gives it back only after the socket is closed (sessions can't leak).
- The whole handshake runs under one deadline.
- A reader task decodes FEED_DATA and hands the events to `on_events`
  synchronously, so the hot path never awaits a consumer.
- If nothing arrives for `silence_timeout_s`, the socket is treated as dead.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Sequence
from typing import Any

import aiohttp
import orjson

from ..tokens import QuoteToken, TokenManager
from ..types import Event
from . import protocol

log = logging.getLogger(__name__)

EventCallback = Callable[[list[Event]], None]
_WS_CLOSE_TIMEOUT_S = 5.0
_MAX_UPSTREAM_MSG = 32 * 1024 * 1024
_REAUTH_WINDOW_S = 60.0
# ERROR codes that mean the quote token is no longer accepted (KI-002). Real DXLink
# also sends the session-limit ERROR with code UNAUTHORIZED, so that is checked first.
_AUTH_ERROR_CODES = frozenset({"UNAUTHORIZED", "TOKEN_EXPIRED"})


class UpstreamError(Exception):
    """The socket failed; reconnecting may help."""


class SessionLimitError(UpstreamError):
    """tastytrade refused the connection: too many sessions for the account."""


class HandshakeTimeout(UpstreamError):
    pass


class SilenceTimeout(UpstreamError):
    pass


class QuoteTokenRejected(UpstreamError):
    """DXLink refused the quote token (rejected or expired); a fresh one may help."""


class SessionBudget:
    """Account-wide cap on concurrent DXLink sockets (spec G1)."""

    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.in_use = 0
        self.peak = 0
        self._sem = asyncio.Semaphore(limit)

    async def acquire(self) -> None:
        await self._sem.acquire()
        self.in_use += 1
        self.peak = max(self.peak, self.in_use)

    def release(self) -> None:
        self.in_use -= 1
        self._sem.release()


def _classify_error(msg: dict[str, Any]) -> UpstreamError:
    text = str(msg.get("message") or msg.get("error") or "unknown error")[:200]
    low = text.lower()
    if "session" in low and ("limit" in low or "exceeded" in low):
        return SessionLimitError(text)
    code = str(msg.get("error") or "").upper()
    if (
        code in _AUTH_ERROR_CODES
        or "authentication" in low
        or ("token" in low and "expired" in low)
    ):
        return QuoteTokenRejected(f"DXLink error: {text}")
    return UpstreamError(f"DXLink error: {text}")


class UpstreamConnection:
    def __init__(
        self,
        *,
        name: str,
        generation: int,
        tokens: TokenManager,
        http: aiohttp.ClientSession,
        budget: SessionBudget,
        on_events: EventCallback,
        handshake_timeout_s: float = 30.0,
        silence_timeout_s: float = 60.0,
        keepalive_s: float = 30.0,
    ) -> None:
        self.name = name
        self.generation = generation
        self._tokens = tokens
        self._http = http
        self._budget = budget
        self._on_events = on_events
        self._handshake_s = handshake_timeout_s
        self._silence_s = silence_timeout_s
        self._keepalive_s = keepalive_s
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._holds_slot = False
        self._opened = False
        self._hs_task: asyncio.Task[None] | None = None
        self._closed = False
        self._closing: asyncio.Future[None] | None = None
        self._done: asyncio.Future[Exception | None] | None = None
        self._fields: dict[str, tuple[str, ...]] = dict(protocol.FIELDS)
        self._reauth_at = float("-inf")
        self._qt: QuoteToken | None = None  # the token this socket authenticated with
        self.live = False
        self.events_in = 0

    async def open(self) -> None:
        if self._closed or self._opened:
            raise UpstreamError(f"{self.name}: open() after close() or called twice")
        self._opened = True
        await self._budget.acquire()
        if self._closed:  # close() raced the wait for a slot
            self._budget.release()
            raise UpstreamError(f"{self.name}: closed while waiting for a session slot")
        self._holds_slot = True
        # The handshake runs as its own task so teardown can cancel and await it
        # (a socket opened mid-handshake is then always closed before the slot
        # is released). The task never calls close(), so there is no cycle.
        self._hs_task = asyncio.create_task(self._handshake(), name=f"{self.name}-handshake")
        try:
            async with asyncio.timeout(self._handshake_s):
                await self._hs_task
        except TimeoutError:
            await self.close()
            raise HandshakeTimeout(
                f"{self.name}: handshake exceeded {self._handshake_s:g}s"
            ) from None
        except asyncio.CancelledError:
            task = asyncio.current_task()
            await self.close()
            if self._hs_task.cancelled() and task is not None and task.cancelling() == 0:
                # teardown cancelled the handshake; this caller was not cancelled
                raise UpstreamError(f"{self.name}: closed during handshake") from None
            raise
        except BaseException:
            await self.close()
            raise
        if self._closed:  # close() raced the handshake; teardown closes the ws
            raise UpstreamError(f"{self.name}: closed during handshake")
        self._done = asyncio.get_running_loop().create_future()
        self._tasks = [
            asyncio.create_task(self._reader(), name=f"{self.name}-reader"),
            asyncio.create_task(self._keepalive(), name=f"{self.name}-keepalive"),
        ]
        self.live = True

    async def subscribe(
        self,
        add: Sequence[dict[str, Any]] = (),
        remove: Sequence[dict[str, Any]] = (),
        *,
        reset: bool = False,
    ) -> None:
        for msg in protocol.subscription_msgs(add, remove, reset=reset):
            await self._send(msg)

    async def wait_closed(self) -> Exception | None:
        if self._done is None:
            return None
        return await asyncio.shield(self._done)

    async def close(self) -> None:
        """Idempotent; safe to call concurrently and from a cancelled task."""
        self._closed = True
        self.live = False
        if self._closing is None:
            self._closing = asyncio.ensure_future(self._teardown())
        await asyncio.shield(self._closing)

    async def _teardown(self) -> None:
        try:
            tasks, self._tasks = self._tasks, []
            if self._hs_task is not None and not self._hs_task.done():
                tasks.append(self._hs_task)
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            ws, self._ws = self._ws, None
            if ws is not None:
                try:
                    await ws.close()  # bounded by ClientWSTimeout(ws_close=...)
                except Exception as e:  # noqa: BLE001 - closing must never raise
                    log.debug("%s: close error %r", self.name, e)
        finally:
            if self._holds_slot:
                self._holds_slot = False
                self._budget.release()
            if self._done is not None and not self._done.done():
                self._done.set_result(None)

    # ---- internals -------------------------------------------------------

    async def _handshake(self) -> None:
        qt = self._qt = await self._tokens.quote_token()
        self._ws = await self._http.ws_connect(
            qt.url,
            autoping=True,
            heartbeat=None,
            compress=0,
            max_msg_size=_MAX_UPSTREAM_MSG,
            timeout=aiohttp.ClientWSTimeout(ws_close=_WS_CLOSE_TIMEOUT_S),
        )
        await self._send(protocol.setup())
        auth_sent = False
        while True:
            msg = await self._recv_json()
            mtype = msg.get("type")
            if mtype == "AUTH_STATE":
                if msg.get("state") == "AUTHORIZED":
                    await self._send(protocol.channel_request())
                elif not auth_sent:
                    await self._send(protocol.auth(qt.token))
                    auth_sent = True
                else:
                    self._tokens.invalidate_quote_token(rejected=qt)
                    raise QuoteTokenRejected(f"{self.name}: quote token rejected")
            elif mtype == "CHANNEL_OPENED" and msg.get("channel") == protocol.FEED_CHANNEL:
                await self._send(protocol.feed_setup())
                return
            elif mtype == "ERROR":
                raise self._dxlink_error(msg)

    async def _reader(self) -> None:
        cause: Exception | None = None
        try:
            while True:
                try:
                    async with asyncio.timeout(self._silence_s):
                        msg = await self._recv_json()
                except TimeoutError:
                    raise SilenceTimeout(f"{self.name}: silent for {self._silence_s:g}s") from None
                mtype = msg.get("type")
                if mtype == "FEED_DATA":
                    self._handle_feed_data(msg)
                elif mtype == "FEED_CONFIG":
                    self._apply_feed_config(msg)
                elif mtype == "AUTH_STATE" and msg.get("state") == "UNAUTHORIZED":
                    await self._reauth()
                elif mtype == "ERROR":
                    raise self._dxlink_error(msg)
                elif mtype == "CHANNEL_CLOSED":
                    raise UpstreamError(f"{self.name}: feed channel closed by server")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - reported through wait_closed()
            cause = e
        finally:
            self.live = False
            if self._done is not None and not self._done.done():
                self._done.set_result(cause)

    def _dxlink_error(self, msg: dict[str, Any]) -> UpstreamError:
        """Classify an ERROR frame. An auth/expiry error drops the cached quote token
        (only if it is still the one this socket used), so the next connect fetches
        a fresh one; the caller's reconnect backoff paces those fetches."""
        err = _classify_error(msg)
        if isinstance(err, QuoteTokenRejected):
            self._tokens.invalidate_quote_token(rejected=self._qt)
        return err

    def _handle_feed_data(self, msg: dict[str, Any]) -> None:
        try:
            events = protocol.decode_feed_data(msg.get("data") or [], self._fields)
        except protocol.ProtocolError as e:
            log.warning("%s: dropped undecodable FEED_DATA: %s", self.name, e)
            return
        if not events:
            return
        self.events_in += len(events)
        try:
            self._on_events(events)
        except Exception:  # a consumer bug must not take the socket down
            log.exception("%s: event handler failed", self.name)

    def _apply_feed_config(self, msg: dict[str, Any]) -> None:
        fields = msg.get("eventFields")
        if not isinstance(fields, dict):
            return
        for etype, names in fields.items():
            if isinstance(names, list) and all(isinstance(n, str) for n in names):
                self._fields[etype] = tuple(names)

    async def _reauth(self) -> None:
        now = time.monotonic()
        if now - self._reauth_at < _REAUTH_WINDOW_S:
            self._tokens.invalidate_quote_token(rejected=self._qt)
            raise QuoteTokenRejected(f"{self.name}: re-auth rejected twice within 60s")
        self._reauth_at = now
        qt = self._qt = await self._tokens.quote_token(force=True)
        log.info("%s: server asked for re-auth; sent a fresh quote token", self.name)
        await self._send(protocol.auth(qt.token))

    async def _keepalive(self) -> None:
        while True:
            await asyncio.sleep(self._keepalive_s)
            try:
                await self._send(protocol.keepalive())
            except Exception:  # noqa: BLE001 - the reader notices the dead socket
                return

    async def _send(self, msg: dict[str, Any]) -> None:
        ws = self._ws
        if ws is None or ws.closed:
            raise UpstreamError(f"{self.name}: not connected")
        await ws.send_str(orjson.dumps(msg).decode())

    async def _recv_json(self) -> dict[str, Any]:
        ws = self._ws
        if ws is None:
            raise UpstreamError(f"{self.name}: not connected")
        m = await ws.receive()
        if m.type is aiohttp.WSMsgType.TEXT:
            try:
                msg = orjson.loads(m.data)
            except orjson.JSONDecodeError:
                raise UpstreamError(f"{self.name}: invalid JSON from server") from None
            if isinstance(msg, dict):
                return msg
            raise UpstreamError(f"{self.name}: non-object message")
        if m.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
            raise UpstreamError(f"{self.name}: socket closed (code {ws.close_code})")
        if m.type is aiohttp.WSMsgType.ERROR:
            raise UpstreamError(f"{self.name}: socket error {ws.exception()!r}")
        raise UpstreamError(f"{self.name}: unexpected frame {m.type!r}")
