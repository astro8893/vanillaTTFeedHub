"""Client websocket sessions: /v1/stream."""

from __future__ import annotations

import asyncio
import logging
import socket
from collections.abc import Callable
from typing import Any

import orjson
from aiohttp import WSMsgType, web

from ..config import EVENT_TYPES, ClientSpec
from ..core.client_queue import ClientQueue
from ..hub import Hub
from ..types import Key
from .keys import AUTH, HUB
from .validate import MAX_SUBS_PER_MSG, parse_subs, reject

log = logging.getLogger(__name__)

PROTO = 1
HELLO_TIMEOUT_S = 5.0
MAX_FRAME_BYTES = 256 * 1024
CLOSE_NO_HELLO = 4001
CLOSE_BAD_PROTO = 4002
CLOSE_SLOW_CONSUMER = 4008
_MAX_EVENTS_PER_FRAME = 500
SEND_TIMEOUT_S = 5.0
_CLOSE_TIMEOUT_S = 2.0
# Websocket ping interval. A peer that doesn't pong within half of it (a half-open
# connection) is dropped. FeedClient and aiohttp clients answer with autoping=True.
HEARTBEAT_S = 10.0


async def send_obj(ws: web.WebSocketResponse, obj: Any) -> None:
    await ws.send_frame(orjson.dumps(obj), WSMsgType.TEXT)


async def write_loop(
    ws: web.WebSocketResponse, q: ClientQueue, on_evict: Callable[[], None]
) -> None:
    """Send whatever is queued the moment it's queued. There are no timers,
    except a per-send deadline so a peer that stopped reading can't pin us."""
    while True:
        ctrl, batch = await q.next_frames(_MAX_EVENTS_PER_FRAME)
        if q.overflowed:
            await _evict(ws, on_evict)
            return
        try:
            async with asyncio.timeout(SEND_TIMEOUT_S):
                for msg in ctrl:
                    await send_obj(ws, msg)
                if batch:
                    await send_obj(ws, {"t": "ev", "d": batch})
        except TimeoutError:
            await _evict(ws, on_evict)
            return


async def _evict(ws: web.WebSocketResponse, on_evict: Callable[[], None]) -> None:
    on_evict()
    try:
        async with asyncio.timeout(_CLOSE_TIMEOUT_S):
            await ws.close(code=CLOSE_SLOW_CONSUMER, message=b"slow consumer")
    except TimeoutError:
        pass


def _loads(data: Any) -> Any:
    try:
        return orjson.loads(data)
    except orjson.JSONDecodeError:
        return None


def _enable_nodelay(request: web.Request) -> bool:
    transport = request.transport
    sock = transport.get_extra_info("socket") if transport is not None else None
    if sock is None:
        return False
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return bool(sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY))
    except OSError:
        return False


class ClientSession:
    def __init__(
        self,
        hub: Hub,
        spec: ClientSpec,
        ws: web.WebSocketResponse,
        transport: asyncio.Transport | None = None,
    ) -> None:
        self.hub = hub
        self.spec = spec
        self.ws = ws
        self.transport = transport
        self.id = hub.new_client_id(spec.name)
        self.q = ClientQueue(hub.settings.queue_limit_all, on_overflow=self._evicted)
        self.evicting = False
        self._writer: asyncio.Task[None] | None = None
        self._evict_task: asyncio.Task[None] | None = None
        self.keys: set[Key] = set()
        self.nodelay = False

    def stats(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "client": self.spec.name,
            "keys": len(self.keys),
            "queued": self.q.depth,
            "superseded": self.q.dropped_superseded,
            "nodelay": self.nodelay,
        }

    async def run(self) -> None:
        hub, name = self.hub, self.spec.name
        writer: asyncio.Task[None] | None = None
        try:
            if not await self._read_hello():
                return
            hub.dispatcher.register(self.q)
            self.q.push_control(
                {"t": "welcome", "proto": PROTO, "gen": hub.pool.generation, "state": hub.state}
            )
            writer = self._writer = asyncio.create_task(
                write_loop(self.ws, self.q, self._evicted), name=f"writer-{self.id}"
            )
            async for m in self.ws:
                if m.type is WSMsgType.TEXT:
                    await self._handle(m.data)
                elif m.type is WSMsgType.ERROR:
                    break
                else:
                    self._error(None, "text frames only")
        finally:
            # aiohttp may cancel this handler when the peer goes away, so do all
            # the synchronous bookkeeping first; only then await anything.
            if writer is not None:
                writer.cancel()
            if self._evict_task is not None:
                self._evict_task.cancel()
            hub.dispatcher.detach(self.keys, self.q)
            hub.dispatcher.unregister(self.q)
            hub.keys_by_name[name] -= len(self.keys)
            self.keys.clear()
            hub.drop_client(self.id)
            if writer is not None:
                await asyncio.gather(writer, return_exceptions=True)
            if not self.ws.closed:
                await self.ws.close()

    def _evicted(self) -> None:
        """Idempotent. Callable from the queue's push side: cancels the writer
        (which may be blocked in a send) and closes, aborting if the peer
        won't take the close frame."""
        if self.evicting:
            return
        self.evicting = True
        self.hub.slow_evictions += 1
        log.warning("evicted slow consumer %s", self.id)
        self._evict_task = asyncio.create_task(self._force_close(), name=f"evict-{self.id}")

    async def _force_close(self) -> None:
        cur = asyncio.current_task()
        if self._writer is not None and self._writer is not cur:
            self._writer.cancel()
        try:
            async with asyncio.timeout(_CLOSE_TIMEOUT_S):
                await self.ws.close(code=CLOSE_SLOW_CONSUMER, message=b"slow consumer")
        except TimeoutError:
            pass
        if self.transport is not None:
            self.transport.abort()

    async def _read_hello(self) -> bool:
        try:
            async with asyncio.timeout(HELLO_TIMEOUT_S):
                m = await self.ws.receive()
        except TimeoutError:
            await self.ws.close(code=CLOSE_NO_HELLO, message=b"hello timeout")
            return False
        msg = _loads(m.data) if m.type is WSMsgType.TEXT else None
        if not isinstance(msg, dict) or msg.get("op") != "hello":
            await self.ws.close(code=CLOSE_NO_HELLO, message=b"hello required")
            return False
        if msg.get("proto") != PROTO:
            await self.ws.close(code=CLOSE_BAD_PROTO, message=b"unsupported protocol")
            return False
        return True

    def _error(self, mid: int | None, reason: str) -> None:
        self.q.push_control({"t": "error", "id": mid, "reason": reason})

    async def _handle(self, data: str) -> None:
        msg = _loads(data)
        if not isinstance(msg, dict):
            self._error(None, "invalid message")
            return
        raw_id = msg.get("id")
        mid = raw_id if isinstance(raw_id, int) else None
        op = msg.get("op")
        if op == "sub":
            await self._sub(mid, msg)
        elif op == "unsub":
            self._unsub(mid, msg)
        elif op == "ping":
            self.q.push_control({"t": "pong", "id": mid})
        else:
            self._error(mid, "unknown op")

    async def _sub(self, mid: int | None, msg: dict[str, Any]) -> None:
        mode = msg.get("mode", "all")
        if mode not in ("all", "latest"):
            self._error(mid, "mode must be 'all' or 'latest'")
            return
        parsed = parse_subs(msg.get("subs"), self.spec.event_types)
        if parsed is None:
            self._error(mid, f"subs must be a list of at most {MAX_SUBS_PER_MSG}")
            return
        keys, rejected = parsed
        name = self.spec.name
        fresh = [k for k in keys if k not in self.keys]
        room = max(0, self.spec.max_keys - self.hub.keys_by_name[name])
        rejected.extend(reject(t, s, "max_keys") for t, s in fresh[room:])
        fresh = fresh[:room]
        # Reserve before awaiting so concurrent connections can't exceed max_keys.
        self.hub.keys_by_name[name] += len(fresh)
        reserved = len(fresh)
        try:
            full = set(await self.hub.subscribe(self.id, fresh)) if fresh else set()
            added = [k for k in fresh if k not in full]
            self.keys.update(added)
            self.hub.keys_by_name[name] -= reserved - len(added)
            reserved = 0
        finally:
            self.hub.keys_by_name[name] -= reserved
        rejected.extend(reject(t, s, "capacity") for t, s in fresh if (t, s) in full)
        held = [k for k in keys if k in self.keys]
        self.q.set_mode(held, mode)
        self.hub.dispatcher.attach(added, self.q)
        self.q.push_control({"t": "ack", "id": mid, "ok": True, "rejected": rejected})
        # Only keys new to this connection get their cached value, queued in the
        # same tick as attach so it precedes every later event (KI-003). Keys it
        # already held have had that value, or will get it, in order.
        if added:
            self.q.push_snapshot(added, self.hub.cache.get_many(added))

    def _unsub(self, mid: int | None, msg: dict[str, Any]) -> None:
        parsed = parse_subs(msg.get("subs"), EVENT_TYPES)
        if parsed is None:
            self._error(mid, f"subs must be a list of at most {MAX_SUBS_PER_MSG}")
            return
        keys, rejected = parsed
        held = [k for k in keys if k in self.keys]
        self.hub.dispatcher.detach(held, self.q)
        self.q.clear_keys(held, self.hub.cache.get_many(held))
        self.keys.difference_update(held)
        self.hub.keys_by_name[self.spec.name] -= len(held)
        self.hub.unsubscribe(self.id, held)
        self.q.push_control({"t": "ack", "id": mid, "ok": True, "rejected": rejected})


async def stream_handler(request: web.Request) -> web.StreamResponse:
    hub = request.app[HUB]
    spec = request.app[AUTH].authenticate(request)
    if spec is None:
        raise web.HTTPUnauthorized(text="unauthorized")
    if len(hub.sessions) >= hub.settings.max_clients:
        raise web.HTTPServiceUnavailable(text="too many clients")
    if hub.conn_count[spec.name] >= spec.max_connections:
        raise web.HTTPTooManyRequests(text="connection limit reached")
    ws = web.WebSocketResponse(
        compress=False, max_msg_size=MAX_FRAME_BYTES, autoping=True, heartbeat=HEARTBEAT_S
    )
    session = ClientSession(hub, spec, ws, request.transport)
    session.nodelay = _enable_nodelay(request)
    # Reserve the slots before the first await so concurrent handshakes can't overshoot.
    hub.sessions.add(session)
    hub.conn_count[spec.name] += 1
    try:
        await ws.prepare(request)
        await session.run()
    finally:
        hub.sessions.discard(session)
        hub.conn_count[spec.name] -= 1
    return ws
