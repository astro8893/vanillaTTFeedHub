"""KI-003: a subscribe-time snapshot is delivered in order with the key's live
events, exactly once, and never re-sent for a key the connection already holds."""

import asyncio
import contextlib
from typing import Any

import orjson
import pytest

from ttfeedhub.config import ClientSpec
from ttfeedhub.hub import Hub
from ttfeedhub.server.stream import ClientSession, write_loop

INDEXED = ["TimeAndSale", "Trade"]


class StubWS:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def send_frame(self, data: bytes, opcode: Any) -> None:
        self.sent.append(orjson.loads(data))

    async def close(self, *, code: int = 1000, message: bytes = b"") -> bool:
        return True


def tick(etype: str, i: int, symbol: str = "SPX") -> dict[str, Any]:
    return {"type": etype, "symbol": symbol, "index": i, "sequence": i, "price": 100.0 + i}


def session(hub: Hub, hub_clients: dict[str, ClientSpec]) -> tuple[ClientSession, StubWS]:
    spec = next(s for s in hub_clients.values() if s.name == "ui")
    ws = StubWS()
    return ClientSession(hub, spec, ws), ws  # type: ignore[arg-type]


async def sub(s: ClientSession, etype: str, mode: str = "all", op: str = "sub") -> None:
    msg = {"op": op, "id": 1, "mode": mode, "subs": [{"type": etype, "symbol": "SPX"}]}
    await s._handle(orjson.dumps(msg).decode())


async def drain(s: ClientSession, ws: StubWS) -> list[tuple[str, Any]]:
    """Run the real writer until the queue is empty; return (frame type, index)
    for every event frame, in wire order."""
    start = len(ws.sent)
    task = asyncio.create_task(write_loop(ws, s.q, lambda: None))  # type: ignore[arg-type]
    for _ in range(100):
        await asyncio.sleep(0.005)
        if s.q.depth == 0:
            break
    await asyncio.sleep(0.01)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    return [
        (f["t"], e.get("index", e.get("price")))
        for f in ws.sent[start:]
        if f["t"] in ("ev", "snap")
        for e in f["d"]
    ]


@pytest.mark.parametrize("etype", INDEXED)
async def test_resub_of_held_key_with_queued_events_delivers_each_once_in_order(
    app_client: Any, hub: Hub, hub_clients: dict[str, ClientSpec], etype: str
) -> None:
    s, ws = session(hub, hub_clients)
    await sub(s, etype)
    hub.dispatcher.on_events([tick(etype, 1), tick(etype, 2)])  # P and R queued, not sent
    await sub(s, etype)  # re-subscribe the held key
    got = await drain(s, ws)
    assert [i for _, i in got] == [1, 2]


@pytest.mark.parametrize("etype", INDEXED)
async def test_resub_of_held_key_after_delivery_sends_no_duplicate(
    app_client: Any, hub: Hub, hub_clients: dict[str, ClientSpec], etype: str
) -> None:
    s, ws = session(hub, hub_clients)
    await sub(s, etype)
    hub.dispatcher.on_events([tick(etype, 1)])
    assert [i for _, i in await drain(s, ws)] == [1]
    await sub(s, etype)
    assert await drain(s, ws) == []


@pytest.mark.parametrize("etype", INDEXED)
async def test_unsub_then_resub_with_queued_events_is_in_order_and_once(
    app_client: Any, hub: Hub, hub_clients: dict[str, ClientSpec], etype: str
) -> None:
    s, ws = session(hub, hub_clients)
    await sub(s, etype)
    hub.dispatcher.on_events([tick(etype, 1), tick(etype, 2)])
    await sub(s, etype, op="unsub")
    await sub(s, etype)
    hub.dispatcher.on_events([tick(etype, 3)])
    assert [i for _, i in await drain(s, ws)] == [1, 2, 3]


@pytest.mark.parametrize("etype", INDEXED)
async def test_unsub_then_resub_after_delivery_sends_no_duplicate(
    app_client: Any, hub: Hub, hub_clients: dict[str, ClientSpec], etype: str
) -> None:
    s, ws = session(hub, hub_clients)
    await sub(s, etype)
    hub.dispatcher.on_events([tick(etype, 1)])
    assert [i for _, i in await drain(s, ws)] == [1]
    await sub(s, etype, op="unsub")
    await sub(s, etype)
    assert await drain(s, ws) == []


@pytest.mark.parametrize("etype", INDEXED)
async def test_new_connection_gets_one_cached_value_then_live(
    app_client: Any, hub: Hub, hub_clients: dict[str, ClientSpec], etype: str
) -> None:
    first, _ = session(hub, hub_clients)
    await sub(first, etype)  # keeps the key live upstream
    hub.dispatcher.on_events([tick(etype, 1)])
    cached = hub.cache.get((etype, "SPX"))
    assert cached is not None
    s, ws = session(hub, hub_clients)  # e.g. a client replaying after reconnect
    await sub(s, etype)
    hub.dispatcher.on_events([tick(etype, 2)])
    got = await drain(s, ws)
    assert got == [("snap", 1), ("ev", 2)]
    snap = next(f for f in ws.sent if f["t"] == "snap")["d"][0]
    # A consumer that dedupes on (hub_rt, index, sequence) sees the replay as the original event.
    assert (snap["rt"], snap["index"], snap["sequence"]) == (
        cached["rt"],
        cached["index"],
        cached["sequence"],
    )


async def test_new_connection_snapshot_follows_queued_events_for_other_keys(
    app_client: Any, hub: Hub, hub_clients: dict[str, ClientSpec]
) -> None:
    first, _ = session(hub, hub_clients)
    await sub(first, "Trade")
    hub.dispatcher.on_events([tick("Trade", 1)])
    s, ws = session(hub, hub_clients)
    await sub(s, "Trade")
    hub.dispatcher.on_events([tick("Trade", 2), tick("Trade", 3)])
    await sub(s, "Trade")  # held: no second snapshot
    assert [i for _, i in await drain(s, ws)] == [1, 2, 3]


async def test_latest_mode_still_conflates_and_resub_adds_nothing(
    app_client: Any, hub: Hub, hub_clients: dict[str, ClientSpec]
) -> None:
    s, ws = session(hub, hub_clients)
    await sub(s, "Quote", mode="latest")
    hub.dispatcher.on_events([tick("Quote", i) for i in (1, 2, 3)])
    await sub(s, "Quote", mode="latest")
    assert [i for _, i in await drain(s, ws)] == [3]
    s2, ws2 = session(hub, hub_clients)
    await sub(s2, "Quote", mode="latest")
    hub.dispatcher.on_events([tick("Quote", i) for i in (4, 5)])
    assert [i for _, i in await drain(s2, ws2)] == [3, 5]
