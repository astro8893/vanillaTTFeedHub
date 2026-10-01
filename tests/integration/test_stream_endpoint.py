import asyncio
import time
from collections.abc import Callable
from typing import Any

import aiohttp
import pytest

from tests.fakes.fake_dxlink import FakeDxLink, quote_row
from tests.fakes.fake_dxlink import row as fake_row
from tests.helpers import auth, eventually
from ttfeedhub.hub import Hub
from ttfeedhub.server import stream


async def connect(app_client: Any, name: str = "ui") -> aiohttp.ClientWebSocketResponse:
    ws: aiohttp.ClientWebSocketResponse = await app_client.ws_connect(
        "/v1/stream", headers=auth(name)
    )
    return ws


async def hello(ws: aiohttp.ClientWebSocketResponse) -> dict[str, Any]:
    await ws.send_json({"op": "hello", "client": "test", "proto": 1})
    msg: dict[str, Any] = await ws.receive_json()
    assert msg["t"] == "welcome" and msg["proto"] == 1
    return msg


async def recv_until(
    ws: aiohttp.ClientWebSocketResponse,
    pred: Callable[[dict[str, Any]], bool],
    timeout: float = 2.0,
) -> dict[str, Any]:
    async with asyncio.timeout(timeout):
        while True:
            m: dict[str, Any] = await ws.receive_json()
            if pred(m):
                return m


async def test_rejects_missing_or_bad_token(app_client: Any) -> None:
    for headers in ({}, {"Authorization": "Bearer wrong"}, {"Authorization": "Basic x"}):
        with pytest.raises(aiohttp.WSServerHandshakeError) as ei:
            await app_client.ws_connect("/v1/stream", headers=headers)
        assert ei.value.status == 401


async def test_hello_is_required(app_client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(stream, "HELLO_TIMEOUT_S", 0.2)
    ws = await connect(app_client)
    await ws.receive()
    assert ws.close_code == 4001


async def test_bad_protocol_version(app_client: Any) -> None:
    ws = await connect(app_client)
    await ws.send_json({"op": "hello", "client": "t", "proto": 99})
    await ws.receive()
    assert ws.close_code == 4002


async def test_subscribe_receive_and_snapshot(app_client: Any, fake_dx: FakeDxLink) -> None:
    ws = await connect(app_client)
    await hello(ws)
    await ws.send_json(
        {"op": "sub", "id": 1, "mode": "all", "subs": [{"type": "Quote", "symbol": "SPX"}]}
    )
    ack = await recv_until(ws, lambda m: m["t"] == "ack")
    assert ack == {"t": "ack", "id": 1, "ok": True, "rejected": []}
    await eventually(lambda: ("Quote", "SPX") in fake_dx.all_subs())
    await fake_dx.emit("Quote", [quote_row("SPX", 5800.0, 5800.5)])
    ev = await recv_until(ws, lambda m: m["t"] == "ev")
    e = ev["d"][0]
    assert e["symbol"] == "SPX" and e["bidPrice"] == 5800.0 and isinstance(e["rt"], float)
    ws2 = await connect(app_client, "recorder")
    await hello(ws2)
    await ws2.send_json({"op": "sub", "id": 7, "subs": [{"type": "Quote", "symbol": "SPX"}]})
    snap = await recv_until(ws2, lambda m: m["t"] == "snap")
    assert snap["d"][0]["bidPrice"] == 5800.0
    assert fake_dx.open_count == 1  # both clients share one upstream socket


async def test_latest_mode_conflates_within_one_frame(app_client: Any, fake_dx: FakeDxLink) -> None:
    ws = await connect(app_client)
    await hello(ws)
    await ws.send_json(
        {"op": "sub", "id": 1, "mode": "latest", "subs": [{"type": "Quote", "symbol": "SPX"}]}
    )
    await recv_until(ws, lambda m: m["t"] == "ack")
    await eventually(lambda: ("Quote", "SPX") in fake_dx.all_subs())
    await fake_dx.emit("Quote", [quote_row("SPX", b, b + 1) for b in (1.0, 2.0, 3.0)])
    ev = await recv_until(ws, lambda m: m["t"] == "ev")
    assert [x["bidPrice"] for x in ev["d"]] == [3.0]


async def test_rejections(app_client: Any) -> None:
    ws = await connect(app_client, "recorder")  # Quote/Trade only, max_keys=3
    await hello(ws)
    subs = [{"type": "Greeks", "symbol": "SPX"}, {"type": "Quote", "symbol": "bad symbol!"}] + [
        {"type": "Quote", "symbol": s} for s in "ABCD"
    ]
    await ws.send_json({"op": "sub", "id": 2, "subs": subs})
    ack = await recv_until(ws, lambda m: m["t"] == "ack")
    reasons = {(r["type"], r["symbol"]): r["reason"] for r in ack["rejected"]}
    assert reasons == {
        ("Greeks", "SPX"): "type",
        ("Quote", "bad symbol!"): "symbol",
        ("Quote", "D"): "max_keys",
    }


async def test_unsubscribe_releases_upstream_after_linger(
    app_client: Any, fake_dx: FakeDxLink
) -> None:
    ws = await connect(app_client)
    await hello(ws)
    await ws.send_json({"op": "sub", "id": 1, "subs": [{"type": "Quote", "symbol": "SPX"}]})
    await recv_until(ws, lambda m: m["t"] == "ack")
    await eventually(lambda: ("Quote", "SPX") in fake_dx.all_subs())
    await ws.send_json({"op": "unsub", "id": 2, "subs": [{"type": "Quote", "symbol": "SPX"}]})
    await recv_until(ws, lambda m: m["t"] == "ack" and m["id"] == 2)
    await eventually(lambda: fake_dx.open_count == 0, timeout=3)


async def test_disconnect_releases_keys(app_client: Any, fake_dx: FakeDxLink, hub: Hub) -> None:
    ws = await connect(app_client)
    await hello(ws)
    await ws.send_json({"op": "sub", "id": 1, "subs": [{"type": "Quote", "symbol": "SPX"}]})
    await recv_until(ws, lambda m: m["t"] == "ack")
    await ws.close()
    await eventually(lambda: hub.conn_count["ui"] == 0 and hub.keys_by_name["ui"] == 0)
    await eventually(lambda: fake_dx.open_count == 0, timeout=3)


async def test_oversized_frame_closes_1009(app_client: Any) -> None:
    ws = await connect(app_client)
    await hello(ws)
    await ws.send_str("x" * (stream.MAX_FRAME_BYTES + 1))
    async with asyncio.timeout(2):
        while not ws.closed:
            await ws.receive()
    assert ws.close_code == 1009


async def test_connection_limit(app_client: Any) -> None:
    a = await connect(app_client)
    b = await connect(app_client)
    with pytest.raises(aiohttp.WSServerHandshakeError) as ei:
        await connect(app_client)
    assert ei.value.status == 429
    await a.close()
    await b.close()


async def test_tcp_nodelay_enabled(app_client: Any, hub: Hub) -> None:
    ws = await connect(app_client)
    await hello(ws)
    await eventually(lambda: len(hub.sessions) == 1)
    assert next(iter(hub.sessions)).nodelay is True


async def test_ping_pong_and_unknown_op(app_client: Any) -> None:
    ws = await connect(app_client)
    await hello(ws)
    await ws.send_json({"op": "ping", "id": 3})
    assert (await recv_until(ws, lambda m: m["t"] == "pong")) == {"t": "pong", "id": 3}
    await ws.send_json({"op": "nope", "id": 4})
    err = await recv_until(ws, lambda m: m["t"] == "error")
    assert err["id"] == 4


async def test_status_heartbeats_arrive(app_client: Any) -> None:
    ws = await connect(app_client)
    await hello(ws)
    st = await recv_until(ws, lambda m: m["t"] == "status")
    assert st["state"] == "idle"


async def test_heartbeat_evicts_a_peer_that_does_not_answer_pings(
    app_client: Any, hub: Hub, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stream, "HEARTBEAT_S", 0.2)
    ws = await app_client.ws_connect("/v1/stream", headers=auth(), autoping=False)
    await hello(ws)
    await eventually(lambda: len(hub.sessions) == 1)
    kinds: set[aiohttp.WSMsgType] = set()
    async with asyncio.timeout(3):
        while not ws.closed:
            kinds.add((await ws.receive()).type)
    assert aiohttp.WSMsgType.PING in kinds
    await eventually(lambda: len(hub.sessions) == 0)


async def test_heartbeat_keeps_an_answering_peer(
    app_client: Any, hub: Hub, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(stream, "HEARTBEAT_S", 0.1)
    ws = await connect(app_client)  # autoping=True, as in FeedClient
    await hello(ws)
    statuses = 0
    async with asyncio.timeout(3):
        while statuses < 10:  # ~1s of hub status heartbeats: many ping rounds
            m = await ws.receive_json()
            statuses += m["t"] == "status"
    assert not ws.closed and len(hub.sessions) == 1


async def test_status_has_since(app_client: Any, fake_dx: FakeDxLink) -> None:
    ws = await connect(app_client)
    await hello(ws)
    idle = await recv_until(ws, lambda m: m["t"] == "status")
    assert idle["state"] == "idle" and isinstance(idle["since"], float)
    assert idle["since"] <= time.time()
    again = await recv_until(ws, lambda m: m["t"] == "status")
    assert again["since"] == idle["since"]  # unchanged while the state is unchanged
    await ws.send_json({"op": "sub", "id": 1, "subs": [{"type": "Quote", "symbol": "SPX"}]})
    live = await recv_until(ws, lambda m: m["t"] == "status" and m["state"] == "live")
    assert live["since"] > idle["since"]


def candle_row_(sym: str, t: int, close: float) -> list[Any]:
    return fake_row("Candle", sym, time=t, index=t, open=close, high=close, low=close, close=close)


async def sub_candle(ws: aiohttp.ClientWebSocketResponse, mid: int, symbol: str) -> None:
    await ws.send_json({"op": "sub", "id": mid, "subs": [{"type": "Candle", "symbol": symbol}]})
    ack = await recv_until(ws, lambda m: m["t"] == "ack" and m["id"] == mid)
    assert ack["rejected"] == []


async def test_candle_events_arrive_under_the_requested_symbol(
    app_client: Any, fake_dx: FakeDxLink
) -> None:
    ws = await connect(app_client)
    await hello(ws)
    await sub_candle(ws, 1, "SPX{=1m,tho=true}")
    await eventually(lambda: ("Candle", "SPX{=m,tho=true}") in fake_dx.all_subs())
    await fake_dx.emit("Candle", [candle_row_("SPX{=1m,tho=true}", 60_000, 5.0)])
    ev = await recv_until(ws, lambda m: m["t"] == "ev")
    assert [(e["symbol"], e["close"]) for e in ev["d"]] == [("SPX{=1m,tho=true}", 5.0)]


async def test_candle_snapshot_uses_the_requested_symbol(
    app_client: Any, fake_dx: FakeDxLink
) -> None:
    ws = await connect(app_client)
    await hello(ws)
    await sub_candle(ws, 1, "SPX{=1m,tho=true}")
    await eventually(lambda: ("Candle", "SPX{=m,tho=true}") in fake_dx.all_subs())
    await fake_dx.emit("Candle", [candle_row_("SPX{=m,tho=true}", 60_000, 5.0)])
    await recv_until(ws, lambda m: m["t"] == "ev")
    r = await app_client.get(
        "/v1/snapshot", params={"key": "Candle:SPX{=1m,tho=true}"}, headers=auth()
    )
    body = await r.json()
    assert r.status == 200 and body["missing"] == []
    assert [(e["symbol"], e["close"]) for e in body["d"]] == [("SPX{=1m,tho=true}", 5.0)]
    ws2 = await connect(app_client)
    await hello(ws2)
    await ws2.send_json(
        {"op": "sub", "id": 1, "subs": [{"type": "Candle", "symbol": "SPX{=1m,tho=true}"}]}
    )
    snap = await recv_until(ws2, lambda m: m["t"] == "snap")
    assert [e["symbol"] for e in snap["d"]] == ["SPX{=1m,tho=true}"]


async def test_one_shot_candle_snapshot_finds_the_normalized_event(
    app_client: Any, fake_dx: FakeDxLink
) -> None:
    fake_dx.auto_rows[("Candle", "SPX{=1d}")] = candle_row_("SPX{=1d}", 0, 7.0)
    r = await app_client.get("/v1/snapshot", params={"key": "Candle:SPX{=1d}"}, headers=auth())
    body = await r.json()
    assert body["missing"] == []
    assert [(e["symbol"], e["close"]) for e in body["d"]] == [("SPX{=1d}", 7.0)]


async def test_candle_aliases_share_one_upstream_subscription(
    app_client: Any, fake_dx: FakeDxLink, hub: Hub
) -> None:
    a = await connect(app_client)
    await hello(a)
    b = await connect(app_client)
    await hello(b)
    await sub_candle(a, 1, "SPX{=1m,tho=true}")
    await sub_candle(b, 1, "SPX{=m,tho=true}")
    await sub_candle(b, 2, "SPX{=1m,tho=true}")  # one client, both aliases
    canon = ("Candle", "SPX{=m,tho=true}")
    await eventually(lambda: canon in fake_dx.all_subs())
    assert [k for k in hub.pool.keys() if k[0] == "Candle"] == [canon]
    assert hub.keys_by_name["ui"] == 3  # one key per client subscription
    await fake_dx.emit("Candle", [candle_row_("SPX{=m,tho=true}", 60_000, 5.0)])
    ev_a = await recv_until(a, lambda m: m["t"] == "ev")
    assert [e["symbol"] for e in ev_a["d"]] == ["SPX{=1m,tho=true}"]
    ev_b = await recv_until(b, lambda m: m["t"] == "ev")
    assert sorted(e["symbol"] for e in ev_b["d"]) == ["SPX{=1m,tho=true}", "SPX{=m,tho=true}"]
    # a drops its alias; b's two aliases keep the upstream subscription
    await a.send_json(
        {"op": "unsub", "id": 3, "subs": [{"type": "Candle", "symbol": "SPX{=1m,tho=true}"}]}
    )
    await recv_until(a, lambda m: m["t"] == "ack" and m["id"] == 3)
    await b.send_json(
        {"op": "unsub", "id": 4, "subs": [{"type": "Candle", "symbol": "SPX{=m,tho=true}"}]}
    )
    await recv_until(b, lambda m: m["t"] == "ack" and m["id"] == 4)
    assert hub.registry.refcount(canon) == 1
    await asyncio.sleep(0.3)  # past linger + reaper
    assert canon in fake_dx.all_subs()
    await fake_dx.emit("Candle", [candle_row_("SPX{=m,tho=true}", 120_000, 6.0)])
    ev_b = await recv_until(b, lambda m: m["t"] == "ev")
    assert [(e["symbol"], e["close"]) for e in ev_b["d"]] == [("SPX{=1m,tho=true}", 6.0)]
    assert hub.keys_by_name["ui"] == 1
    await b.send_json(
        {"op": "unsub", "id": 5, "subs": [{"type": "Candle", "symbol": "SPX{=1m,tho=true}"}]}
    )
    await recv_until(b, lambda m: m["t"] == "ack" and m["id"] == 5)
    await eventually(lambda: fake_dx.open_count == 0, timeout=3)
