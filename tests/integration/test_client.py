import asyncio
import time
from typing import Any

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from ttfeedhub_client import FeedClient, FeedError

from tests.fakes.fake_dxlink import FakeDxLink, quote_row, row
from tests.helpers import TOKENS, eventually
from ttfeedhub.hub import Hub

K = ("Quote", "SPX")
DAY = 86_400_000


def base(app_client: Any) -> str:
    return str(app_client.make_url("")).rstrip("/")


async def test_subscribe_stream_last_and_staleness(app_client: Any, fake_dx: FakeDxLink) -> None:
    async with FeedClient(base(app_client), TOKENS["ui"], name="t") as fc:
        await eventually(lambda: fc.state == "live")
        assert await fc.subscribe([K]) == []
        await eventually(lambda: K in fake_dx.all_subs())
        await fake_dx.emit("Quote", [quote_row("SPX", 1.0, 2.0)])
        batch = await asyncio.wait_for(anext(aiter(fc.events())), 2)
        assert batch[0]["askPrice"] == 2.0
        last = fc.last(K)
        assert last is not None and last["askPrice"] == 2.0
        s = fc.staleness(K)
        assert s is not None and 0 <= s < 1


async def test_subscriptions_made_before_connect_are_sent(
    app_client: Any, fake_dx: FakeDxLink
) -> None:
    fc = FeedClient(base(app_client), TOKENS["ui"], name="t")
    assert await fc.subscribe([K]) == []
    await fc.start()
    try:
        await eventually(lambda: K in fake_dx.all_subs())
    finally:
        await fc.close()


async def test_reconnect_replays_and_reports_gap(
    app_client: Any, hub: Hub, fake_dx: FakeDxLink
) -> None:
    gaps: list[str] = []
    async with FeedClient(base(app_client), TOKENS["ui"], name="t") as fc:
        fc.on_gap(gaps.append)
        await eventually(lambda: fc.state == "live")
        await fc.subscribe([K])
        await eventually(lambda: len(hub.sessions) == 1)
        for s in list(hub.sessions):
            await s.ws.close()
        await eventually(lambda: "reconnected" in gaps and len(hub.sessions) == 1, timeout=3)
        await eventually(lambda: K in fake_dx.all_subs())


async def test_rejected_keys_are_returned_and_not_replayed(app_client: Any) -> None:
    async with FeedClient(base(app_client), TOKENS["recorder"], name="c") as fc:
        await eventually(lambda: fc.state == "live")
        rejected = await fc.subscribe([("Greeks", "SPX")])
        assert rejected and rejected[0]["reason"] == "type"
        assert ("Greeks", "SPX") not in fc._subs


async def test_snapshot_and_candles_helpers(app_client: Any, fake_dx: FakeDxLink) -> None:
    fake_dx.auto_rows[("Quote", "SPY")] = quote_row("SPY", 5.0, 6.0)
    start = int(time.time() * 1000) - 5 * DAY
    fake_dx.candles["SPY{=1d}"] = [(start + i * DAY, 1.0, 2.0, 0.5, 1.5, 10.0) for i in range(3)]
    async with FeedClient(base(app_client), TOKENS["ui"], name="t") as fc:
        snap = await fc.snapshot([("Quote", "SPY")])
        assert snap[("Quote", "SPY")]["bidPrice"] == 5.0
        bars = await fc.candles("SPY", "1d", from_ms=start)
        assert len(bars) == 3


async def test_snapshot_helper_handles_2000_keys(app_client: Any) -> None:
    keys = [("Greeks", f".SPXW271217{'CP'[i % 2]}{4000 + i}") for i in range(2000)]
    async with FeedClient(base(app_client), TOKENS["ui"], name="t") as fc:
        assert await fc.snapshot(keys) == {}  # no data, but no request-line overflow


async def test_snapshot_detail_reports_missing_and_rejected(
    app_client: Any, fake_dx: FakeDxLink
) -> None:
    fake_dx.auto_rows[("Quote", "SPY")] = quote_row("SPY", 5.0, 6.0)
    keys = [
        ("Quote", "SPY"),  # has data
        ("Trade", "/ESZ26:XCME"),  # no data; symbol contains ':'
        ("Quote", "NODATA"),  # no data
        ("Greeks", "SPX"),  # type not allowed for recorder
        ("Quote", "OVER"),  # past recorder's max_keys=3 headroom: rejected and missing
    ]
    async with FeedClient(base(app_client), TOKENS["recorder"], name="c") as fc:
        events, missing, rejected = await fc.snapshot_detail(keys)
    assert set(events) == {("Quote", "SPY")}
    assert events[("Quote", "SPY")]["bidPrice"] == 5.0
    assert missing == [("Trade", "/ESZ26:XCME"), ("Quote", "NODATA"), ("Quote", "OVER")]
    assert rejected == [("Greeks", "SPX"), ("Quote", "OVER")]


async def test_snapshot_carries_summary_prev_day_volume(
    app_client: Any, fake_dx: FakeDxLink
) -> None:
    fake_dx.auto_rows[("Summary", "SPY")] = row("Summary", "SPY", prevDayVolume=12345)
    async with FeedClient(base(app_client), TOKENS["ui"], name="t") as fc:
        snap = await fc.snapshot([("Summary", "SPY")])
    assert snap[("Summary", "SPY")]["prevDayVolume"] == 12345


async def _raw_hub(messages: list[dict[str, Any]], gap_s: float = 0.05) -> TestServer:
    async def handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive()  # hello
        for m in messages:
            await ws.send_json(m)
            await asyncio.sleep(gap_s)
        async for _ in ws:  # stay silent until the client disconnects
            pass
        return ws

    app = web.Application()
    app.router.add_get("/v1/stream", handler)
    server = TestServer(app)
    await server.start_server()
    return server


async def test_stale_when_hub_goes_quiet() -> None:
    server = await _raw_hub([{"t": "welcome", "proto": 1, "gen": 0, "state": "live"}])
    states: list[str] = []
    try:
        async with FeedClient(
            str(server.make_url("")).rstrip("/"), "x", name="t", stale_after=0.2
        ) as fc:
            fc.on_state(states.append)
            await eventually(lambda: fc.state == "stale")
        assert "live" in states
    finally:
        await server.close()


async def test_hub_not_live_marks_stale_and_generation_change_is_a_gap() -> None:
    server = await _raw_hub(
        [
            {"t": "welcome", "proto": 1, "gen": 1, "state": "live"},
            {"t": "status", "state": "reconnecting", "gen": 1},
            {"t": "status", "state": "live", "gen": 2},
        ],
        gap_s=0.1,
    )
    gaps: list[str] = []
    seen: list[str] = []
    try:
        async with FeedClient(
            str(server.make_url("")).rstrip("/"), "x", name="t", stale_after=5
        ) as fc:
            fc.on_gap(gaps.append)
            fc.on_state(seen.append)
            await eventually(lambda: fc.generation == 2 and fc.state == "live")
        assert "stale" in seen and gaps == ["upstream_reconnect"]
    finally:
        await server.close()


class _ScriptedHub:
    """Raw websocket hub: sends `on_connect(n)` frames after hello (n = connection count),
    then answers each client op via `on_op`, staying silent otherwise."""

    def __init__(self, on_connect: Any, on_op: Any = None) -> None:
        self.connections = 0
        self._on_connect = on_connect
        self._on_op = on_op
        self.server: TestServer | None = None

    async def start(self) -> str:
        async def handler(request: web.Request) -> web.WebSocketResponse:
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            self.connections += 1
            await ws.receive()  # hello
            for m in self._on_connect(self.connections):
                await ws.send_json(m)
            async for msg in ws:
                if self._on_op is not None and msg.type == web.WSMsgType.TEXT:
                    for reply in self._on_op(msg.json()):
                        await ws.send_json(reply)
            return ws

        app = web.Application()
        app.router.add_get("/v1/stream", handler)
        self.server = TestServer(app)
        await self.server.start_server()
        return str(self.server.make_url("")).rstrip("/")

    async def close(self) -> None:
        if self.server is not None:
            await self.server.close()


WELCOME = {"t": "welcome", "proto": 1, "gen": 1, "state": "live"}


def _ev(i: int) -> dict[str, Any]:
    return {"t": "ev", "d": [{"type": "Quote", "symbol": "SPX", "rt": time.time(), "n": i}]}


async def test_half_open_connection_is_torn_down_and_reconnected() -> None:
    hub = _ScriptedHub(lambda n: [WELCOME])  # welcome, then total silence
    url = await hub.start()
    gaps: list[str] = []
    try:
        async with FeedClient(url, "x", name="t", stale_after=0.2, dead_after=0.6) as fc:
            fc.on_gap(gaps.append)
            await eventually(lambda: hub.connections >= 2 and "reconnected" in gaps, timeout=5)
    finally:
        await hub.close()


async def test_undrained_events_do_not_cause_a_reconnect_storm() -> None:
    hub = _ScriptedHub(lambda n: [WELCOME] + [_ev(i) for i in range(5 if n == 1 else 1)])
    url = await hub.start()
    gaps: list[str] = []
    states: list[str] = []
    try:
        async with FeedClient(url, "x", name="t", stale_after=5, max_pending_batches=2) as fc:
            fc.on_gap(gaps.append)
            fc.on_state(states.append)
            await eventually(lambda: "client_overflow" in gaps)
            await eventually(lambda: hub.connections == 2 and fc.state == "live", timeout=3)
            await asyncio.sleep(1.2)
            assert hub.connections == 2
            assert gaps.count("client_overflow") == 1
            assert fc.state == "live" and "stale" not in states
    finally:
        await hub.close()


async def test_unexpected_exception_does_not_kill_the_run_loop() -> None:
    def on_connect(n: int) -> list[dict[str, Any]]:
        bad: dict[str, Any] = {"t": "ev", "d": [{"no": "type"}]}  # KeyError in the reader
        return [WELCOME, bad] if n == 1 else [WELCOME]

    hub = _ScriptedHub(on_connect)
    url = await hub.start()
    try:
        async with FeedClient(url, "x", name="t", stale_after=5) as fc:
            await eventually(lambda: hub.connections >= 2 and fc.state == "live", timeout=4)
    finally:
        await hub.close()


async def test_send_failure_surfaces_as_feed_error() -> None:
    class Broken:
        async def send_str(self, _: str) -> None:
            raise ConnectionResetError("boom")

    fc = FeedClient("http://127.0.0.1:1", "x", name="t")
    fc._ws = Broken()  # type: ignore[assignment]
    with pytest.raises(FeedError, match="disconnected"):
        await fc._request({"op": "ping"})


async def test_close_fails_in_flight_requests_immediately() -> None:
    hub = _ScriptedHub(lambda n: [WELCOME])  # never acks
    url = await hub.start()
    try:
        fc = FeedClient(url, "x", name="t", stale_after=5)
        await fc.start()
        await eventually(lambda: fc.state == "live")
        task = asyncio.create_task(fc.subscribe([K]))
        await eventually(lambda: bool(fc._pending))
        await fc.close()
        await asyncio.wait_for(task, 2)  # would take 10s (request timeout) without the fix
    finally:
        await hub.close()


async def test_hub_error_reply_to_sub_raises_and_forgets_keys() -> None:
    def on_op(op: dict[str, Any]) -> list[dict[str, Any]]:
        return [{"t": "error", "id": op["id"], "reason": "boom"}] if op.get("op") == "sub" else []

    hub = _ScriptedHub(lambda n: [WELCOME], on_op)
    url = await hub.start()
    try:
        async with FeedClient(url, "x", name="t", stale_after=5) as fc:
            await eventually(lambda: fc.state == "live")
            with pytest.raises(FeedError, match="boom"):
                await fc.subscribe([K])
            assert K not in fc._subs
    finally:
        await hub.close()


async def test_start_twice_or_after_close_raises() -> None:
    fc = FeedClient("http://127.0.0.1:1", "x", name="t")
    await fc.start()
    with pytest.raises(RuntimeError):
        await fc.start()
    await fc.close()
    with pytest.raises(RuntimeError):
        await fc.start()


async def test_dead_watchdog_cannot_leave_live_standing() -> None:
    hub = _ScriptedHub(lambda n: [WELCOME])  # welcome, then silence
    url = await hub.start()
    try:
        async with FeedClient(url, "x", name="t", stale_after=0.2, dead_after=100) as fc:
            await eventually(lambda: fc.state == "live")
            watchdog = fc._tasks[1]
            watchdog.cancel()
            await asyncio.gather(watchdog, return_exceptions=True)
            await eventually(lambda: fc.state == "stale")
    finally:
        await hub.close()


async def test_watchdog_survives_an_iteration_error(monkeypatch: pytest.MonkeyPatch) -> None:
    hub = _ScriptedHub(lambda n: [WELCOME])
    url = await hub.start()
    try:
        async with FeedClient(url, "x", name="t", stale_after=0.2, dead_after=100) as fc:
            await eventually(lambda: fc.state == "live")
            real = fc._refresh_state
            calls = 0

            def flaky() -> None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise RuntimeError("boom")
                real()

            monkeypatch.setattr(fc, "_refresh_state", flaky)
            await eventually(lambda: calls >= 3)
            assert not fc._tasks[1].done()
    finally:
        await hub.close()


async def test_idle_hub_is_not_live_while_the_client_has_subscriptions() -> None:
    idle = {"t": "welcome", "proto": 1, "gen": 0, "state": "idle"}

    def on_op(op: dict[str, Any]) -> list[dict[str, Any]]:  # upstream comes up after the sub
        if op.get("op") != "sub":
            return []
        return [
            {"t": "ack", "id": op["id"], "ok": True, "rejected": []},
            {"t": "status", "state": "live", "gen": 1},
        ]

    hub = _ScriptedHub(lambda n: [idle], on_op)
    url = await hub.start()
    seen: list[tuple[str, str]] = []
    try:
        fc = FeedClient(url, "x", name="t", stale_after=5)
        fc.on_state(lambda s: seen.append((s, fc.hub_state)))
        await fc.subscribe([K])  # remembered, replayed on connect
        await fc.start()
        try:
            await eventually(lambda: fc.hub_state == "live" and fc.state == "live")
        finally:
            await fc.close()
        assert ("stale", "idle") in seen
        assert ("live", "idle") not in seen
    finally:
        await hub.close()


async def test_idle_hub_is_live_without_subscriptions() -> None:
    hub = _ScriptedHub(lambda n: [{"t": "welcome", "proto": 1, "gen": 0, "state": "idle"}])
    url = await hub.start()
    try:
        async with FeedClient(url, "x", name="t", stale_after=5) as fc:
            await eventually(lambda: fc.state == "live")
    finally:
        await hub.close()


async def test_server_close_leaves_live_before_the_close_handshake(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def handler(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        await ws.receive()  # hello
        await ws.send_json(WELCOME)
        await ws.close(code=1001, message=b"going away")
        return ws

    app = web.Application()
    app.router.add_get("/v1/stream", handler)
    server = TestServer(app)
    await server.start_server()
    at_exit: list[tuple[bool, str]] = []
    real_aexit = aiohttp.ClientWebSocketResponse.__aexit__
    fc = FeedClient(str(server.make_url("")).rstrip("/"), "x", name="t", stale_after=5)

    async def spy(self: aiohttp.ClientWebSocketResponse, *a: Any) -> None:
        at_exit.append((fc._ws is None, fc.state))
        await real_aexit(self, *a)

    monkeypatch.setattr(aiohttp.ClientWebSocketResponse, "__aexit__", spy)
    try:
        await fc.start()
        await eventually(lambda: bool(at_exit))
        assert at_exit[0] == (True, "reconnecting")
    finally:
        await fc.close()
        await server.close()


IDLE = {"t": "welcome", "proto": 1, "gen": 0, "state": "idle"}


async def _kill_watchdog(fc: FeedClient) -> None:
    """So nothing but the code under test can move the state."""
    watchdog = fc._tasks[1]
    watchdog.cancel()
    await asyncio.gather(watchdog, return_exceptions=True)


async def test_subscribe_on_an_idle_hub_leaves_live_immediately() -> None:
    hub = _ScriptedHub(lambda n: [IDLE], lambda op: [])  # never acks, never goes live
    url = await hub.start()
    try:
        async with FeedClient(url, "x", name="t", stale_after=5) as fc:
            await eventually(lambda: fc.state == "live")
            await _kill_watchdog(fc)
            seen: list[str] = []
            fc.on_state(seen.append)
            sub = asyncio.create_task(fc.subscribe([K]))
            await eventually(lambda: K in fc._subs)
            assert fc.hub_state == "idle"
            assert fc.state != "live"
            assert seen == ["stale"]  # callbacks see it too, without a watchdog tick
            sub.cancel()
            await asyncio.gather(sub, return_exceptions=True)
    finally:
        await hub.close()


async def test_unsubscribing_the_last_key_on_an_idle_hub_goes_live_immediately() -> None:
    def on_op(op: dict[str, Any]) -> list[dict[str, Any]]:  # acks subs, never unsubs
        if op.get("op") != "sub":
            return []
        return [{"t": "ack", "id": op["id"], "ok": True, "rejected": []}]

    hub = _ScriptedHub(lambda n: [IDLE], on_op)
    url = await hub.start()
    try:
        async with FeedClient(url, "x", name="t", stale_after=5) as fc:
            await eventually(lambda: fc.state == "live")
            await fc.subscribe([K])
            await eventually(lambda: fc.state == "stale")
            await _kill_watchdog(fc)
            seen: list[str] = []
            fc.on_state(seen.append)
            unsub = asyncio.create_task(fc.unsubscribe([K]))
            await eventually(lambda: K not in fc._subs)
            assert seen == ["live"]
            unsub.cancel()
            await asyncio.gather(unsub, return_exceptions=True)
    finally:
        await hub.close()


async def test_rejected_subscribe_on_an_idle_hub_goes_back_to_live() -> None:
    def on_op(op: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            {
                "t": "ack",
                "id": op["id"],
                "ok": True,
                "rejected": [{"type": K[0], "symbol": K[1], "reason": "x"}],
            }
        ]

    hub = _ScriptedHub(lambda n: [IDLE], on_op)
    url = await hub.start()
    try:
        async with FeedClient(url, "x", name="t", stale_after=5) as fc:
            await eventually(lambda: fc.state == "live")
            await _kill_watchdog(fc)
            assert len(await fc.subscribe([K])) == 1
            assert fc.state == "live"
    finally:
        await hub.close()
