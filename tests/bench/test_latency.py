"""Latency benchmark (spec §7). Run with `bash scripts/bench.sh` (Linux + uvloop).

The hub and the fake DXLink run in this process. The 4 clients run in
separate processes and measure (receive time - hub receive time `rt`).

The spec (G2) targets the latency the hub ADDS. On a shared VM (Docker Desktop
with other containers running) scheduler/wake-up jitter alone gives a p99 of
several ms, far above the absolute targets. So each rate is run as 3
baseline/hub pairs (alternating order) in the same session: the baseline is a
minimal no-hub relay (same protocol, same 4 FeedClient processes, same rates,
warmup and duration), the other run is the real hub. The assertions are on the
MEDIAN of the per-pair hub-minus-baseline deltas. Absolute p50/p99/max for every
run are still printed, so on an idle host they can be compared with the spec's
absolute targets (p99 < 1 ms idle, < 2 ms at 5,000 events/s).

Every event carries a sequence number (in bidPrice). In mode="all" every event
must be delivered to every client, so each run also asserts delivered == emitted
(x clients) for the measured window; late or lost events would otherwise bias the
p99 downward.
"""

from __future__ import annotations

import asyncio
import multiprocessing as mp
import statistics
import time
from typing import Any

import aiohttp
import orjson
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from tests.fakes.fake_api import FakeApi
from tests.fakes.fake_dxlink import FakeDxLink, quote_row
from ttfeedhub.config import ClientSpec, Settings, hash_token
from ttfeedhub.hub import Hub
from ttfeedhub.server.app import build_app
from ttfeedhub.tokens import TokenManager

pytestmark = pytest.mark.bench

TOKEN = "bench-token-0123456789abcdef"
N_KEYS = 1000
N_CLIENTS = 4
WARMUP_S = 1.0
GRACE_S = 3.0  # how long clients keep reading after the last emit
PAIRS = 3
DELIVERY_TOLERANCE = 0.005


def _client_main(
    url: str, keys: list[tuple[str, str]], out: Any, last_seq: Any, stop_at: Any
) -> None:
    """Record (seq, latency) for every event until the last emitted seq arrives (or grace)."""

    async def run() -> None:
        from ttfeedhub_client import FeedClient

        seqs: list[int] = []
        lats: list[float] = []
        gaps: list[str] = []
        async with FeedClient(url, TOKEN, name="bench", max_pending_batches=100_000) as fc:
            fc.on_gap(gaps.append)
            await fc.subscribe(keys, mode="all")

            async def consume() -> None:
                async for batch in fc.events():
                    now = time.time()
                    for ev in batch:
                        seqs.append(int(ev["bidPrice"]))
                        lats.append(now - ev["rt"])

            task = asyncio.create_task(consume())
            while True:
                await asyncio.sleep(0.05)
                stop = stop_at.value
                if stop > 0 and (
                    (seqs and seqs[-1] >= last_seq.value >= 0) or time.time() > stop + GRACE_S
                ):
                    break
            task.cancel()
        out.put((seqs, lats, gaps))

    try:
        import uvloop

        uvloop.run(run())
    except ImportError:
        asyncio.run(run())


async def _eventually(pred: Any, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > deadline:
            raise AssertionError("benchmark setup timed out")
        await asyncio.sleep(0.05)


class _BaselineRelay:
    """Minimal no-hub server speaking just enough of the hub protocol for FeedClient."""

    def __init__(self) -> None:
        self.clients: list[web.WebSocketResponse] = []
        self.subscribed = 0
        self._server: TestServer | None = None

    async def _handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(compress=False)
        await ws.prepare(request)
        self.clients.append(ws)
        pinger = asyncio.create_task(self._status_loop(ws))
        try:
            async for m in ws:
                if m.type is not aiohttp.WSMsgType.TEXT:
                    continue
                msg = orjson.loads(m.data)
                if msg.get("op") == "hello":
                    await self._send(ws, {"t": "welcome", "proto": 1, "gen": 1, "state": "live"})
                elif msg.get("op") == "sub":
                    await self._send(
                        ws, {"t": "ack", "id": msg.get("id"), "ok": True, "rejected": []}
                    )
                    self.subscribed += 1
        finally:
            pinger.cancel()
            await asyncio.gather(pinger, return_exceptions=True)
            if ws in self.clients:
                self.clients.remove(ws)
        return ws

    @staticmethod
    async def _send(ws: web.WebSocketResponse, obj: dict[str, Any]) -> None:
        await ws.send_frame(orjson.dumps(obj), aiohttp.WSMsgType.TEXT)

    async def _status_loop(self, ws: web.WebSocketResponse) -> None:
        while True:
            await asyncio.sleep(2.0)
            await self._send(ws, {"t": "status", "state": "live", "gen": 1})

    async def start(self) -> str:
        app = web.Application()
        app.router.add_get("/v1/stream", self._handler)
        self._server = TestServer(app)
        await self._server.start_server()
        return str(self._server.make_url("")).rstrip("/")

    async def emit(self, rows: list[dict[str, Any]]) -> None:
        rt = time.time()
        for r in rows:
            r["rt"] = rt
        frame = orjson.dumps({"t": "ev", "d": rows})  # encode once, fan out concurrently
        await asyncio.gather(
            *(ws.send_frame(frame, aiohttp.WSMsgType.TEXT) for ws in list(self.clients))
        )

    async def close(self) -> None:
        if self._server is not None:
            await self._server.close()


class _RunResult:
    def __init__(self) -> None:
        self.lat: list[float] = []
        self.cpu = 0.0
        self.emitted = 0  # events emitted in the measured window
        self.delivered = 0  # summed over clients, events in the measured window
        self.gaps = 0
        self.extra = ""


async def _drive(
    url: str, ready: Any, emit: Any, rate_per_s: float, duration_s: float
) -> _RunResult:
    """Spawn the client processes, feed events at `rate_per_s`, collect and split results."""
    keys = [("Quote", f"SYM{i}") for i in range(N_KEYS)]
    ctx = mp.get_context("spawn")
    out = ctx.Queue()
    last_seq = ctx.Value("q", -1)
    stop_at = ctx.Value("d", 0.0)
    procs = [
        ctx.Process(target=_client_main, args=(url, keys, out, last_seq, stop_at))
        for _ in range(N_CLIENTS)
    ]
    for p in procs:
        p.start()
    try:
        await _eventually(ready, 60)
        per_tick = max(1, int(rate_per_s / 100))
        tick = per_tick / rate_per_s
        t_warm = time.time() + WARMUP_S
        t_end = t_warm + duration_s
        start_seq: int | None = None
        cpu0, wall0, i = time.process_time(), time.monotonic(), 0
        while time.time() < t_end:
            if start_seq is None and time.time() >= t_warm:
                start_seq = i
            await emit([(i + j, f"SYM{(i + j) % N_KEYS}") for j in range(per_tick)])
            i += per_tick
            await asyncio.sleep(tick)
        cpu = (time.process_time() - cpu0) / (time.monotonic() - wall0)
        assert start_seq is not None
        last_seq.value = i - 1
        stop_at.value = time.time()
        results = [await asyncio.to_thread(out.get, True, 60) for _ in procs]
        for p in procs:
            await asyncio.to_thread(p.join, 30)
    finally:
        for p in procs:
            if p.is_alive():
                p.terminate()
                p.join(5)
    res = _RunResult()
    res.cpu = cpu
    res.emitted = i - start_seq
    for seqs, lats, gaps in results:
        res.gaps += len(gaps)
        for s, x in zip(seqs, lats, strict=True):
            if s >= start_seq:
                res.delivered += 1
                res.lat.append(x)
    return res


async def _run_baseline(rate_per_s: float, duration_s: float) -> _RunResult:
    relay = _BaselineRelay()
    url = await relay.start()

    async def emit(items: list[tuple[int, str]]) -> None:
        await relay.emit(
            [
                {
                    "type": "Quote",
                    "symbol": s,
                    "bidPrice": float(seq),
                    "askPrice": 1.1,
                    "bidSize": 1,
                    "askSize": 1,
                }
                for seq, s in items
            ]
        )

    try:
        return await _drive(
            url, lambda: relay.subscribed >= N_CLIENTS, emit, rate_per_s, duration_s
        )
    finally:
        await relay.close()


async def _run_hub(rate_per_s: float, duration_s: float) -> _RunResult:
    fake_dx = FakeDxLink()
    await fake_dx.start()
    api = FakeApi()
    api.quote_url = fake_dx.url
    await api.start()
    try:
        async with aiohttp.ClientSession() as http:
            tokens = TokenManager(
                http,
                api_base=api.url,
                client_secret="bench-secret-xyz",
                refresh_token="bench-refresh-xyz",
                allow_insecure=True,
            )
            spec = ClientSpec(
                name="bench", token_sha256=hash_token(TOKEN), max_keys=5000, max_connections=8
            )
            hub = Hub(Settings(), {spec.token_sha256: spec}, tokens, http)
            server = TestServer(build_app(hub))
            await server.start_server()
            url = str(server.make_url("")).rstrip("/")

            async def emit(items: list[tuple[int, str]]) -> None:
                await fake_dx.emit("Quote", [quote_row(s, float(seq), 1.1) for seq, s in items])

            def ready() -> bool:
                return len(fake_dx.all_subs()) >= N_KEYS and len(hub.sessions) == N_CLIENTS

            try:
                res = await _drive(url, ready, emit, rate_per_s, duration_s)
                res.extra = f"slow_evictions={hub.slow_evictions}"
                return res
            finally:
                await server.close()
    finally:
        await api.close()
        await fake_dx.close()


def _stats(lat: list[float]) -> tuple[float, float, float]:
    q = statistics.quantiles(lat, n=100)
    return q[49] * 1e3, q[98] * 1e3, max(lat) * 1e3


def _check_delivery(name: str, res: _RunResult) -> None:
    want = res.emitted * N_CLIENTS
    assert res.gaps == 0, f"{name}: client saw {res.gaps} gaps/reconnects"
    assert abs(res.delivered - want) <= want * DELIVERY_TOLERANCE, (
        f"{name}: delivered {res.delivered} != emitted {want} (x{N_CLIENTS} clients)"
    )


async def _compare(rate_per_s: float, duration_s: float) -> tuple[float, float]:
    """Run PAIRS baseline/hub pairs (alternating order); return median (d_p50, d_p99) in ms."""
    tag = f"rate={rate_per_s:g}/s"
    d50s: list[float] = []
    d99s: list[float] = []
    floor = rate_per_s * N_CLIENTS * duration_s * 0.5
    for n in range(PAIRS):
        if n % 2 == 0:
            base = await _run_baseline(rate_per_s, duration_s)
            hub = await _run_hub(rate_per_s, duration_s)
        else:
            hub = await _run_hub(rate_per_s, duration_s)
            base = await _run_baseline(rate_per_s, duration_s)
        b50, b99, bmax = _stats(base.lat)
        h50, h99, hmax = _stats(hub.lat)
        d50s.append(h50 - b50)
        d99s.append(h99 - b99)
        print(
            f"\n{tag} pair{n} BASELINE emitted={base.emitted} delivered={base.delivered}/"
            f"{base.emitted * N_CLIENTS} p50={b50:.3f}ms p99={b99:.3f}ms max={bmax:.3f}ms "
            f"cpu(relay)={base.cpu:.0%}\n"
            f"{tag} pair{n} HUB      emitted={hub.emitted} delivered={hub.delivered}/"
            f"{hub.emitted * N_CLIENTS} p50={h50:.3f}ms p99={h99:.3f}ms max={hmax:.3f}ms "
            f"cpu(hub+fake)={hub.cpu:.0%} {hub.extra}\n"
            f"{tag} pair{n} DELTA    p50={h50 - b50:+.3f}ms p99={h99 - b99:+.3f}ms "
            f"max={hmax - bmax:+.3f}ms"
        )
        assert base.emitted * N_CLIENTS > floor, "baseline emitted far below the requested rate"
        assert hub.emitted * N_CLIENTS > floor, "hub run emitted far below the requested rate"
        _check_delivery("baseline", base)
        _check_delivery("hub", hub)
    m50, m99 = statistics.median(d50s), statistics.median(d99s)
    print(f"{tag} MEDIAN DELTA p50={m50:+.3f}ms p99={m99:+.3f}ms")
    return m50, m99


async def test_idle_latency_added_by_hub() -> None:
    d50, d99 = await _compare(rate_per_s=20, duration_s=8.0)
    assert d99 < 1.0
    assert d50 < 0.5


async def test_busy_latency_added_by_hub() -> None:
    d50, d99 = await _compare(rate_per_s=5000, duration_s=5.0)
    assert d99 < 2.0
    assert d50 < 0.5
