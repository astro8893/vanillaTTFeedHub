import asyncio
from typing import Any

import pytest

from tests.fakes.fake_dxlink import FakeDxLink, quote_row
from tests.helpers import eventually
from ttfeedhub.types import Event
from ttfeedhub.upstream import stream_pool
from ttfeedhub.upstream.connection import SessionBudget
from ttfeedhub.upstream.stream_pool import Backoff, StreamPool, _Shard

K1, K2, K3 = ("Quote", "SPX"), ("Quote", "SPY"), ("Quote", "QQQ")


def fast() -> Backoff:
    return Backoff(base=0.02, cap=0.05, jitter=0)


def make_pool(
    tokens,
    http,
    *,
    budget: SessionBudget | None = None,
    events: list[Event] | None = None,
    states: list[str] | None = None,
    **kw: Any,
) -> StreamPool:
    opts: dict[str, Any] = dict(
        per_socket=5000,
        max_sockets=3,
        handshake_timeout_s=2.0,
        silence_timeout_s=5.0,
        backoff_factory=fast,
        limit_backoff_factory=fast,
    )
    opts.update(kw)
    return StreamPool(
        tokens=tokens,
        http=http,
        budget=budget or SessionBudget(5),
        on_events=(events.extend if events is not None else (lambda e: None)),
        on_state=(lambda s, g: states.append(s)) if states is not None else (lambda s, g: None),
        **opts,
    )


async def test_idle_until_first_key_then_live(tokens, http, fake_dx: FakeDxLink) -> None:
    states: list[str] = []
    events: list[Event] = []
    pool = make_pool(tokens, http, states=states, events=events)
    assert pool.state == "idle" and fake_dx.open_count == 0
    assert await pool.add([K1]) == []
    await eventually(lambda: pool.state == "live")
    assert states[:2] == ["connecting", "live"]
    await eventually(lambda: fake_dx.all_subs() == {K1})  # server has processed the sub
    await fake_dx.emit("Quote", [quote_row("SPX", 1, 2)])
    await eventually(lambda: len(events) == 1)
    await pool.close()


async def test_reconnect_resubscribes_everything_with_new_generation(
    tokens, http, fake_dx: FakeDxLink
) -> None:
    pool = make_pool(tokens, http)
    await pool.add([K1, K2])
    await eventually(lambda: pool.state == "live")
    gen = pool.generation
    await fake_dx.drop_all()
    await eventually(
        lambda: pool.state == "live" and pool.generation > gen and fake_dx.all_subs() >= {K1, K2}
    )
    first_sub = next(m for m in fake_dx.conns[0].received if m["type"] == "FEED_SUBSCRIPTION")
    assert first_sub.get("reset") is True
    await pool.close()


async def test_keys_added_while_connecting_are_subscribed(
    tokens, http, fake_dx: FakeDxLink
) -> None:
    fake_dx.stall_handshake = True
    pool = make_pool(tokens, http, handshake_timeout_s=0.3)
    await pool.add([K1])
    await asyncio.sleep(0.1)
    await pool.add([K2])
    fake_dx.stall_handshake = False
    await eventually(lambda: pool.state == "live" and fake_dx.all_subs() >= {K1, K2}, timeout=3)
    await pool.close()


async def test_sticky_first_fit_sharding_and_capacity(tokens, http, fake_dx: FakeDxLink) -> None:
    pool = make_pool(tokens, http, per_socket=2, max_sockets=2)
    rejected = await pool.add([("Quote", f"S{i}") for i in range(5)])
    assert rejected == [("Quote", "S4")]
    await eventually(
        lambda: (
            pool.state == "live"
            and fake_dx.open_count == 2
            and sorted(len(c.subs) for c in fake_dx.conns) == [2, 2]
        )
    )
    await pool.close()


async def test_remove_unsubscribes_and_last_key_closes_socket(
    tokens, http, fake_dx: FakeDxLink
) -> None:
    budget = SessionBudget(5)
    pool = make_pool(tokens, http, budget=budget)
    await pool.add([K1, K2])
    await eventually(lambda: pool.state == "live")
    await pool.remove([K1])
    await eventually(lambda: fake_dx.all_subs() == {K2})
    await pool.remove([K2])
    await eventually(lambda: pool.state == "idle" and fake_dx.open_count == 0)
    assert budget.in_use == 0
    await pool.close()


async def test_generation_guard_drops_stale_events(tokens, http) -> None:
    events: list[Event] = []
    pool = make_pool(tokens, http, events=events)
    sh = _Shard(0)
    sh.generation = 5
    pool._deliver(sh, 4, [{"type": "Quote", "symbol": "X"}])
    pool._deliver(sh, 5, [{"type": "Quote", "symbol": "Y"}])
    assert [e["symbol"] for e in events] == ["Y"]


async def test_session_limit_uses_long_backoff(tokens, http, fake_dx: FakeDxLink) -> None:
    fake_dx.session_limit = True
    delays: list[float] = []

    async def fake_sleep(d: float) -> None:
        delays.append(d)
        await asyncio.sleep(0.01)

    pool = make_pool(
        tokens,
        http,
        sleep=fake_sleep,
        limit_backoff_factory=lambda: Backoff(base=30, cap=300, jitter=0),
    )
    await pool.add([K1])
    await eventually(lambda: len(delays) >= 3)
    assert delays[:3] == [30, 60, 120]
    assert pool.state == "reconnecting"
    await pool.close()


async def test_budget_is_never_exceeded(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(1)
    pool = make_pool(tokens, http, budget=budget, per_socket=1, max_sockets=3)
    await pool.add([K1, K2, K3])
    await asyncio.sleep(0.5)
    assert budget.peak == 1 and fake_dx.peak_open == 1
    await pool.close()
    assert budget.in_use == 0


async def test_new_shard_is_not_reported_live(tokens, http, fake_dx: FakeDxLink) -> None:
    pool = make_pool(tokens, http, per_socket=1, max_sockets=2)
    await pool.add([K1])
    await eventually(lambda: pool.state == "live")
    await pool.add([K2])
    assert pool.state != "live"
    await eventually(lambda: pool.state == "live" and fake_dx.all_subs() == {K1, K2})
    await pool.close()


async def test_add_racing_stop_loses_no_keys(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    pool = make_pool(tokens, http, budget=budget)
    await pool.add([K1])
    await eventually(lambda: pool.state == "live")
    stopper = asyncio.create_task(pool.remove([K1]))
    await asyncio.sleep(0)
    await pool.add([K2])
    await stopper
    await eventually(lambda: pool.state == "live" and fake_dx.all_subs() == {K2})
    assert fake_dx.peak_open == 1
    await pool.close()
    assert budget.in_use == 0


async def test_close_releases_budget_immediately(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    pool = make_pool(tokens, http, budget=budget)
    await pool.add([K1, K2])
    await eventually(lambda: pool.state == "live")
    await pool.close()
    assert budget.in_use == 0


async def test_candle_from_time_is_the_current_bar_start_on_add_and_resubscribe(
    tokens, http, fake_dx: FakeDxLink, monkeypatch: pytest.MonkeyPatch
) -> None:
    kc = ("Candle", "SPX{=1m}")
    kc_dx = ("Candle", "SPX{=m}")  # as the pool subscribes it and dxFeed files it
    t0 = 1_727_600_123_456  # 43.456s into a minute
    now = {"ms": t0}
    monkeypatch.setattr(stream_pool, "_now_ms", lambda: now["ms"])
    pool = make_pool(tokens, http)
    await pool.add([kc])
    await eventually(lambda: pool.state == "live" and fake_dx.all_subs() >= {kc_dx})
    # the in-progress bar (started at :00) is included
    assert fake_dx.conns[0].subs[kc_dx] == 1_727_600_100_000
    now["ms"] = t0 + 60_000  # a minute later the stream reconnects
    gen = pool.generation
    await fake_dx.drop_all()
    await eventually(
        lambda: pool.state == "live" and pool.generation > gen and len(fake_dx.candle_requests) >= 2
    )
    assert fake_dx.candle_requests[-1] == ("SPX{=m}", 1_727_600_160_000)
    await pool.close()


async def test_keys_removed_during_resubscribe_are_unsubscribed(
    tokens, http, fake_dx: FakeDxLink
) -> None:
    pool = make_pool(tokens, http)
    await pool.add([K1, K2])
    await eventually(lambda: pool.state == "live")
    await fake_dx.drop_all()
    await pool.remove([K1])
    await eventually(lambda: pool.state == "live" and fake_dx.all_subs() == {K2}, timeout=3)
    await pool.close()


async def test_unexpected_error_does_not_kill_the_shard(tokens, http, fake_dx: FakeDxLink) -> None:
    pool = make_pool(tokens, http)
    real = pool._resubscribe
    calls = 0

    async def flaky(sh, conn):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        await real(sh, conn)

    pool._resubscribe = flaky  # type: ignore[method-assign]
    await pool.add([K1])
    await eventually(lambda: pool.state == "live" and fake_dx.all_subs() == {K1}, timeout=3)
    await pool.close()


def _pool_tasks() -> list[asyncio.Task[Any]]:
    return [
        t for t in asyncio.all_tasks() if (t.get_name() or "").startswith("stream") and not t.done()
    ]


async def test_close_during_deferred_start_leaves_nothing_running(
    tokens, http, fake_dx: FakeDxLink
) -> None:
    budget = SessionBudget(5)
    pool = make_pool(tokens, http, budget=budget)
    await pool.add([K1])
    await eventually(lambda: pool.state == "live")
    stopper = asyncio.create_task(pool.remove([K1]))
    await asyncio.sleep(0)
    adder = asyncio.create_task(pool.add([K2]))
    await asyncio.sleep(0)
    await pool.close()
    await asyncio.gather(stopper, adder, return_exceptions=True)
    assert budget.in_use == 0 and not _pool_tasks()
    await eventually(lambda: fake_dx.open_count == 0)
    await asyncio.sleep(0.2)
    assert fake_dx.open_count == 0 and budget.in_use == 0 and not _pool_tasks()


async def test_add_after_close_is_rejected(tokens, http, fake_dx: FakeDxLink) -> None:
    pool = make_pool(tokens, http)
    await pool.close()
    assert await pool.add([K1, K2]) == [K1, K2]
    await asyncio.sleep(0.1)
    assert fake_dx.open_count == 0 and not pool.keys()


async def test_cancelled_add_during_stop_still_subscribes(
    tokens, http, fake_dx: FakeDxLink
) -> None:
    pool = make_pool(tokens, http)
    await pool.add([K1])
    await eventually(lambda: pool.state == "live")
    stopper = asyncio.create_task(pool.remove([K1]))
    await asyncio.sleep(0)
    adder = asyncio.create_task(pool.add([K2]))
    await asyncio.sleep(0)
    adder.cancel()
    await asyncio.gather(stopper, adder, return_exceptions=True)
    await eventually(lambda: pool.state == "live" and fake_dx.all_subs() == {K2})
    await pool.close()


async def test_subscription_sends_on_a_shard_never_interleave(
    tokens, http, fake_dx: FakeDxLink
) -> None:
    pool = make_pool(tokens, http)
    await pool.add([K1])
    await eventually(lambda: pool.state == "live")
    sh = pool._shards[0]
    assert sh.conn is not None
    real = sh.conn.subscribe
    inflight = peak = 0

    async def slow_subscribe(*a: Any, **kw: Any) -> None:  # a multi-chunk send
        nonlocal inflight, peak
        inflight += 1
        peak = max(peak, inflight)
        try:
            for _ in range(3):
                await asyncio.sleep(0)
            await real(*a, **kw)
        finally:
            inflight -= 1

    sh.conn.subscribe = slow_subscribe  # type: ignore[method-assign]
    await asyncio.gather(pool.add([K2]), pool.remove([K1]), pool.add([K3]))
    assert peak == 1
    await eventually(lambda: fake_dx.all_subs() == {K2, K3})
    await pool.close()
