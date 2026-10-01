import asyncio
from typing import Any

import pytest

from tests.fakes.fake_dxlink import FakeDxLink
from tests.helpers import eventually
from ttfeedhub.upstream.connection import SessionBudget, UpstreamError
from ttfeedhub.upstream.history import HistoryPool, HistoryTimeout

DAY, MIN = 86_400_000, 60_000


def bars(n: int, start: int, step: int) -> list[tuple[int, float, float, float, float, float]]:
    return [(start + i * step, 10.0 + i, 11.0 + i, 9.0 + i, 10.5 + i, 100.0) for i in range(n)]


def make(tokens, http, budget: SessionBudget | None = None, **kw: Any) -> HistoryPool:
    opts: dict[str, Any] = dict(
        timeout_s=1.0, idle_close_s=0.3, quiet_s=0.2, handshake_timeout_s=2.0, silence_timeout_s=5.0
    )
    opts.update(kw)
    return HistoryPool(tokens=tokens, http=http, budget=budget or SessionBudget(5), **opts)


async def test_bars_oldest_first_without_sentinel(tokens, http, fake_dx: FakeDxLink) -> None:
    fake_dx.candles["SPX{=1d}"] = bars(5, 1_000_000, DAY)
    h = make(tokens, http)
    out = await h.candles("SPX", "1d", 1_000_000)
    assert [b["t"] for b in out] == [1_000_000 + i * DAY for i in range(5)]
    assert out[0] == {
        "t": 1_000_000,
        "o": 10.0,
        "h": 11.0,
        "l": 9.0,
        "c": 10.5,
        "v": 100.0,
        "vwap": None,
    }
    await h.close()


async def test_to_bound_and_tho_symbol(tokens, http, fake_dx: FakeDxLink) -> None:
    fake_dx.candles["SPX{=1m,tho=true}"] = bars(10, 0, MIN)
    h = make(tokens, http)
    out = await h.candles("SPX", "1m", 0, to_ms=4 * MIN, tho=True)
    assert len(out) == 5
    await h.close()


async def test_repeat_request_fetches_only_the_gap(tokens, http, fake_dx: FakeDxLink) -> None:
    fake_dx.candles["SPY{=1m}"] = bars(10, 0, MIN)
    h = make(tokens, http)
    await h.candles("SPY", "1m", 0)
    fake_dx.candles["SPY{=1m}"] = bars(12, 0, MIN)
    out = await h.candles("SPY", "1m", 0)
    assert len(out) == 12
    assert fake_dx.candle_requests[-1] == ("SPY{=m}", 9 * MIN - MIN)
    await h.close()


async def test_empty_range_returns_empty_list(tokens, http, fake_dx: FakeDxLink) -> None:
    fake_dx.candles["X{=1d}"] = []
    h = make(tokens, http)
    assert await h.candles("X", "1d", 0) == []
    await h.close()


async def test_unknown_symbol_times_out_and_frees_socket(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    h = make(tokens, http, budget, timeout_s=0.3)
    with pytest.raises(HistoryTimeout):
        await h.candles("NOPE", "1d", 0)
    assert budget.in_use == 0
    await h.close()


async def test_at_most_two_sockets(tokens, http, fake_dx: FakeDxLink) -> None:
    for s in "ABCD":
        fake_dx.candles[f"{s}{{=1d}}"] = bars(3, 0, DAY)
    h = make(tokens, http, max_sockets=2)
    await asyncio.gather(*(h.candles(s, "1d", 0) for s in "ABCD"))
    assert fake_dx.peak_open <= 2
    await h.close()


async def test_idle_socket_is_reused_then_closed(tokens, http, fake_dx: FakeDxLink) -> None:
    fake_dx.candles["A{=1d}"] = bars(3, 0, DAY)
    h = make(tokens, http)
    await h.candles("A", "1d", 0)
    await h.candles("A", "1d", 0)
    assert fake_dx.connect_attempts == 1
    await eventually(lambda: fake_dx.open_count == 0, timeout=2)
    await h.close()


async def test_cancelled_caller_frees_socket_and_slot(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    h = make(tokens, http, budget, timeout_s=30.0)  # unknown symbol: waits until cancelled
    task = asyncio.create_task(h.candles("NOPE", "1d", 0))
    await eventually(lambda: fake_dx.open_count == 1 and budget.in_use == 1)
    await asyncio.sleep(0.1)  # let the request reach its wait
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert budget.in_use == 0
    await eventually(lambda: fake_dx.open_count == 0)
    # the pool slot is free again: a normal request still works
    fake_dx.candles["A{=1d}"] = bars(3, 0, DAY)
    assert len(await h.candles("A", "1d", 0)) == 3
    await h.close()
    assert budget.in_use == 0


async def test_cancel_while_waiting_for_pool_slot(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    h = make(tokens, http, budget, max_sockets=1, timeout_s=30.0)
    first = asyncio.create_task(h.candles("NOPE", "1d", 0))
    await eventually(lambda: budget.in_use == 1)
    second = asyncio.create_task(h.candles("NOPE2", "1d", 0))
    await asyncio.sleep(0.1)
    second.cancel()
    first.cancel()
    await asyncio.gather(first, second, return_exceptions=True)
    assert budget.in_use == 0
    fake_dx.candles["A{=1d}"] = bars(3, 0, DAY)
    out = await asyncio.wait_for(h.candles("A", "1d", 0), timeout=5)
    assert len(out) == 3  # the single pool slot is free again
    await h.close()
    assert budget.in_use == 0


async def test_open_failure_leaks_nothing(tokens, http, fake_dx: FakeDxLink) -> None:
    fake_dx.session_limit = True
    budget = SessionBudget(5)
    h = make(tokens, http, budget)
    with pytest.raises(UpstreamError):
        await h.candles("A", "1d", 0)
    assert budget.in_use == 0
    fake_dx.session_limit = False
    fake_dx.candles["A{=1d}"] = bars(2, 0, DAY)
    assert len(await h.candles("A", "1d", 0)) == 2  # pool slot was released
    await h.close()
    assert budget.in_use == 0


async def test_close_during_request_leaves_no_socket(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    h = make(tokens, http, budget, idle_close_s=30.0, timeout_s=30.0)
    task = asyncio.create_task(h.candles("NOPE", "1d", 0))  # no data: stays in flight
    await eventually(lambda: budget.in_use == 1 and len(fake_dx.candle_requests) == 1)
    await asyncio.sleep(0.2)
    assert not task.done()
    await h.close()
    await asyncio.gather(task, return_exceptions=True)
    assert budget.in_use == 0
    await eventually(lambda: fake_dx.open_count == 0)


async def test_candles_after_close_raises_and_opens_nothing(
    tokens, http, fake_dx: FakeDxLink
) -> None:
    h = make(tokens, http)
    await h.close()
    with pytest.raises(UpstreamError):
        await h.candles("A", "1d", 0)
    assert fake_dx.connect_attempts == 0


async def test_close_racing_reaper_closes_all_expired(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    for s in "AB":
        fake_dx.candles[f"{s}{{=1d}}"] = bars(3, 0, DAY)
    h = make(tokens, http, budget, idle_close_s=0.2)
    await asyncio.gather(h.candles("A", "1d", 0), h.candles("B", "1d", 0))
    assert h.stats()["idle_sockets"] == 2
    await asyncio.sleep(0.2)  # both expired; the reaper is about to close them
    await h.close()
    assert budget.in_use == 0
    await eventually(lambda: fake_dx.open_count == 0)


async def test_overall_timeout_covers_queueing(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    h = make(tokens, http, budget, max_sockets=1, timeout_s=0.4)
    first = asyncio.create_task(h.candles("NOPE", "1d", 0))
    await eventually(lambda: budget.in_use == 1)
    with pytest.raises(HistoryTimeout):
        await h.candles("NOPE2", "1d", 0)  # never gets the slot
    with pytest.raises(HistoryTimeout):
        await first
    assert budget.in_use == 0
    await h.close()


async def test_unsubscribe_failure_keeps_bars(
    tokens, http, fake_dx: FakeDxLink, monkeypatch
) -> None:
    from ttfeedhub.upstream.connection import UpstreamConnection

    real = UpstreamConnection.subscribe

    async def flaky(self, add=(), remove=(), *, reset=False):  # type: ignore[no-untyped-def]
        if remove:
            raise UpstreamError("boom")
        await real(self, add, remove, reset=reset)

    monkeypatch.setattr(UpstreamConnection, "subscribe", flaky)
    budget = SessionBudget(5)
    fake_dx.candles["A{=1d}"] = bars(3, 0, DAY)
    h = make(tokens, http, budget)
    assert len(await h.candles("A", "1d", 0)) == 3
    assert h.stats()["idle_sockets"] == 0  # socket dropped, not reused
    assert budget.in_use == 0
    await h.close()


@pytest.mark.parametrize(
    ("interval", "tho", "step", "sent"),
    [
        ("1m", True, MIN, "SPX{=m,tho=true}"),
        ("1d", False, DAY, "SPX{=d}"),
        ("1h", False, 60 * MIN, "SPX{=h}"),
        ("5m", False, 5 * MIN, "SPX{=5m}"),
    ],
)
async def test_normalized_candle_symbols_return_every_bar(
    tokens, http, fake_dx: FakeDxLink, interval: str, tho: bool, step: int, sent: str
) -> None:
    # The fake labels bars as dxFeed does (SPX{=1m,tho=true} -> SPX{=m,tho=true}).
    fake_dx.candles[f"SPX{{={interval}{',tho=true' if tho else ''}}}"] = bars(6, 0, step)
    h = make(tokens, http)
    out = await h.candles("SPX", interval, 0, tho=tho)
    assert [b["t"] for b in out] == [i * step for i in range(6)]
    assert fake_dx.candle_requests == [(sent, 0)]  # subscribed in canonical form
    await h.close()
