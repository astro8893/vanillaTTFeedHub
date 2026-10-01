import asyncio

import aiohttp
import pytest

from tests.fakes.fake_api import FakeApi
from tests.fakes.fake_dxlink import FakeDxLink, quote_row
from tests.helpers import eventually
from ttfeedhub.tokens import TokenManager
from ttfeedhub.types import Event
from ttfeedhub.upstream import protocol
from ttfeedhub.upstream.connection import (
    HandshakeTimeout,
    QuoteTokenRejected,
    SessionBudget,
    SessionLimitError,
    SilenceTimeout,
    UpstreamConnection,
)


def make(
    tokens: TokenManager,
    http: aiohttp.ClientSession,
    budget: SessionBudget,
    sink: list[Event],
    **kw: float,
) -> UpstreamConnection:
    return UpstreamConnection(
        name="t", generation=1, tokens=tokens, http=http, budget=budget, on_events=sink.extend, **kw
    )


async def test_handshake_subscribe_receive_close(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    got: list[Event] = []
    c = make(tokens, http, budget, got)
    await c.open()
    try:
        assert c.live and budget.in_use == 1
        # open() returns as soon as FEED_SETUP is sent; let the server receive it
        await eventually(lambda: len(fake_dx.conns[0].received) >= 4)
        kinds = [m["type"] for m in fake_dx.conns[0].received if m["type"] != "KEEPALIVE"]
        assert kinds == ["SETUP", "AUTH", "CHANNEL_REQUEST", "FEED_SETUP"]
        await c.subscribe(add=[protocol.sub_entry(("Quote", "SPX"))])
        await eventually(lambda: ("Quote", "SPX") in fake_dx.all_subs())
        await fake_dx.emit("Quote", [quote_row("SPX", 5800.0, 5800.5)])
        await eventually(lambda: len(got) == 1)
        assert got[0]["type"] == "Quote" and got[0]["symbol"] == "SPX"
        assert got[0]["bidPrice"] == 5800.0 and c.events_in == 1
    finally:
        await c.close()
    assert budget.in_use == 0 and not c.live
    await eventually(lambda: fake_dx.open_count == 0)


async def test_stalled_handshake_times_out_and_releases_slot(
    tokens, http, fake_dx: FakeDxLink
) -> None:
    fake_dx.stall_handshake = True
    budget = SessionBudget(5)
    with pytest.raises(HandshakeTimeout):
        await make(tokens, http, budget, [], handshake_timeout_s=0.3).open()
    assert budget.in_use == 0
    await eventually(lambda: fake_dx.open_count == 0)


async def test_session_limit_is_classified(tokens, http, fake_dx: FakeDxLink) -> None:
    fake_dx.session_limit = True
    budget = SessionBudget(5)
    with pytest.raises(SessionLimitError):
        await make(tokens, http, budget, []).open()
    assert budget.in_use == 0


async def test_silence_ends_the_connection(tokens, http, fake_dx: FakeDxLink) -> None:
    c = make(tokens, http, SessionBudget(5), [], silence_timeout_s=0.3)
    await c.open()
    fake_dx.silent = True
    cause = await asyncio.wait_for(c.wait_closed(), 2)
    assert isinstance(cause, SilenceTimeout) and not c.live
    await c.close()


async def test_server_drop_is_reported(tokens, http, fake_dx: FakeDxLink) -> None:
    c = make(tokens, http, SessionBudget(5), [])
    await c.open()
    await fake_dx.drop_all()
    cause = await asyncio.wait_for(c.wait_closed(), 2)
    assert cause is not None
    await c.close()


async def test_reauth_with_fresh_token(
    tokens, http, fake_dx: FakeDxLink, fake_api: FakeApi
) -> None:
    c = make(tokens, http, SessionBudget(5), [])
    await c.open()
    calls = fake_api.quote_calls
    await fake_dx.unauthorize()
    await eventually(
        lambda: fake_api.quote_calls == calls + 1 and all(x.authorized for x in fake_dx.conns)
    )
    assert c.live
    await c.close()


async def test_rejected_quote_token_fails_handshake(tokens, http, fake_dx: FakeDxLink) -> None:
    fake_dx.valid_tokens = {"nothing-valid"}
    with pytest.raises(QuoteTokenRejected):
        await make(tokens, http, SessionBudget(5), []).open()


async def test_budget_blocks_extra_sockets(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(1)
    a = make(tokens, http, budget, [])
    await a.open()
    b = make(tokens, http, budget, [])
    opening = asyncio.create_task(b.open())
    await asyncio.sleep(0.2)
    assert not opening.done() and fake_dx.open_count == 1
    await a.close()
    await asyncio.wait_for(opening, 2)
    assert budget.peak == 1
    await b.close()


async def test_keepalives_are_sent(tokens, http, fake_dx: FakeDxLink) -> None:
    c = make(tokens, http, SessionBudget(5), [], keepalive_s=0.05)
    await c.open()
    await eventually(lambda: bool(fake_dx.conns) and fake_dx.conns[0].keepalives >= 2)
    await c.close()


async def test_dispatch_exception_does_not_kill_socket(tokens, http, fake_dx: FakeDxLink) -> None:
    calls = []

    def bad_sink(events: list[Event]) -> None:
        calls.append(len(events))
        raise RuntimeError("consumer bug")

    c = UpstreamConnection(
        name="t",
        generation=1,
        tokens=tokens,
        http=http,
        budget=SessionBudget(5),
        on_events=bad_sink,
    )
    await c.open()
    await c.subscribe(add=[protocol.sub_entry(("Quote", "SPX"))])
    await eventually(lambda: ("Quote", "SPX") in fake_dx.all_subs())
    await fake_dx.emit("Quote", [quote_row("SPX", 1, 2)])
    await fake_dx.emit("Quote", [quote_row("SPX", 1, 2)])
    await eventually(lambda: len(calls) == 2)
    assert c.live
    await c.close()


async def test_cancelled_open_releases_slot(tokens, http, fake_dx: FakeDxLink) -> None:
    fake_dx.stall_handshake = True
    budget = SessionBudget(5)
    task = asyncio.create_task(make(tokens, http, budget, [], handshake_timeout_s=30).open())
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert budget.in_use == 0
    await eventually(lambda: fake_dx.open_count == 0)


async def test_cancelled_close_still_releases_slot(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    c = make(tokens, http, budget, [])
    await c.open()
    task = asyncio.create_task(c.close())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await eventually(lambda: budget.in_use == 0)
    await asyncio.wait_for(c.close(), 2)
    await eventually(lambda: fake_dx.open_count == 0)


async def test_concurrent_closes(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    c = make(tokens, http, budget, [])
    await c.open()
    await asyncio.wait_for(asyncio.gather(c.close(), c.close()), 2)
    assert budget.in_use == 0 and budget.peak == 1
    await eventually(lambda: fake_dx.open_count == 0)


async def test_open_after_close_raises(tokens, http, fake_dx: FakeDxLink) -> None:
    from ttfeedhub.upstream.connection import UpstreamError

    budget = SessionBudget(5)
    c = make(tokens, http, budget, [])
    await c.close()
    with pytest.raises(UpstreamError):
        await c.open()
    assert budget.in_use == 0 and fake_dx.connect_attempts == 0


async def test_second_unauthorized_within_window_is_rejected(
    tokens, http, fake_dx: FakeDxLink
) -> None:
    c = make(tokens, http, SessionBudget(5), [])
    await c.open()
    await fake_dx.unauthorize()
    await eventually(lambda: all(x.authorized for x in fake_dx.conns))
    await fake_dx.unauthorize()
    cause = await asyncio.wait_for(c.wait_closed(), 2)
    assert isinstance(cause, QuoteTokenRejected)
    await c.close()


async def test_close_during_stalled_handshake_closes_socket_before_slot(
    tokens, http, fake_dx: FakeDxLink
) -> None:
    from ttfeedhub.upstream.connection import UpstreamError

    fake_dx.stall_handshake = True
    budget = SessionBudget(1)
    c = make(tokens, http, budget, [], handshake_timeout_s=30)
    opening = asyncio.create_task(c.open())
    await eventually(lambda: fake_dx.open_count == 1)
    await c.close()
    assert budget.in_use == 0
    await eventually(lambda: fake_dx.open_count == 0)
    with pytest.raises(UpstreamError):
        await opening
    # the freed slot is usable again and no socket is left behind
    fake_dx.stall_handshake = False
    d = make(tokens, http, budget, [])
    await d.open()
    assert fake_dx.open_count == 1
    await d.close()


async def test_slot_held_until_socket_closed(tokens, http, fake_dx: FakeDxLink) -> None:
    budget = SessionBudget(5)
    c = make(tokens, http, budget, [])
    await c.open()
    t1 = asyncio.create_task(c.close())
    t2 = asyncio.create_task(c.close())
    await asyncio.sleep(0)
    assert budget.in_use == 1  # teardown has not completed yet
    await asyncio.gather(t1, t2)
    assert budget.in_use == 0
