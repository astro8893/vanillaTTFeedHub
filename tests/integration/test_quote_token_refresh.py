"""KI-002: an expired quote token is replaced without a hub restart.

Observed live: DXLink sent ERROR "Your authentication token has expired..." on
stream0, and every reconnect reused the cached token ("Authentication failed")."""

import asyncio
import logging
import re
from typing import Any

import pytest

from tests.fakes.fake_api import FakeApi
from tests.fakes.fake_dxlink import FakeDxLink, quote_row
from tests.helpers import eventually
from ttfeedhub.tokens import TokenManager
from ttfeedhub.types import Event
from ttfeedhub.upstream.connection import SessionBudget
from ttfeedhub.upstream.history import HistoryPool
from ttfeedhub.upstream.stream_pool import Backoff, StreamPool

K1 = ("Quote", "SPX")
DAY = 86_400_000
REFRESH_LOG = "quote token refreshed after DXLink auth error"
QUOTE_TOKEN_RE = re.compile(r"quote-\d+-0123456789")  # FakeApi's token format


def fast() -> Backoff:
    return Backoff(base=0.02, cap=0.05, jitter=0)


def make_pool(tokens, http, events: list[Event]) -> StreamPool:
    return StreamPool(
        tokens=tokens,
        http=http,
        budget=SessionBudget(5),
        on_events=events.extend,
        on_state=lambda s, g: None,
        per_socket=5000,
        max_sockets=3,
        handshake_timeout_s=2.0,
        silence_timeout_s=5.0,
        backoff_factory=fast,
        limit_backoff_factory=fast,
    )


def make_history(tokens, http, **kw: Any) -> HistoryPool:
    opts: dict[str, Any] = dict(
        timeout_s=2.0,
        idle_close_s=30.0,
        quiet_s=0.2,
        handshake_timeout_s=2.0,
        silence_timeout_s=5.0,
    )
    opts.update(kw)
    return HistoryPool(tokens=tokens, http=http, budget=SessionBudget(5), **opts)


def assert_clean_logs(caplog: pytest.LogCaptureFixture, refreshes: int) -> None:
    msgs = [r.getMessage() for r in caplog.records]
    assert sum(REFRESH_LOG in m for m in msgs) == refreshes
    assert [r.levelno for r in caplog.records if REFRESH_LOG in r.getMessage()] == [
        logging.INFO
    ] * refreshes
    assert not any(QUOTE_TOKEN_RE.search(m) for m in msgs)  # tokens are never logged


@pytest.mark.parametrize("code", ["UNAUTHORIZED", "UNKNOWN"])  # UNKNOWN: message-text fallback
async def test_stream_recovers_from_token_expiry_with_one_new_token(
    tokens: TokenManager,
    http,
    fake_dx: FakeDxLink,
    fake_api: FakeApi,
    caplog: pytest.LogCaptureFixture,
    code: str,
) -> None:
    caplog.set_level(logging.DEBUG)
    fake_dx.auth_error_code = code
    events: list[Event] = []
    pool = make_pool(tokens, http, events)
    await pool.add([K1])
    await eventually(lambda: pool.state == "live" and fake_dx.all_subs() == {K1})
    old = fake_dx.conns[0].token
    assert old is not None and fake_api.quote_calls == 1
    gen = pool.generation

    await fake_dx.expire_token(old)

    await eventually(
        lambda: (
            pool.state == "live"
            and pool.generation > gen
            and fake_dx.all_subs() == {K1}
            and all(c.authorized for c in fake_dx.conns)
        )
    )
    await asyncio.sleep(0.3)  # several backoff periods: nothing else is fetched
    assert fake_api.quote_calls == 2
    assert pool.state == "live" and fake_dx.conns[0].token not in (None, old)
    await fake_dx.emit("Quote", [quote_row("SPX", 1, 2)])
    await eventually(lambda: len(events) == 1)
    await pool.close()
    assert_clean_logs(caplog, refreshes=1)


async def test_stream_does_not_hammer_the_token_endpoint(
    tokens: TokenManager, http, fake_dx: FakeDxLink, fake_api: FakeApi
) -> None:
    pool = make_pool(tokens, http, [])
    await pool.add([K1])
    await eventually(lambda: pool.state == "live")
    attempts, calls = fake_dx.connect_attempts, fake_api.quote_calls
    fake_dx.reject_all_tokens = True
    await fake_dx.expire_token(fake_dx.conns[0].token or "")
    await asyncio.sleep(0.5)
    new_attempts = fake_dx.connect_attempts - attempts
    new_calls = fake_api.quote_calls - calls
    assert new_attempts >= 3 and pool.state != "live"
    # at most one forced refresh per backoff cycle (one per reconnect attempt)
    assert 1 <= new_calls <= new_attempts
    fake_dx.reject_all_tokens = False  # tastytrade recovers: so does the stream
    await eventually(lambda: pool.state == "live")
    await pool.close()


async def test_idle_history_socket_token_expiry_refreshes_once(
    tokens: TokenManager,
    http,
    fake_dx: FakeDxLink,
    fake_api: FakeApi,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    fake_dx.candles["SPX{=1d}"] = [(0, 1.0, 2.0, 0.5, 1.5, 10.0)]
    h = make_history(tokens, http)
    assert len(await h.candles("SPX", "1d", 0)) == 1
    old = fake_dx.conns[0].token
    assert old is not None
    await fake_dx.expire_token(old)
    await eventually(lambda: not any(c.live for c, _, _ in h._idle))  # the hub heard it
    assert len(await h.candles("SPX", "1d", 0)) == 1
    assert fake_api.quote_calls == 2
    assert all(c.token != old for c in fake_dx.conns)
    await h.close()
    assert_clean_logs(caplog, refreshes=1)


async def test_history_request_after_silent_expiry_recovers_on_its_own(
    tokens: TokenManager,
    http,
    fake_dx: FakeDxLink,
    fake_api: FakeApi,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No socket open when the token expires: the next request's handshake is the
    first to hear about it, and it must still return bars."""
    caplog.set_level(logging.DEBUG)
    fake_dx.candles["SPX{=1d}"] = [(0, 1.0, 2.0, 0.5, 1.5, 10.0)]
    h = make_history(tokens, http, idle_close_s=0.1)
    assert len(await h.candles("SPX", "1d", 0)) == 1
    old = fake_dx.conns[0].token
    assert old is not None
    await eventually(lambda: fake_dx.open_count == 0)  # reaped
    fake_dx.rejected_tokens.add(old)
    assert len(await h.candles("SPX", "1d", 0)) == 1
    assert fake_api.quote_calls == 2
    await h.close()
    assert_clean_logs(caplog, refreshes=1)


async def test_late_error_on_old_token_keeps_the_new_one(
    tokens: TokenManager, http, fake_dx: FakeDxLink, fake_api: FakeApi
) -> None:
    """Two sockets on the same expired token: only one new token is fetched."""
    fake_dx.candles["SPX{=1d}"] = [(0, 1.0, 2.0, 0.5, 1.5, 10.0)]
    pool = make_pool(tokens, http, [])
    await pool.add([K1])
    await eventually(lambda: pool.state == "live")
    h = make_history(tokens, http)
    await h.candles("SPX", "1d", 0)
    assert fake_dx.open_count == 2 and fake_api.quote_calls == 1
    old = fake_dx.conns[0].token or ""
    await fake_dx.expire_token(old)
    await eventually(
        lambda: (
            pool.state == "live"
            and fake_dx.all_subs() >= {K1}
            and not any(c.live for c, _, _ in h._idle)
        )
    )
    assert len(await h.candles("SPX", "1d", 0)) == 1
    await asyncio.sleep(0.2)
    assert fake_api.quote_calls == 2
    await h.close()
    await pool.close()
