import aiohttp
import pytest

from tests.fakes.fake_api import FakeApi
from ttfeedhub.logsafe import redact
from ttfeedhub.tokens import AuthFailed, ScopeError, TokenError, TokenManager


class Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def mgr(http: aiohttp.ClientSession, api: FakeApi, clock: Clock | None = None) -> TokenManager:
    return TokenManager(
        http,
        api_base=api.url,
        client_secret="client-secret-xyz",
        refresh_token="refresh-token-xyz",
        allow_insecure=True,
        clock=clock or Clock(),
    )


async def test_access_token_cached_until_near_expiry(
    http: aiohttp.ClientSession, fake_api: FakeApi
) -> None:
    clock = Clock()
    tm = mgr(http, fake_api, clock)
    a = await tm.access_token()
    assert await tm.access_token() == a
    assert fake_api.refresh_calls == 1
    assert fake_api.last_refresh_body == {
        "grant_type": "refresh_token",
        "client_secret": "client-secret-xyz",
        "refresh_token": "refresh-token-xyz",
    }
    clock.t += 870  # within 60s of the 900s expiry
    assert await tm.access_token() != a
    assert fake_api.refresh_calls == 2


async def test_rejected_grant_raises_auth_failed(
    http: aiohttp.ClientSession, fake_api: FakeApi
) -> None:
    fake_api.refresh_status = 401
    with pytest.raises(AuthFailed):
        await mgr(http, fake_api).access_token()


async def test_server_error_is_transient(http: aiohttp.ClientSession, fake_api: FakeApi) -> None:
    fake_api.refresh_status = 503
    with pytest.raises(TokenError) as ei:
        await mgr(http, fake_api).access_token()
    assert not isinstance(ei.value, AuthFailed)


async def test_trade_scope_is_refused(http: aiohttp.ClientSession, fake_api: FakeApi) -> None:
    fake_api.scope = "read trade"
    with pytest.raises(ScopeError):
        await mgr(http, fake_api).access_token()


async def test_missing_scope_is_allowed(http: aiohttp.ClientSession, fake_api: FakeApi) -> None:
    fake_api.scope = None
    tm = mgr(http, fake_api)
    await tm.access_token()
    assert tm.scopes is None


async def test_quote_token_cached_forced_and_invalidated(
    http: aiohttp.ClientSession, fake_api: FakeApi
) -> None:
    tm = mgr(http, fake_api)
    q1 = await tm.quote_token()
    assert q1.url == fake_api.quote_url
    assert fake_api.last_auth == f"Bearer {await tm.access_token()}"
    assert await tm.quote_token() is q1
    q2 = await tm.quote_token(force=True)
    assert q2.token != q1.token and fake_api.quote_calls == 2
    tm.invalidate_quote_token()
    await tm.quote_token()
    assert fake_api.quote_calls == 3


async def test_plain_ws_url_refused_unless_allowed(
    http: aiohttp.ClientSession, fake_api: FakeApi
) -> None:
    tm = TokenManager(
        http,
        api_base=fake_api.url,
        client_secret="client-secret-xyz",
        refresh_token="refresh-token-xyz",
    )
    with pytest.raises(TokenError, match="wss"):
        await tm.quote_token()


async def test_tokens_registered_for_redaction(
    http: aiohttp.ClientSession, fake_api: FakeApi
) -> None:
    tm = mgr(http, fake_api)
    a = await tm.access_token()
    q = await tm.quote_token()
    assert a not in redact(f"x {a}")
    assert q.token not in redact(q.token)


async def test_non_json_200_is_transient_token_error(
    http: aiohttp.ClientSession, fake_api: FakeApi
) -> None:
    fake_api.refresh_body_override = "<html>maintenance</html>"
    with pytest.raises(TokenError) as ei:
        await mgr(http, fake_api).access_token()
    assert not isinstance(ei.value, AuthFailed)


async def test_bad_expires_in_is_token_error(
    http: aiohttp.ClientSession, fake_api: FakeApi
) -> None:
    fake_api.expires_in = "soon"
    with pytest.raises(TokenError, match="expires_in"):
        await mgr(http, fake_api).access_token()


async def test_non_string_scope_fails_closed(
    http: aiohttp.ClientSession, fake_api: FakeApi
) -> None:
    fake_api.scope = ["read", "trade"]
    with pytest.raises(ScopeError):
        await mgr(http, fake_api).access_token()


async def test_invalidate_only_the_rejected_quote_token(
    http: aiohttp.ClientSession, fake_api: FakeApi
) -> None:
    tm = mgr(http, fake_api)
    q1 = await tm.quote_token()
    tm.invalidate_quote_token(rejected=q1)
    q2 = await tm.quote_token()
    assert q2.token != q1.token and fake_api.quote_calls == 2
    tm.invalidate_quote_token(rejected=q1)  # a late report about the old token
    assert await tm.quote_token() is q2 and fake_api.quote_calls == 2
