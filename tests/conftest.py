from collections.abc import AsyncIterator

import aiohttp
import pytest

from tests.fakes.fake_api import FakeApi
from tests.fakes.fake_dxlink import FakeDxLink
from ttfeedhub.tokens import TokenManager


@pytest.fixture
async def fake_dx() -> AsyncIterator[FakeDxLink]:
    dx = FakeDxLink()
    await dx.start()
    yield dx
    await dx.close()


@pytest.fixture
async def fake_api(fake_dx: FakeDxLink) -> AsyncIterator[FakeApi]:
    api = FakeApi()
    api.quote_url = fake_dx.url
    await api.start()
    yield api
    await api.close()


@pytest.fixture
async def http() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as s:
        yield s


@pytest.fixture
def tokens(http: aiohttp.ClientSession, fake_api: FakeApi) -> TokenManager:
    return TokenManager(
        http,
        api_base=fake_api.url,
        client_secret="client-secret-xyz",
        refresh_token="refresh-token-xyz",
        allow_insecure=True,
    )
