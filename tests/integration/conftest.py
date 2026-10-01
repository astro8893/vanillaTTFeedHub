from collections.abc import AsyncIterator

import aiohttp
import pytest
from aiohttp.test_utils import TestClient, TestServer

from tests.helpers import TOKENS
from ttfeedhub.config import ClientSpec, Settings, hash_token
from ttfeedhub.hub import Hub
from ttfeedhub.server.app import build_app
from ttfeedhub.tokens import TokenManager
from ttfeedhub.upstream.stream_pool import Backoff


@pytest.fixture
def hub_settings() -> Settings:
    return Settings(
        linger_s=0.1,
        reaper_interval_s=0.05,
        heartbeat_s=0.1,
        handshake_timeout_s=2.0,
        silence_timeout_s=5.0,
        history_timeout_s=0.5,
        history_idle_close_s=0.3,
        one_shot_wait_s=0.5,
        auth_retry_s=0.2,
    )


@pytest.fixture
def hub_clients() -> dict[str, ClientSpec]:
    specs = [
        ClientSpec(
            name="ui",
            token_sha256=hash_token(TOKENS["ui"]),
            max_keys=100,
            max_connections=2,
            candles_per_min=2,
            snapshots_per_min=3,
        ),
        ClientSpec(
            name="recorder",
            token_sha256=hash_token(TOKENS["recorder"]),
            max_keys=3,
            max_connections=2,
            event_types=frozenset({"Quote", "Trade"}),
        ),
    ]
    return {s.token_sha256: s for s in specs}


@pytest.fixture
def hub(
    hub_settings: Settings,
    hub_clients: dict[str, ClientSpec],
    tokens: TokenManager,
    http: aiohttp.ClientSession,
) -> Hub:
    return Hub(
        hub_settings,
        hub_clients,
        tokens,
        http,
        backoff_factory=lambda: Backoff(base=0.05, cap=0.2, jitter=0),
    )


@pytest.fixture
async def app_client(hub: Hub) -> AsyncIterator[TestClient]:  # type: ignore[type-arg]
    client = TestClient(TestServer(build_app(hub)))
    await client.start_server()
    yield client
    await client.close()
