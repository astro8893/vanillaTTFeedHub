import asyncio
from typing import Any

import pytest

from tests.fakes.fake_dxlink import FakeDxLink, quote_row
from tests.helpers import eventually
from tests.integration.test_stream_endpoint import connect, hello, recv_until
from ttfeedhub.config import Settings
from ttfeedhub.hub import Hub


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
        queue_limit_all=5,
    )


async def test_stalled_client_is_evicted_and_others_keep_receiving(
    app_client: Any, fake_dx: FakeDxLink, hub: Hub
) -> None:
    slow = await connect(app_client)
    await hello(slow)
    await slow.send_json({"op": "sub", "id": 1, "subs": [{"type": "Quote", "symbol": "SPX"}]})
    await recv_until(slow, lambda m: m["t"] == "ack")
    # slow now stops reading entirely
    good = await connect(app_client, "recorder")
    await hello(good)
    await good.send_json(
        {"op": "sub", "id": 1, "mode": "latest", "subs": [{"type": "Quote", "symbol": "SPX"}]}
    )
    await recv_until(good, lambda m: m["t"] == "ack")
    await eventually(lambda: ("Quote", "SPX") in fake_dx.all_subs())
    assert hub.conn_count["ui"] == 1
    await fake_dx.emit("Quote", [quote_row("SPX", float(i), i + 1.0) for i in range(50)])
    await eventually(lambda: hub.conn_count["ui"] == 0, timeout=5)
    assert hub.slow_evictions == 1
    assert hub.keys_by_name["ui"] == 0
    assert hub.conn_count["recorder"] == 1
    await fake_dx.emit("Quote", [quote_row("SPX", 999.0, 1000.0)])
    ev = await recv_until(good, lambda m: m["t"] == "ev" and m["d"][-1]["bidPrice"] == 999.0)
    assert ev["d"][-1]["symbol"] == "SPX"


async def test_max_keys_holds_across_concurrent_connections(app_client: Any, hub: Hub) -> None:
    a = await connect(app_client, "recorder")
    b = await connect(app_client, "recorder")
    await hello(a)
    await hello(b)
    await a.send_json(
        {"op": "sub", "id": 1, "subs": [{"type": "Quote", "symbol": s} for s in "ABC"]}
    )
    await b.send_json(
        {"op": "sub", "id": 1, "subs": [{"type": "Quote", "symbol": s} for s in "XYZ"]}
    )
    acks = await asyncio.gather(
        recv_until(a, lambda m: m["t"] == "ack"), recv_until(b, lambda m: m["t"] == "ack")
    )
    rejected = sum(len(x["rejected"]) for x in acks)
    assert rejected == 3
    assert hub.keys_by_name["recorder"] == 3
    assert sum(len(s.keys) for s in hub.sessions) == 3
