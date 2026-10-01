import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from tests.fakes.fake_dxlink import FakeDxLink, quote_row
from tests.helpers import auth, eventually
from ttfeedhub.hub import Hub

DAY = 86_400_000


async def test_health_idle(app_client: Any) -> None:
    r = await app_client.get("/health")
    assert r.status == 200
    assert await r.json() == {"ok": True, "state": "idle"}


async def test_health_auth_failed_is_503(app_client: Any, hub: Hub) -> None:
    hub._auth_ok = False
    r = await app_client.get("/health")
    assert r.status == 503
    assert (await r.json())["state"] == "auth_failed"


async def test_stats_requires_auth(app_client: Any) -> None:
    assert (await app_client.get("/v1/stats")).status == 401
    r = await app_client.get("/v1/stats", headers=auth())
    body = await r.json()
    assert r.status == 200 and body["sockets"]["limit"] == 5


async def test_snapshot_cached_and_one_shot(app_client: Any, fake_dx: FakeDxLink) -> None:
    fake_dx.auto_rows[("Quote", "SPY")] = quote_row("SPY", 500.0, 500.1)
    r = await app_client.get(
        "/v1/snapshot", params=[("key", "Quote:SPY"), ("key", "Quote:NODATA")], headers=auth()
    )
    body = await r.json()
    assert r.status == 200
    assert [e["symbol"] for e in body["d"]] == ["SPY"]
    assert body["missing"] == ["Quote:NODATA"]
    await eventually(lambda: fake_dx.open_count == 0, timeout=3)  # one-shot lingers, then leaves


async def test_snapshot_validation(app_client: Any) -> None:
    assert (await app_client.get("/v1/snapshot", headers=auth())).status == 400
    many = [("key", f"Quote:S{i}") for i in range(2001)]
    assert (
        await app_client.get("/v1/snapshot", params=many, headers=auth("recorder"))
    ).status == 400
    r = await app_client.get(
        "/v1/snapshot",
        params=[("key", "Greeks:SPX"), ("key", "noseparator")],
        headers=auth("recorder"),
    )
    body = await r.json()
    assert {x["reason"] for x in body["rejected"]} == {"type", "format"}


def option_keys(n: int) -> list[str]:
    return [f"Greeks:.SPXW271217{'CP'[i % 2]}{4000 + i}" for i in range(n)]


async def test_snapshot_post_accepts_2000_option_keys(app_client: Any) -> None:
    keys = option_keys(2000)
    r = await app_client.post("/v1/snapshot", json={"keys": keys}, headers=auth())
    assert r.status == 200
    body = await r.json()
    assert len(body["missing"]) == 2000
    # ui has max_keys=100: the rest are not fetched
    assert sum(x["reason"] == "max_keys" for x in body["rejected"]) == 1900


async def test_snapshot_post_validation(app_client: Any) -> None:
    for payload in (b"not json", b'{"keys": "Quote:SPX"}', b'{"keys": [1]}', b"[]"):
        r = await app_client.post("/v1/snapshot", data=payload, headers=auth())
        assert r.status == 400, payload
    r = await app_client.post("/v1/snapshot", json={"keys": option_keys(2001)}, headers=auth())
    assert r.status == 400
    assert (await app_client.post("/v1/snapshot", json={"keys": ["Quote:SPX"]})).status == 401


async def test_snapshot_rate_limited_per_client(app_client: Any) -> None:
    body = {"keys": ["noseparator"]}  # parses to nothing, so no one-shot wait
    for _ in range(3):  # ui: snapshots_per_min=3
        assert (await app_client.post("/v1/snapshot", json=body, headers=auth())).status == 200
    assert (await app_client.post("/v1/snapshot", json=body, headers=auth())).status == 429
    r = await app_client.get("/v1/snapshot", params=[("key", "Quote:SPX")], headers=auth())
    assert r.status == 429  # GET shares the bucket
    # other clients have their own bucket
    r = await app_client.post("/v1/snapshot", json=body, headers=auth("recorder"))
    assert r.status == 200


async def test_snapshot_one_shot_counts_against_max_keys(
    app_client: Any, hub: Hub, monkeypatch: Any
) -> None:
    fetched: list[list[tuple[str, str]]] = []
    real = hub.one_shot

    async def spy(keys: list[tuple[str, str]], wait_s: float) -> list[Any]:
        fetched.append(list(keys))
        assert hub.keys_by_name["recorder"] == len(keys)  # reserved while fetching
        return await real(keys, wait_s)

    monkeypatch.setattr(hub, "one_shot", spy)
    keys = [f"Quote:{s}" for s in "ABCDE"]  # recorder: max_keys=3
    r = await app_client.post("/v1/snapshot", json={"keys": keys}, headers=auth("recorder"))
    body = await r.json()
    assert r.status == 200
    assert fetched == [[("Quote", "A"), ("Quote", "B"), ("Quote", "C")]]
    assert [(x["symbol"], x["reason"]) for x in body["rejected"]] == [
        ("D", "max_keys"),
        ("E", "max_keys"),
    ]
    assert body["missing"] == keys
    assert hub.keys_by_name["recorder"] == 0


async def test_candles_endpoint(app_client: Any, fake_dx: FakeDxLink) -> None:
    start = int(time.time() * 1000) - 10 * DAY
    fake_dx.candles["SPX{=1d}"] = [(start + i * DAY, 1.0, 2.0, 0.5, 1.5, 10.0) for i in range(5)]
    r = await app_client.get(
        "/v1/candles",
        params={"symbol": "SPX", "interval": "1d", "from": str(start)},
        headers=auth(),
    )
    assert r.status == 200
    assert len((await r.json())["d"]) == 5


async def test_candles_bad_params(app_client: Any) -> None:
    now = str(int(time.time() * 1000))
    for params in (
        {"symbol": "SPX", "interval": "2m", "from": now},
        {"symbol": "SP X", "interval": "1d", "from": now},
        {"symbol": "SPX", "interval": "1d"},
        {"symbol": "SPX", "interval": "1d", "from": "0"},
    ):
        assert (await app_client.get("/v1/candles", params=params, headers=auth())).status == 400
    r = await app_client.get(
        "/v1/candles",
        params={"symbol": "SPX", "interval": "1d", "from": now},
        headers=auth("recorder"),
    )
    assert r.status == 403  # recorder may not use Candle


async def test_candles_rate_limit_and_timeout(app_client: Any) -> None:
    params = {"symbol": "NOPE", "interval": "1d", "from": str(int(time.time() * 1000) - DAY)}
    assert (await app_client.get("/v1/candles", params=params, headers=auth())).status == 504
    assert (await app_client.get("/v1/candles", params=params, headers=auth())).status == 504
    assert (await app_client.get("/v1/candles", params=params, headers=auth())).status == 429


def test_main_exits_2_without_secrets(tmp_path: Path) -> None:
    env = {
        **os.environ,
        "TTFH_SECRETS_DIR": str(tmp_path),
        "PYTHONPATH": str(Path(__file__).parents[2] / "src"),
    }
    p = subprocess.run(
        [sys.executable, "-m", "ttfeedhub"], env=env, capture_output=True, text=True, timeout=30
    )
    assert p.returncode == 2
    assert "missing secret file" in p.stderr
