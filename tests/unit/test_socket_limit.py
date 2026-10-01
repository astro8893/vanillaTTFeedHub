from typing import Any

import pytest

from ttfeedhub.tools.socket_limit import chain_symbols, measure, recommend
from ttfeedhub.upstream.connection import UpstreamError


def test_chain_symbols_dedupes_in_order() -> None:
    body = {
        "data": {
            "items": [
                {
                    "expirations": [
                        {
                            "strikes": [
                                {"call-streamer-symbol": ".A1C", "put-streamer-symbol": ".A1P"},
                                {"call-streamer-symbol": ".A2C", "put-streamer-symbol": None},
                            ]
                        },
                        {
                            "strikes": [
                                {"call-streamer-symbol": ".A1C", "put-streamer-symbol": ".A3P"}
                            ]
                        },
                    ]
                }
            ]
        }
    }
    assert chain_symbols(body) == [".A1C", ".A1P", ".A2C", ".A3P"]


def test_recommend_is_80_percent_of_largest_alive_step() -> None:
    results = [
        {"subscribed": 1000, "alive": True},
        {"subscribed": 4000, "alive": True},
        {"subscribed": 6000, "alive": False},
    ]
    assert recommend(results) == 3200
    assert recommend([{"subscribed": 1000, "alive": False}]) is None


class StubConn:
    def __init__(self, die_after: int) -> None:
        self.calls = 0
        self.live = True
        self.die_after = die_after

    async def subscribe(self, add: Any = (), remove: Any = (), *, reset: bool = False) -> None:
        self.calls += 1
        if self.calls >= self.die_after:
            self.live = False


async def test_measure_stops_when_socket_dies() -> None:
    conn = StubConn(die_after=2)
    results = await measure(conn, [f"S{i}" for i in range(50)], [10, 20, 30], 0.01, set())  # type: ignore[arg-type]
    assert [r["subscribed"] for r in results] == [10, 20]
    assert results[-1]["alive"] is False


class StubConnWithError:
    def __init__(self, fail_on_call: int) -> None:
        self.calls = 0
        self.live = True
        self.fail_on_call = fail_on_call

    async def subscribe(self, add: Any = (), remove: Any = (), *, reset: bool = False) -> None:
        self.calls += 1
        if self.calls == self.fail_on_call:
            raise UpstreamError("connection lost")


async def test_measure_handles_upstream_error_during_subscribe() -> None:
    conn = StubConnWithError(fail_on_call=2)
    results = await measure(conn, [f"S{i}" for i in range(50)], [10, 20, 30], 0.01, set())  # type: ignore[arg-type]
    assert [r["subscribed"] for r in results] == [10, 20]
    assert results[-1]["alive"] is False
    assert results[-1]["symbols_with_data"] == 0


def test_parse_steps_validates_positive() -> None:
    import argparse

    from ttfeedhub.tools.socket_limit import _parse_steps

    # Valid positive steps
    assert _parse_steps("100,50,200") == [50, 100, 200]  # Sorted
    assert _parse_steps("1") == [1]

    # Invalid non-positive steps
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_steps("100,0,200")
    with pytest.raises(argparse.ArgumentTypeError):
        _parse_steps("-1")


def test_chain_symbols_returns_empty_list_for_empty_body() -> None:
    assert chain_symbols({}) == []
    assert chain_symbols({"data": None}) == []
    assert chain_symbols({"data": {"items": []}}) == []


@pytest.mark.parametrize(
    ("stats", "refused"),
    [
        ({"sockets": {"in_use": 0, "limit": 5}}, False),
        ({"sockets": {"in_use": 4, "limit": 5}}, False),
        ({"sockets": {"in_use": 5, "limit": 5}}, True),
        ({"sockets": {"in_use": 9}}, True),
        ({"sockets": {}}, True),
        ({"sockets": {"in_use": "2"}}, True),
        ({}, True),
    ],
)
def test_refusal_reason(stats: dict[str, Any], refused: bool) -> None:
    from ttfeedhub.tools.socket_limit import refusal_reason

    assert (refusal_reason(stats) is not None) is refused


class _Probe:
    """Stubs the hub query and the measurement so main() never touches the network."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, stats: Any) -> None:
        from ttfeedhub.tools import socket_limit

        self.ran = False
        self.queried: list[tuple[str, str]] = []

        async def fetch(url: str, token: str) -> Any:
            self.queried.append((url, token))
            if isinstance(stats, Exception):
                raise stats
            return stats

        async def run(*a: Any) -> int:
            self.ran = True
            return 0

        monkeypatch.setattr(socket_limit, "fetch_hub_stats", fetch)
        monkeypatch.setattr(socket_limit, "_run", run)


def _token(tmp_path: Any) -> str:
    f = tmp_path / "tok"
    f.write_text("tok-smoke-0123456789\n")
    return str(f)


def test_main_requires_yes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    from ttfeedhub.tools.socket_limit import main

    probe = _Probe(monkeypatch, {"sockets": {"in_use": 1, "limit": 5}})
    assert main(["--hub-token-file", _token(tmp_path)]) != 0
    assert not probe.ran
    assert probe.queried == [("http://127.0.0.1:8700", "tok-smoke-0123456789")]
    err = capsys.readouterr().err
    assert "account-wide" in err and "other apps on this account" in err and "--yes" in err


def test_main_refuses_when_hub_holds_too_many_sessions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    from ttfeedhub.tools.socket_limit import main

    probe = _Probe(monkeypatch, {"sockets": {"in_use": 5, "limit": 5}})
    assert main(["--hub-token-file", _token(tmp_path), "--yes"]) != 0
    assert not probe.ran


def test_main_refuses_without_hub_stats(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    from ttfeedhub.tools.socket_limit import main

    probe = _Probe(monkeypatch, OSError("connection refused"))
    assert main(["--hub-token-file", _token(tmp_path), "--yes"]) != 0
    assert main(["--yes"]) != 0  # no token file: cannot query the hub
    assert not probe.ran


def test_main_proceeds_with_yes_and_room(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
    from ttfeedhub.tools.socket_limit import main

    probe = _Probe(monkeypatch, {"sockets": {"in_use": 4, "limit": 5}})
    url = "http://hub:8700"
    assert main(["--hub-url", url, "--hub-token-file", _token(tmp_path), "--yes"]) == 0
    assert probe.ran and probe.queried[0][0] == url
