from typing import Any

from ttfeedhub.core.cache import LatestCache
from ttfeedhub.core.dispatch import Dispatcher


class Sink:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self.ctrl: list[dict[str, Any]] = []

    def push(self, ev: dict[str, Any]) -> None:
        self.events.append(ev)

    def push_control(self, msg: dict[str, Any]) -> None:
        self.ctrl.append(msg)


def ev(sym: str, **f: Any) -> dict[str, Any]:
    return {"type": "Quote", "symbol": sym, **f}


def test_on_events_stamps_rt_updates_cache_and_routes() -> None:
    cache = LatestCache()
    d = Dispatcher(cache, clock=lambda: 123.0)
    a, b = Sink(), Sink()
    d.attach([("Quote", "SPX")], a)
    d.attach([("Quote", "SPY")], b)
    d.on_events([ev("SPX", bidPrice=1), ev("SPY", bidPrice=2), ev("QQQ")])
    assert [e["symbol"] for e in a.events] == ["SPX"]
    assert [e["symbol"] for e in b.events] == ["SPY"]
    qqq = cache.get(("Quote", "QQQ"))
    assert qqq is not None and qqq["rt"] == 123.0
    assert d.events_in == 3


def test_detach_stops_delivery() -> None:
    d = Dispatcher(LatestCache())
    a = Sink()
    d.attach([("Quote", "SPX")], a)
    d.detach([("Quote", "SPX")], a)
    d.on_events([ev("SPX")])
    assert a.events == []


def test_broadcast_reaches_registered_sinks_only() -> None:
    d = Dispatcher(LatestCache())
    a, b = Sink(), Sink()
    d.register(a)
    d.broadcast({"t": "status"})
    d.unregister(a)
    d.broadcast({"t": "status2"})
    assert a.ctrl == [{"t": "status"}] and b.ctrl == []


def candle(sym: str, **f: Any) -> dict[str, Any]:
    return {"type": "Candle", "symbol": sym, **f}


def test_candle_events_reach_each_alias_under_its_own_symbol() -> None:
    cache = LatestCache()
    d = Dispatcher(cache, clock=lambda: 1.0)
    a, b = Sink(), Sink()
    d.attach([("Candle", "SPX{=1m,tho=true}")], a)
    d.attach([("Candle", "SPX{=m,tho=true}")], b)
    raw = candle("SPX{=m,tho=true}", close=5.0)  # as dxFeed labels it
    d.on_events([raw])
    assert [(e["symbol"], e["close"]) for e in a.events] == [("SPX{=1m,tho=true}", 5.0)]
    assert b.events == [raw]  # exact match: the shared event, not a copy
    assert raw["symbol"] == "SPX{=m,tho=true}"  # never mutated
    d.detach([("Candle", "SPX{=1m,tho=true}")], a)
    d.on_events([candle("SPX{=m,tho=true}", close=6.0)])
    assert len(a.events) == 1 and len(b.events) == 2


def test_candle_cache_answers_every_alias() -> None:
    cache = LatestCache()
    d = Dispatcher(cache, clock=lambda: 1.0)
    d.on_events([candle("SPX{=m,tho=true}", close=5.0)])
    got = cache.get(("Candle", "SPX{=1m,tho=true}"))
    assert got is not None and got["symbol"] == "SPX{=1m,tho=true}" and got["close"] == 5.0
    many = cache.get_many([("Candle", "SPX{=m,tho=true}"), ("Candle", "SPX{=1m,tho=true}")])
    assert [e["symbol"] for e in many] == ["SPX{=m,tho=true}", "SPX{=1m,tho=true}"]
