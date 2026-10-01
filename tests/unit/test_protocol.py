from typing import Any

import pytest

from ttfeedhub.upstream import protocol as p


def test_setup_and_auth() -> None:
    s = p.setup()
    assert s["type"] == "SETUP" and s["channel"] == 0
    assert s["keepaliveTimeout"] == 60 and s["acceptKeepaliveTimeout"] == 30
    assert p.auth("qt") == {"type": "AUTH", "channel": 0, "token": "qt"}
    assert p.keepalive() == {"type": "KEEPALIVE", "channel": 0}


def test_feed_setup_requests_compact_unaggregated_fields() -> None:
    m = p.feed_setup()
    assert m["channel"] == p.FEED_CHANNEL
    assert m["acceptDataFormat"] == "COMPACT"
    assert m["acceptAggregationPeriod"] == 0
    assert m["acceptEventFields"]["Quote"][0] == "eventSymbol"
    assert set(m["acceptEventFields"]) == {
        "Quote",
        "Trade",
        "TimeAndSale",
        "Greeks",
        "Summary",
        "Candle",
    }
    assert "prevDayVolume" in m["acceptEventFields"]["Summary"]  # SDK Summary.prev_day_volume


def test_sub_entry_candle_from_time() -> None:
    assert p.sub_entry(("Quote", "SPX")) == {"type": "Quote", "symbol": "SPX"}
    assert p.sub_entry(("Candle", "SPX{=1m}"), 123) == {
        "type": "Candle",
        "symbol": "SPX{=m}",  # upstream always gets dxFeed's canonical form
        "fromTime": 123,
    }


def test_subscription_messages_are_chunked_at_50() -> None:
    adds = [p.sub_entry(("Quote", f"S{i}")) for i in range(120)]
    msgs = p.subscription_msgs(adds, reset=True)
    assert [len(m["add"]) for m in msgs] == [50, 50, 20]
    assert msgs[0]["reset"] is True and "reset" not in msgs[1]
    rem = p.subscription_msgs([], [p.sub_entry(("Quote", "A"))])
    assert rem == [
        {"type": "FEED_SUBSCRIPTION", "channel": 1, "remove": [{"type": "Quote", "symbol": "A"}]}
    ]


def test_reset_without_adds() -> None:
    assert p.subscription_msgs([], reset=True) == [
        {"type": "FEED_SUBSCRIPTION", "channel": 1, "reset": True}
    ]


def test_decode_compact_rows_and_multiple_types() -> None:
    fields = {"Quote": ("eventSymbol", "bidPrice", "askPrice"), "Trade": ("eventSymbol", "price")}
    data = ["Quote", ["SPX", 1.0, 2.0, "SPY", 3.0, "NaN"], "Trade", ["SPX", 5.5]]
    assert p.decode_feed_data(data, fields) == [
        {"type": "Quote", "symbol": "SPX", "bidPrice": 1.0, "askPrice": 2.0},
        {"type": "Quote", "symbol": "SPY", "bidPrice": 3.0, "askPrice": None},
        {"type": "Trade", "symbol": "SPX", "price": 5.5},
    ]


@pytest.mark.parametrize(
    "data",
    [["Quote"], ["Quote", ["SPX", 1.0]], ["Nope", []], ["Quote", "notalist"]],
)
def test_decode_rejects_malformed(data: list[Any]) -> None:
    with pytest.raises(p.ProtocolError):
        p.decode_feed_data(data, {"Quote": ("eventSymbol", "bidPrice", "askPrice")})
