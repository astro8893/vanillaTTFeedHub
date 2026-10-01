"""Pure DXLink wire codec — no I/O.

Message shapes follow dxfeed's DXLink protocol as tastytrade serves it:
SETUP → AUTH_STATE/AUTH → CHANNEL_REQUEST → FEED_SETUP (COMPACT) →
FEED_SUBSCRIPTION, with FEED_DATA arriving as
[type, [row values...], type, [row values...], ...].
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from ..symbols import canonical_key
from ..types import Event, Key

DXLINK_VERSION = "0.1-DXF-JS/0.3.0"
FEED_CHANNEL = 1
SUB_CHUNK = 50  # DXLink closes the socket (1009) when one subscription lists too many
KEEPALIVE_TIMEOUT_S = 60
ACCEPT_KEEPALIVE_TIMEOUT_S = 30

# One fixed field set per event type: the union of what current consumers use.
FIELDS: dict[str, tuple[str, ...]] = {
    "Quote": (
        "eventSymbol",
        "eventTime",
        "bidPrice",
        "askPrice",
        "bidSize",
        "askSize",
        "bidTime",
        "askTime",
    ),
    "Trade": ("eventSymbol", "eventTime", "price", "size", "time", "sequence", "dayVolume"),
    "TimeAndSale": (
        "eventSymbol",
        "eventTime",
        "eventFlags",
        "index",
        "time",
        "sequence",
        "price",
        "size",
        "bidPrice",
        "askPrice",
        "aggressorSide",
    ),
    "Greeks": (
        "eventSymbol",
        "eventTime",
        "eventFlags",
        "index",
        "time",
        "price",
        "volatility",
        "delta",
        "gamma",
        "theta",
        "rho",
        "vega",
    ),
    "Summary": (
        "eventSymbol",
        "eventTime",
        "dayOpenPrice",
        "dayHighPrice",
        "dayLowPrice",
        "dayClosePrice",
        "prevDayClosePrice",
        "prevDayVolume",
        "openInterest",
    ),
    "Candle": (
        "eventSymbol",
        "eventTime",
        "eventFlags",
        "index",
        "time",
        "sequence",
        "count",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "impVolatility",
    ),
}

_NONFINITE = frozenset({"NaN", "Infinity", "-Infinity"})


class ProtocolError(Exception):
    """A DXLink message didn't match the expected shape."""


def setup() -> dict[str, Any]:
    return {
        "type": "SETUP",
        "channel": 0,
        "version": DXLINK_VERSION,
        "keepaliveTimeout": KEEPALIVE_TIMEOUT_S,
        "acceptKeepaliveTimeout": ACCEPT_KEEPALIVE_TIMEOUT_S,
    }


def auth(token: str) -> dict[str, Any]:
    return {"type": "AUTH", "channel": 0, "token": token}


def keepalive() -> dict[str, Any]:
    return {"type": "KEEPALIVE", "channel": 0}


def channel_request() -> dict[str, Any]:
    return {
        "type": "CHANNEL_REQUEST",
        "channel": FEED_CHANNEL,
        "service": "FEED",
        "parameters": {"contract": "AUTO"},
    }


def feed_setup() -> dict[str, Any]:
    return {
        "type": "FEED_SETUP",
        "channel": FEED_CHANNEL,
        "acceptAggregationPeriod": 0,
        "acceptDataFormat": "COMPACT",
        "acceptEventFields": {k: list(v) for k, v in FIELDS.items()},
    }


def sub_entry(key: Key, from_time: int | None = None) -> dict[str, Any]:
    etype, symbol = canonical_key(key)  # dxFeed's form, whatever the caller passed
    entry: dict[str, Any] = {"type": etype, "symbol": symbol}
    if from_time is not None:
        entry["fromTime"] = from_time
    return entry


def subscription_msgs(
    add: Sequence[dict[str, Any]],
    remove: Sequence[dict[str, Any]] = (),
    *,
    reset: bool = False,
) -> list[dict[str, Any]]:
    msgs: list[dict[str, Any]] = []
    for i in range(0, len(add), SUB_CHUNK):
        msg: dict[str, Any] = {
            "type": "FEED_SUBSCRIPTION",
            "channel": FEED_CHANNEL,
            "add": list(add[i : i + SUB_CHUNK]),
        }
        if reset and i == 0:
            msg["reset"] = True
        msgs.append(msg)
    if reset and not add:
        msgs.append({"type": "FEED_SUBSCRIPTION", "channel": FEED_CHANNEL, "reset": True})
    for i in range(0, len(remove), SUB_CHUNK):
        msgs.append(
            {
                "type": "FEED_SUBSCRIPTION",
                "channel": FEED_CHANNEL,
                "remove": list(remove[i : i + SUB_CHUNK]),
            }
        )
    return msgs


def decode_feed_data(data: Sequence[Any], fields: Mapping[str, Sequence[str]]) -> list[Event]:
    """Decode COMPACT FEED_DATA into event dicts (hot path — keep it lean)."""
    if len(data) % 2:
        raise ProtocolError("odd COMPACT data length")
    out: list[Event] = []
    for i in range(0, len(data), 2):
        etype, values = data[i], data[i + 1]
        names = fields.get(etype) if isinstance(etype, str) else None
        if names is None or not isinstance(values, list):
            raise ProtocolError(f"unexpected event type {etype!r}")
        n = len(names)
        if n == 0 or len(values) % n:
            raise ProtocolError(f"{etype}: {len(values)} values is not a multiple of {n}")
        for j in range(0, len(values), n):
            ev: Event = {"type": etype}
            for name, v in zip(names, values[j : j + n], strict=True):
                if name == "eventSymbol":
                    ev["symbol"] = v
                elif isinstance(v, str) and v in _NONFINITE:
                    ev[name] = None
                else:
                    ev[name] = v
            out.append(ev)
    return out
