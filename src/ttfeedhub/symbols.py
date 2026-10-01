"""dxFeed candle symbol canonicalization.

dxFeed labels Candle events with a normalized symbol: a period multiplier of 1
is dropped (`SPX{=1m,tho=true}` arrives as `SPX{=m,tho=true}`) and attributes
are kept in key order. The hub subscribes upstream, caches and matches events
by the canonical form, and hands every client the symbol it asked for.
"""

from __future__ import annotations

import functools
import re

from .types import Key

# The period attribute value: an optional multiplier and a unit (CandleType short names).
_PERIOD_RE = re.compile(r"(\d+(?:\.\d+)?)?(mo|[tsmhdwy])")


@functools.lru_cache(maxsize=8192)
def canonical_candle(symbol: str) -> str:
    """Canonical form of a candle symbol; anything unparseable is returned as is.
    Idempotent."""
    if not symbol.endswith("}"):
        return symbol
    brace = symbol.find("{")
    if brace <= 0:
        return symbol
    attrs: dict[str, str] = {}
    for part in symbol[brace + 1 : -1].split(","):
        k, sep, v = part.partition("=")
        if not sep or k in attrs:
            return symbol
        attrs[k] = v
    period = attrs.get("")
    if period is not None:
        m = _PERIOD_RE.fullmatch(period)
        if m is None:
            return symbol
        n, unit = m.groups()
        if n is not None and float(n) == 1:
            attrs[""] = unit
    return symbol[:brace] + "{" + ",".join(f"{k}={attrs[k]}" for k in sorted(attrs)) + "}"


def canonical_key(key: Key) -> Key:
    """The key the hub subscribes, caches and routes by: only Candle keys change."""
    if key[0] != "Candle":
        return key
    symbol = canonical_candle(key[1])
    return key if symbol == key[1] else (key[0], symbol)
