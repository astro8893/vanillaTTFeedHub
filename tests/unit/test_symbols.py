import pytest

from ttfeedhub.symbols import canonical_candle, canonical_key

CASES = [
    ("SPX{=1m,tho=true}", "SPX{=m,tho=true}"),  # observed live
    ("SPX{=1d}", "SPX{=d}"),  # observed live
    ("SPX{=1h}", "SPX{=h}"),  # observed live
    ("SPX{=5m}", "SPX{=5m}"),  # observed live: unchanged
    ("SPX{=15m,tho=true}", "SPX{=15m,tho=true}"),
    ("SPX{=1s}", "SPX{=s}"),
    ("SPX{=1w}", "SPX{=w}"),
    ("SPX{=1mo}", "SPX{=mo}"),
    ("SPX{=2mo}", "SPX{=2mo}"),
    ("SPX{=1y}", "SPX{=y}"),
    ("SPX{=10m}", "SPX{=10m}"),
    ("SPX{=m}", "SPX{=m}"),
    ("/ESZ26:XCME{=1m}", "/ESZ26:XCME{=m}"),
    ("SPX{tho=true,=1m}", "SPX{=m,tho=true}"),  # attributes in key order
    ("SPX", "SPX"),  # no attributes
    ("SPX{=1m", "SPX{=1m"),  # malformed: left alone
    ("SPX{=1x}", "SPX{=1x}"),  # unknown unit: left alone
]


@pytest.mark.parametrize(("raw", "canon"), CASES)
def test_canonical_candle(raw: str, canon: str) -> None:
    assert canonical_candle(raw) == canon


@pytest.mark.parametrize(("raw", "canon"), CASES)
def test_canonical_candle_is_idempotent(raw: str, canon: str) -> None:
    assert canonical_candle(canonical_candle(raw)) == canon


def test_canonical_key_only_touches_candles() -> None:
    assert canonical_key(("Candle", "SPX{=1m}")) == ("Candle", "SPX{=m}")
    assert canonical_key(("Quote", "SPX{=1m}")) == ("Quote", "SPX{=1m}")
    k = ("Quote", "SPX")
    assert canonical_key(k) is k
