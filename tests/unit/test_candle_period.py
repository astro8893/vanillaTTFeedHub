import pytest

from ttfeedhub.upstream.stream_pool import candle_from_time, candle_period_ms

S, M, H, D, W = 1000, 60_000, 3_600_000, 86_400_000, 604_800_000


@pytest.mark.parametrize(
    ("symbol", "period"),
    [
        ("SPY{=1m}", M),
        ("SPX{=5m,tho=true}", 5 * M),
        ("SPX{tho=true,=15m}", 15 * M),
        ("/ES{=30s}", 30 * S),
        ("SPX{=1h}", H),
        ("SPX{=2d}", 2 * D),
        ("SPX{=d}", D),
        ("SPX{=w}", W),
        ("SPX{=1w}", W),
        ("SPX", D),  # no period: default 1 day
        ("SPX{=1mo}", D),  # unsupported unit
        ("SPX{=0m}", D),  # zero period
        ("SPX{=m", D),  # malformed
    ],
)
def test_candle_period_ms(symbol: str, period: int) -> None:
    assert candle_period_ms(symbol) == period


def test_candle_from_time_floors_to_the_current_bar_start() -> None:
    now = 1_727_600_123_456  # 2024-09-29T09:08:43.456Z
    assert candle_from_time("SPY{=1m}", now) == 1_727_600_100_000
    assert candle_from_time("SPY{=15m}", now) == 1_727_599_500_000
    assert candle_from_time("SPY{=1h}", now) == 1_727_600_123_456 // H * H
    assert candle_from_time("SPY{=d}", now) == 1_727_568_000_000  # UTC midnight
    assert candle_from_time("SPY{=1m}", 1_727_600_100_000) == 1_727_600_100_000  # exact edge
