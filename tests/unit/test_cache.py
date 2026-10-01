import asyncio

from ttfeedhub.core.cache import LatestCache

K = ("Quote", "SPX")


def test_update_get_many_drop() -> None:
    c = LatestCache()
    ev = {"type": "Quote", "symbol": "SPX", "bidPrice": 1.0}
    c.update(K, ev)
    assert c.get(K) is ev
    assert c.get_many([K, ("Quote", "NONE")]) == [ev]
    assert len(c) == 1
    c.drop([K])
    assert c.get(K) is None


async def test_wait_for_existing_value_returns_immediately() -> None:
    c = LatestCache()
    ev = {"type": "Quote", "symbol": "SPX"}
    c.update(K, ev)
    assert await c.wait_for(K, 1) is ev


async def test_wait_for_wakes_on_update() -> None:
    c = LatestCache()
    t = asyncio.create_task(c.wait_for(K, 1))
    await asyncio.sleep(0)
    ev = {"type": "Quote", "symbol": "SPX"}
    c.update(K, ev)
    assert await t is ev


async def test_wait_for_times_out_and_cleans_up() -> None:
    c = LatestCache()
    assert await c.wait_for(K, 0.05) is None
    assert c.waiter_count() == 0


async def test_wait_for_a_candle_alias_wakes_on_the_canonical_update() -> None:
    c = LatestCache()
    t = asyncio.create_task(c.wait_for(("Candle", "SPX{=1d}"), 1))
    await asyncio.sleep(0)
    c.update(("Candle", "SPX{=d}"), {"type": "Candle", "symbol": "SPX{=d}", "close": 1.0})
    got = await t
    assert got is not None and got["symbol"] == "SPX{=1d}"
