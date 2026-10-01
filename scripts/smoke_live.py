"""Read-only smoke test against a running hub.

The hub has no host port, so run it from a throwaway container on "ttfeed"
(the token is read from stdin with --token-file -); see README "Verify it".
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import statistics
import sys
import time
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "client"))

from ttfeedhub_client import FeedClient, FeedError

STREAM_CANDLE = ("Candle", "SPY{=1m}")


async def stream_candle(fc: FeedClient, wait_s: float) -> bool:
    """Subscribe a streaming 1m candle; True if an event for it arrives within wait_s."""
    rejected = await fc.subscribe([STREAM_CANDLE], mode="all")
    if rejected:
        print("stream candle rejected:", rejected, file=sys.stderr)
        return False
    try:
        async with asyncio.timeout(wait_s):
            async for batch in fc.events():
                if any((e["type"], e["symbol"]) == STREAM_CANDLE for e in batch):
                    return True
    except TimeoutError:
        pass
    finally:
        await fc.unsubscribe([STREAM_CANDLE])
    return False


async def run(url: str, token: str, seconds: float, *, candle_wait_s: float = 15.0) -> int:
    keys = [("Quote", "SPX"), ("Trade", "SPX"), ("Quote", "SPY")]
    lat: list[float] = []
    counts: dict[tuple[str, str], int] = {}
    rc = 0
    async with FeedClient(url, token, name="smoke") as fc:
        rejected = await fc.subscribe(keys)
        if rejected:
            print("rejected:", rejected)
        end = time.time() + seconds

        async def consume() -> None:
            async for batch in fc.events():
                now = time.time()
                for ev in batch:
                    k = (ev["type"], ev["symbol"])
                    counts[k] = counts.get(k, 0) + 1
                    lat.append(now - ev["rt"])

        task = asyncio.create_task(consume())
        await asyncio.sleep(max(0.0, end - time.time()))
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        print(f"state={fc.state} hub={fc.hub_state} generation={fc.generation}")
        for k in keys:
            last = fc.last(k)
            print(
                f"{k[0]:>6} {k[1]:<6} events={counts.get(k, 0):>6} fields={sorted(last) if last else None}"
            )
        if not counts:
            print("error: no stream events received", file=sys.stderr)
            rc = 1
        if len(lat) >= 2:
            q = statistics.quantiles(lat, n=100)
            print(f"hub->client latency p50={q[49] * 1e3:.2f}ms p99={q[98] * 1e3:.2f}ms")
        try:
            bars = await fc.candles("SPY", "1d", from_ms=int((time.time() - 30 * 86400) * 1000))
            print(f"SPY daily bars (30d): {len(bars)}")
        except (FeedError, aiohttp.ClientError, TimeoutError) as e:
            print(f"error: candles request failed: {e}", file=sys.stderr)
            rc = 1
        got = await stream_candle(fc, candle_wait_s)
        print(
            f"stream {STREAM_CANDLE[0]}:{STREAM_CANDLE[1]} event within {candle_wait_s:g}s: "
            f"{'yes' if got else 'no'}"
        )
    return rc


def read_token(path: str) -> str:
    """The token from a file, or from stdin when path is "-" (never argv/env)."""
    raw = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
    return raw.strip()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", default="http://ttfeedhub:8700")
    p.add_argument("--token-file", required=True, help="token file, or - for stdin")
    p.add_argument("--seconds", type=float, default=15.0)
    a = p.parse_args()
    return asyncio.run(run(a.url, read_token(a.token_file), a.seconds))


if __name__ == "__main__":
    raise SystemExit(main())
