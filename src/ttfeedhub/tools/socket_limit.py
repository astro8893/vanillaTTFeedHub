"""Measure how many subscriptions one DXLink socket sustains (spec §10).

Opens ONE extra read-only socket. Run it during market hours, inside the hub
container so it uses the hub's secrets. It first asks the running hub how many
sessions it holds, refuses unless the hub holds at most 4, and needs --yes:

    docker compose exec -T ttfeedhub python -m ttfeedhub.tools.socket_limit
        --hub-token-file - --yes < secrets/smoke_token      (one line)
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import aiohttp

from ..config import ConfigError, load_settings, read_secret
from ..logsafe import setup_logging
from ..tokens import TokenManager
from ..types import Event
from ..upstream import protocol
from ..upstream.connection import SessionBudget, UpstreamConnection, UpstreamError

DEFAULT_STEPS = (1000, 2000, 4000, 6000, 8000, 10000)
DEFAULT_HUB_URL = "http://127.0.0.1:8700"
MAX_HUB_SOCKETS = 4  # hub + probe must stay within the hub's own budget of 5
WARNING = (
    "WARNING: tastytrade's DXLink session limit is account-wide. This probe opens one\n"
    "extra session, and other apps on this account also hold sessions. If the\n"
    "account is at its limit, the probe or one of them is refused."
)


def refusal_reason(stats: dict[str, Any]) -> str | None:
    """Why the probe must not run, given the hub's /v1/stats; None if it may."""
    sockets = stats.get("sockets")
    in_use = sockets.get("in_use") if isinstance(sockets, dict) else None
    if not isinstance(in_use, int) or isinstance(in_use, bool):
        return "the hub's /v1/stats has no sockets.in_use"
    if in_use > MAX_HUB_SOCKETS:
        return f"the hub holds {in_use} sessions (the probe needs in_use <= {MAX_HUB_SOCKETS})"
    return None


async def fetch_hub_stats(hub_url: str, token: str) -> dict[str, Any]:
    async with (
        aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as http,
        http.get(
            hub_url.rstrip("/") + "/v1/stats", headers={"Authorization": f"Bearer {token}"}
        ) as r,
    ):
        r.raise_for_status()
        body = await r.json(content_type=None)
    return body if isinstance(body, dict) else {}


def _read_token(path: str) -> str:
    raw = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
    return raw.strip()


def chain_symbols(body: dict[str, Any]) -> list[str]:
    seen: dict[str, None] = {}
    for item in (body.get("data") or {}).get("items") or []:
        for exp in item.get("expirations") or []:
            for strike in exp.get("strikes") or []:
                for side in ("call-streamer-symbol", "put-streamer-symbol"):
                    sym = strike.get(side)
                    if isinstance(sym, str) and sym:
                        seen[sym] = None
    return list(seen)


def recommend(results: list[dict[str, Any]]) -> int | None:
    alive = [r["subscribed"] for r in results if r["alive"]]
    return int(max(alive) * 0.8) if alive else None


async def measure(
    conn: UpstreamConnection, symbols: list[str], steps: list[int], dwell_s: float, seen: set[str]
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    done = 0
    for step in steps:
        n = min(step, len(symbols))
        if n > done:
            try:
                await conn.subscribe(
                    add=[protocol.sub_entry(("Quote", s)) for s in symbols[done:n]]
                )
            except UpstreamError:
                results.append({"subscribed": n, "symbols_with_data": 0, "alive": False})
                break
            done = n
        seen.clear()
        await asyncio.sleep(dwell_s)
        results.append({"subscribed": n, "symbols_with_data": len(seen), "alive": conn.live})
        print(f"{n:>7} subscribed {len(seen):>7} ticking  alive={conn.live}", flush=True)
        if not conn.live or n == len(symbols):
            break
    return results


async def _fetch_chain(
    http: aiohttp.ClientSession, tokens: TokenManager, api_base: str, underlying: str
) -> list[str]:
    bearer = await tokens.access_token()
    async with http.get(
        f"{api_base}/option-chains/{underlying}/nested",
        headers={"Authorization": f"Bearer {bearer}"},
    ) as r:
        r.raise_for_status()
        return chain_symbols(await r.json(content_type=None))


async def _run(underlying: str, steps: list[int], dwell_s: float) -> int:
    settings = load_settings()
    secret = read_secret(settings.secrets_dir, "tt_client_secret")
    refresh = read_secret(settings.secrets_dir, "tt_refresh_token")
    seen: set[str] = set()

    def on_events(events: list[Event]) -> None:
        for ev in events:
            seen.add(ev["symbol"])

    async with aiohttp.ClientSession() as http:
        tokens = TokenManager(
            http, api_base=settings.api_base, client_secret=secret, refresh_token=refresh
        )
        symbols = await _fetch_chain(http, tokens, settings.api_base, underlying)
        if not symbols:
            print("error: empty option chain", file=sys.stderr)
            return 1
        print(f"{len(symbols)} option symbols in the {underlying} chain")
        conn = UpstreamConnection(
            name="probe",
            generation=1,
            tokens=tokens,
            http=http,
            budget=SessionBudget(1),
            on_events=on_events,
        )
        await conn.open()
        try:
            results = await measure(conn, symbols, steps, dwell_s, seen)
        finally:
            await conn.close()
    rec = recommend(results)
    if rec is None:
        print("the socket died at the first step; check the logs")
        return 1
    # If the last result has n == len(symbols) and is alive, the limit was not reached
    if results and results[-1]["subscribed"] == len(symbols) and results[-1]["alive"]:
        print(
            f"note: limit not reached at {len(symbols)} symbols (recommendation is a lower bound)"
        )
    print(f"recommendation: TTFH_STREAM_SYMBOLS_PER_SOCKET={rec}")
    return 0


def _parse_steps(s: str) -> list[int]:
    steps = [int(x) for x in s.split(",")]
    for step in steps:
        if step <= 0:
            raise argparse.ArgumentTypeError(f"step must be positive, got {step}")
    return sorted(steps)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--underlying", default="SPXW")
    p.add_argument("--steps", type=_parse_steps, default=list(DEFAULT_STEPS))
    p.add_argument("--dwell", type=float, default=15.0)
    p.add_argument("--hub-url", default=DEFAULT_HUB_URL, help="the running hub (for /v1/stats)")
    p.add_argument(
        "--hub-token-file", help="a hub client token, used to read /v1/stats ('-' = stdin)"
    )
    p.add_argument("--yes", action="store_true", help="really open the extra session")
    a = p.parse_args(argv)
    setup_logging("WARNING")
    print(WARNING, file=sys.stderr)
    if not a.hub_token_file:
        print("refusing: --hub-token-file is required to check the hub first", file=sys.stderr)
        return 2
    try:
        token = _read_token(a.hub_token_file)
        stats = asyncio.run(fetch_hub_stats(a.hub_url, token))
    except (OSError, ValueError, aiohttp.ClientError, TimeoutError) as e:
        print(f"refusing: cannot read {a.hub_url}/v1/stats: {e}", file=sys.stderr)
        return 1
    reason = refusal_reason(stats)
    if reason is not None:
        print(f"refusing: {reason}", file=sys.stderr)
        return 1
    print(f"hub holds {stats['sockets']['in_use']} DXLink sessions", file=sys.stderr)
    if not a.yes:
        print("re-run with --yes to open one extra DXLink session", file=sys.stderr)
        return 2
    try:
        return asyncio.run(_run(a.underlying, a.steps, a.dwell))
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2
    except (aiohttp.ClientError, UpstreamError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
