# vanillaTTFeedHub

A self-hosted market-data hub for **one** tastytrade account: it is the single
owner of that account's DXLink market-data sockets, and fans the data out to
your own local apps.

Your local apps (dashboards, recorders, trading bots) connect to the hub
instead of opening their own DXLink sockets. That keeps the account well under
tastytrade's session limit. The hub uses at most 5 sockets, and normally 1.

- **Real time:** events are pushed the instant they arrive, with no polling or
  batching timers. The latency the hub adds is measured against a no-hub
  baseline (`bash scripts/bench.sh`, median of 3 pairs). Absolute p99 on a
  shared machine is dominated by host jitter, so trust the hub-added figure.
- **Slow clients are evicted:** a client that cannot keep up is closed with
  code 4008 instead of making the hub buffer without limit. Clients reconnect
  and resubscribe automatically.
- **Read-only:** it has market data only, with no order or account endpoints.
  It refuses to start with a trade-scoped grant.
- **Private:** it is reachable only on the internal Docker network `ttfeed`,
  as `http://ttfeedhub:8700`. compose publishes no host port, so
  `127.0.0.1:8700` and `host.docker.internal` do not reach it; operators use
  `docker compose exec` (see Operations). Outside Docker the hub binds
  `127.0.0.1` by default; the image sets `TTFH_HOST=0.0.0.0` because a
  container must listen on all interfaces.

Known issues, lessons learned and open items: [docs/KNOWN_ISSUES.md](docs/KNOWN_ISSUES.md).

## Who this is for: one hub per account

This project is for someone who has their own tastytrade account and runs
several of their own programs against its market data on one machine.

- **Run your own hub, with your own tastytrade OAuth grant.** Each person runs
  a separate hub for their own account. The hub authenticates as you, and the
  data it receives is licensed to you.
- **Do not share a hub, or one account's market data, with other people.**
  tastytrade and dxFeed market data comes with redistribution terms; serving
  your account's feed to someone else's apps, or exposing the hub on a network
  other people can reach, can breach them. Keep the hub on the internal
  `ttfeed` Docker network, for your own apps only.
- Not affiliated with or endorsed by tastytrade or dxFeed. Use at your own risk;
  nothing here is trading advice.

## Setup

1. **Create a read-only tastytrade OAuth client for the hub.**
   - On my.tastytrade.com, go to Manage → My Profile → API → OAuth Applications → New.
   - Give it **read** scope only. Copy the client secret, then use Create Grant to get a refresh token.
2. **Write the secrets.** These are files and never env vars; `secrets/` is gitignored.
   Each file holds the **bare value only**: no `NAME=` label (not
   `TT_SECRET=abc…`, just `abc…`), no quotes, no trailing newline. A file copied
   from a `.env` with its label still in it makes tastytrade reject the grant
   (`token refresh rejected: HTTP 400`) every few minutes; see KI-004.
   ```bash
   mkdir -p secrets && chmod 700 secrets
   (umask 077
    printf '%s' '<client secret>' > secrets/tt_client_secret
    printf '%s' '<refresh token>' > secrets/tt_refresh_token)
   chmod 600 secrets/tt_client_secret secrets/tt_refresh_token
   grep -l '^[A-Za-z_]*=' secrets/tt_* && echo "remove the NAME= label from the file(s) above"
   ```
   On a Linux host, compose mounts these files with their host owner and mode, so
   also `sudo chown 10001 secrets/tt_*` (the container's user) or the hub can't read them.
3. **Create a token for each of your apps.** Each token is printed once; store it in that app's config.
   `new_client.py` runs on the host with Python 3.12+ and the standard library only (no venv needed).
   ```bash
   python scripts/new_client.py my-dashboard --file clients.toml
   (umask 077; python scripts/new_client.py smoke --file clients.toml > secrets/smoke_token)
   ```
   Per-client limits: `--max-keys`, `--max-connections`, `--types`,
   `--candles-per-min` and `--snapshots-per-min` (default 60).
   `clients.toml.example` shows the resulting format.
4. **Create the network once:** `docker network create --internal ttfeed`
   - `ttfeed` is internal: no egress, clients only; the hub reaches tastytrade
     via its `default` network.
   - The hub's compose and every app's compose declare it `external`, so no
     project's `docker compose down` removes it, and the hub and your apps can
     start in any order (an app only needs the network to exist).
5. **Start the hub:** `docker compose up -d --build`
   - Use `docker compose restart`, `stop`/`start` or `down`/`up` for the hub;
     none of them touches `ttfeed`.
   - `docker compose stop` takes a second or two: clients are closed with code 1001
     (going away) and reconnect by themselves when the hub is back.
6. **Verify it:**
   - `docker inspect --format '{{.State.Health.Status}}' ttfeedhub` should print
     `healthy` (the image runs a stdlib healthcheck against `/health` every 10s).
     To see the body:
     ```bash
     docker compose exec -T ttfeedhub python -c "import urllib.request as u; print(u.urlopen('http://127.0.0.1:8700/health').read().decode())"
     ```
     It should print `{"ok":true,"state":"idle"}`.
   - Smoke test, from a throwaway container on `ttfeed` (`scripts/` and `client/`
     are not in the image; the token goes in on stdin, never argv or env).
     `MSYS_NO_PATHCONV=1` stops Git Bash on Windows from rewriting the container
     paths; it is harmless elsewhere:
     ```bash
     MSYS_NO_PATHCONV=1 docker run --rm -i --network ttfeed \
         -v "$PWD/scripts:/w/scripts:ro" -v "$PWD/client:/w/client:ro" \
         ttfeedhub:local python /w/scripts/smoke_live.py \
         --url http://ttfeedhub:8700 --token-file - < secrets/smoke_token
     ```

## Installing the client

The Python client (`ttfeedhub_client`) lives in `client/`. Its only
dependencies are aiohttp and orjson. Either:

- **pip install it from this repository** (once it is published; replace
  `astro8893` with the account that hosts it, and pin a tag or commit):
  ```bash
  pip install "ttfeedhub-client @ git+https://github.com/astro8893/vanillaTTFeedHub@<tag-or-commit>#subdirectory=client"
  ```
- **or vendor it:** copy the `client/ttfeedhub_client/` directory into your app.

## Connecting your app

In your app's compose file:
```yaml
services:
  app:
    networks: [default, ttfeed]
    environment:
      FEED_HUB_URL: http://ttfeedhub:8700
      FEED_HUB_TOKEN_FILE: /run/secrets/feed_hub_token
    secrets: [feed_hub_token]
networks:
  ttfeed:
    external: true
    name: ttfeed
secrets:
  feed_hub_token:
    file: ./secrets/feed_hub_token   # the token new_client.py printed, bare value only
```
`ttfeed` is the operator-created network (Setup step 4). The app must use
`http://ttfeedhub:8700` on `ttfeed`; never loopback or `host.docker.internal`
(the hub publishes no host port). Keep the token in a file, not in an env var
or on the command line.

```python
import os
from pathlib import Path

from ttfeedhub_client import FeedClient

token = Path(os.environ["FEED_HUB_TOKEN_FILE"]).read_text().strip()
async with FeedClient(os.environ["FEED_HUB_URL"], token, name="my-dashboard") as feed:
    await feed.subscribe([("Quote", "SPY"), ("Greeks", ".SPY271217C500")], mode="latest")
    async for batch in feed.events():
        ...
```
- Use `mode="all"` for anything that must see every event (recorders, trading
  bots). You get every event, in order.
- Use `mode="latest"` for UIs. You get the newest value per symbol.
- Subscribe to candle streams (`("Candle", "SPX{=1m}")`) with `mode="all"`:
  a bar is updated many times while it is open, and `latest` would conflate
  those updates. The stream starts at the bar in progress; history comes from
  `feed.candles(...)` (`GET /v1/candles`).
- `feed.snapshot(keys)` returns the latest values for up to 2,000 keys via
  `POST /v1/snapshot` with the body `{"keys": ["Quote:SPX", ...]}`. (The
  `GET /v1/snapshot?key=...` form still works, but long key lists overflow the
  request line.) Keys with no cached value are fetched once; those count
  against the client's `max_keys`, and snapshot requests are rate limited per
  client (`snapshots_per_min`, HTTP 429).
- `feed.snapshot_detail(keys)` returns `(events, missing, rejected)`: `missing`
  keys had no data after the one-shot fetch, `rejected` keys were refused (type,
  symbol or `max_keys` headroom; a key refused for headroom is also missing).
- Trading code must check both `feed.state == "live"` and `feed.staleness(key)`
  before acting on data. `state` says the pipe is up; `staleness` says how old
  that particular key's last value is.
- A connection with no frames for `dead_after` seconds is dropped and
  re-established. Pass `dead_after=` to `FeedClient` to tune it.
- Call `start()` once per client; it cannot be called twice. Create a new
  `FeedClient` instead.

### Recommended: keep a direct fallback

The hub is one process; it restarts, upgrades and occasionally loses its
upstream. An app that matters should keep its own direct DXLink path (for
example via the tastytrade SDK) and fail over to it, rather than stopping when
the hub is unavailable. Rules that have worked well:

- **Fail over to direct** when the hub has not been `live` for **10 s**
  (`feed.state != "live"`), or when the data you depend on goes stale
  (`feed.staleness(key)` above your own threshold) even though the state says
  `live`. Watch data age per stream, not just connection state (KI-011).
- **Switch back to the hub** only after it has been `live` for **30 s** *and*
  is delivering fresh data for your keys. The delay stops flapping when the hub
  is restarting.
- **Keep your subscriptions registered on the hub while on direct.** The
  client keeps reconnecting and replays them, so the hub is warm when you
  switch back. Treat `on_gap(...)` as "events may have been missed" and
  resync (snapshot or history) after every switch.
- Count the direct sessions you open. Each one uses the account-wide DXLink
  session budget the hub exists to protect.

## Operations

| Task | Command |
|---|---|
| Logs | `docker compose logs -f ttfeedhub` |
| Health | `docker inspect --format '{{.State.Health.Status}}' ttfeedhub` |
| Stats (sockets, clients, rates) | See below; the token goes in on stdin |
| Rotate a client token | Delete its block in `clients.toml`, run `new_client.py` again, then `docker compose restart` |
| Measure the per-socket limit | See below; during market hours only |

The hub has no host port, so `/v1/stats` is read from inside the container:
```bash
docker compose exec -T ttfeedhub python -c "import sys, urllib.request as u; r = u.Request('http://127.0.0.1:8700/v1/stats', headers={'Authorization': 'Bearer ' + sys.stdin.read().strip()}); print(u.urlopen(r, timeout=5).read().decode())" < secrets/smoke_token
```

### Measuring the per-socket limit

`ttfeedhub.tools.socket_limit` opens **one extra DXLink session**. The session
limit is account-wide, and other apps on this account hold sessions too. So
the tool first reads the running hub's `/v1/stats` (`--hub-url`, default
`http://127.0.0.1:8700`, which is the hub itself when run inside its container;
`--hub-token-file`, where `-` reads stdin), refuses unless the hub holds at
most 4 sessions, and does nothing without `--yes`:
```bash
docker compose exec -T ttfeedhub python -m ttfeedhub.tools.socket_limit \
    --hub-token-file - --yes < secrets/smoke_token
```

## Development

```bash
python -m venv .venv && .venv/Scripts/python -m pip install -U pip && .venv/Scripts/python -m pip install -e ".[dev]"
bash scripts/check.sh    # ruff, mypy --strict, bandit, pip-audit, pytest
bash scripts/bench.sh    # hub-added latency benchmark in a Linux container
bash scripts/lock.sh     # regenerate hash-pinned requirements.lock
```
(On Linux/macOS the venv interpreter is `.venv/bin/python`; `check.sh` finds either.)
See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT; see [LICENSE](LICENSE).
