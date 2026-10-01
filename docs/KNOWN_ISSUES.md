# Known issues and lessons learned

Things that have gone wrong running this hub against live tastytrade/dxFeed
data, what was done about them, and what is still open. Each entry has a stable
`KI-NNN` id; source comments refer to these ids. If you hit one of the open
items, or something new, please open an issue with the log lines involved
(never include tokens or secrets).

| ID | Title | Status |
|---|---|---|
| KI-001 | dxFeed normalizes candle symbols (`{=1m}` → `{=m}`) | Fixed |
| KI-002 | DXLink quote tokens expire daily (around 00:00 UTC) | Fixed |
| KI-003 | Subscribe snapshots could be duplicated or overtake live events | Fixed |
| KI-004 | Secret files must hold bare values, not `NAME=value` | Documented; prevention open |
| KI-005 | Hub reachable on a host port | Fixed |
| KI-006 | Keys rejected during the client's reconnect replay are not exposed | Open |
| KI-007 | Canonical candle form keeps dxFeed default attributes | Open |
| KI-008 | One-shot snapshot wait is fixed at 3 s | Open |
| KI-009 | A rejected client token is logged over and over | Open |
| KI-010 | Fresh quote tokens that are also rejected cause a fast fetch loop | Open |
| KI-011 | Monitor data age per stream, not just connection state | Lesson / open |

---

## Fixed

### KI-001: dxFeed normalizes candle symbols
- **Symptom:** every `GET /v1/candles` returned 504 "history request timed
  out", and live 1-minute candles stopped advancing while quotes kept updating.
- **Cause:** dxFeed returns Candle events under a normalized symbol: a period
  multiplier of 1 is dropped. `SPX{=1m,tho=true}` comes back as
  `SPX{=m,tho=true}`, `{=1d}` as `{=d}` and `{=1h}` as `{=h}`. Matching events by
  the exact requested symbol discarded all of them.
- **Fix:** the hub subscribes upstream with the canonical dxFeed form and
  delivers events under each client's requested symbol. The test fake
  normalizes symbols like real dxFeed does.
- **Lesson:** a fake that echoes symbols verbatim hid this; fakes should copy
  the quirks of the real service.

### KI-002: DXLink quote tokens expire daily
- **Symptom:** shortly after 00:00 UTC, DXLink sends an ERROR such as *Your
  authentication token has expired, reauthentication is required*. A hub that
  kept the cached token then logged `DXLink error: Authentication failed` on
  every reconnect, for hours, until restarted.
- **Fix:** an ERROR or UNAUTHORIZED about authentication or token expiry now
  clears the cached quote token, so the next connect fetches a fresh one
  (the session-limit error is excluded). History requests retry once with a new
  token. Expected recovery is about 1–3 s.
- **What to look for:** one `quote token refreshed after DXLink auth error`
  line per day in the hub log, and no run of `Authentication failed`.

### KI-003: Subscribe snapshots delivered in order, never duplicated
- **Symptom (before the fix):** the cached value sent on subscribe travelled as
  a control frame that could overtake queued live events, and a re-subscribe
  replayed cached TimeAndSale/Trade events again. Consumers counting prints
  could double-count or, after deduplication, drop trades.
- **Fix:** subscribe snapshots now travel in the ordered event stream; only
  keys new to the connection get a snapshot (a re-subscribe sends none); a new
  connection gets one cached value per key, then live events.
- **Consumers rely on this:** an app that dedupes trades on
  `(hub_rt, index, sequence)` sees a snapshot replay as the same event as the
  original, so it is dropped correctly and nothing is counted twice.

### KI-004: Secret files must hold bare values
- **Symptom:** hub log `tastytrade rejected the grant: token refresh rejected:
  HTTP 400`, repeating every few minutes; the hub never goes live.
- **Cause:** `secrets/tt_client_secret` and `secrets/tt_refresh_token` contained
  `TT_SECRET=<value>`-style lines copied from a `.env` file.
- **Fix:** put only the bare value in each file (see README, Setup step 2).
  Check with `grep -l '^[A-Za-z_]*=' secrets/tt_*`, which should print nothing.
- **Open:** the hub could reject (or strip) a `NAME=` prefix with an error
  naming the file.

### KI-005: Hub reachable on a host port
- **Cause:** early versions published `127.0.0.1:8700` on the host.
- **Fix:** compose publishes no host port. The hub is reachable only on the
  operator-created internal `ttfeed` Docker network; operators use
  `docker compose exec`. Check with `docker inspect ttfeedhub`: there should be
  no host port binding.

## Open

### KI-006: Keys rejected during the client's reconnect replay are not exposed
- After a hub restart that runs into `max_keys` or hub capacity limits,
  `FeedClient`'s reconnect replay can have keys rejected. The client drops them
  from its subscriptions and only logs a `ttfeedhub rejected` warning; your app
  is not told, so those keys can silently go unserved with no failover.
  Rejections returned by `subscribe()` itself are reported normally.
- **Workaround:** watch `staleness(key)` for every key you depend on, and treat
  a key with no data as a reason to fail over or resubscribe.
- **Fix plan:** an `on_reject(keys, reason)` callback (or a rejected-keys
  counter) on `FeedClient` that also covers replay.

### KI-007: Canonical candle form keeps dxFeed default attributes
- dxFeed strips default attributes (`tho=false`, `price=last`, `a=m`) from
  echoed candle symbols. A client subscribing e.g. `SPX{=1m,tho=false}` may get
  events as `SPX{=m}`; the hub's canonical forms would then differ and the
  events would be dropped. Latent: avoid spelling out default attributes.
- **Fix plan:** strip dxFeed default attributes when canonicalizing, or at
  least count unmatched Candle events in `/v1/stats`. `candle_period_ms` also
  misreads the `mo` and `y` periods.

### KI-008: One-shot snapshot wait is fixed at 3 s
- `one_shot_wait_s = 3.0` in `config.py`, with no env mapping and no
  per-request override. Snapshots of keys the hub has not cached (for example a
  fresh option chain) can come back with many keys in `missing`.
- **Workaround:** retry the `missing` keys once.
- **Fix plan:** a bounded per-request `wait_s`.

### KI-009: A rejected client token is logged over and over
- A consumer with a wrong or revoked token logs an HTTP 401 handshake error on
  every reconnect attempt, forever.
- **Fix plan:** rate-limit the message, or demote it after the first one.

### KI-010: Fresh quote tokens that are also rejected cause a fast fetch loop
- Follow-up to KI-002. If freshly fetched quote tokens are rejected too (an
  account problem rather than expiry), the stream fetches a new token on every
  reconnect, roughly 120 an hour.
- **Fix plan:** fall back to the slower `auth_retry_s` interval after about 3
  consecutive rejections of fresh tokens, and log the raw code and text of any
  error matched by code only, in case DXLink rewords its messages.

### KI-011: Monitor data age per stream, not just connection state
- A lesson from KI-001: a consumer's health check said `live` with no gaps
  while its candles had stopped, because liveness came from quotes on the same
  connection.
- **Recommendation:** track the age of the last event per stream you depend on
  (quotes, greeks, candles, …) via `staleness(key)`, alert or fail over when a
  subscribed stream goes silent during market hours, and report recent gap
  counts (e.g. last 15 min) rather than only cumulative totals.
