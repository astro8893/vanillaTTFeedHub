"""Per-client outbound queue.

- Keys in `all` mode: FIFO and bounded. On overflow the session is evicted as
  a slow consumer (close 4008), rather than buffering without limit.
- Keys in `latest` mode: the newest value per key. Conflation only drops
  values that were already superseded; the newest value is never delayed.
- Control frames (acks, errors, status) are sent before events.
- A subscribe-time snapshot joins the event stream (KI-003): it goes out as a
  `snap` frame after everything already queued and before any later event, so
  it can never overtake a live event for its key. A snapshot equal to the last
  value a key had when it was unsubscribed is not sent again.
"""

from __future__ import annotations

import asyncio
import itertools
from collections import deque
from collections.abc import Callable, Iterable
from typing import Any

from ..types import Event, Key

CTRL_LIMIT = 1000
# Last values remembered for unsubscribed keys, so a re-subscribe doesn't
# resend one the client already has. Oldest are forgotten first.
GONE_LIMIT = 20_000


class _Snap(dict[str, Any]):
    """A `snap` frame waiting in the event FIFO. Only ever at a position the
    writer reaches after every event queued before it."""


class ClientQueue:
    def __init__(self, limit_all: int, on_overflow: Callable[[], None] | None = None) -> None:
        self._limit = limit_all
        self.on_overflow = on_overflow
        self._all: deque[Event] = deque()  # events, and _Snap frames in their place
        self._snaps = 0  # _Snap frames in _all
        self._gone: dict[Key, Event] = {}
        self._latest: dict[Key, Event] = {}
        self._ctrl: deque[dict[str, Any]] = deque()
        self._modes: dict[Key, str] = {}
        self._ready = asyncio.Event()
        self.overflowed = False
        self.dropped_superseded = 0

    @property
    def depth(self) -> int:
        return len(self._all) + len(self._latest) + len(self._ctrl)

    def set_mode(self, keys: Iterable[Key], mode: str) -> None:
        for key in keys:
            if mode == "all" and self._modes.get(key) == "latest":
                # The pending conflated value goes out before any later event,
                # so the key's values never go backwards.
                pending = self._latest.pop(key, None)
                if pending is not None:
                    if len(self._all) >= self._limit:
                        self._overflow()
                    else:
                        self._all.append(pending)
            self._modes[key] = mode

    def clear_keys(self, keys: Iterable[Key], last: Iterable[Event] = ()) -> None:
        """Forget `keys`. `last` holds their latest values (the cache's): each is
        remembered unless it was a conflated value this drops undelivered."""
        latest = {(e["type"], e["symbol"]): e for e in last}
        gone = self._gone
        for key in keys:
            self._modes.pop(key, None)
            dropped = self._latest.pop(key, None)
            value = latest.get(key)
            gone.pop(key, None)
            if value is not None and dropped is None:
                gone[key] = value
        while len(gone) > GONE_LIMIT:
            del gone[next(iter(gone))]

    def push_snapshot(self, keys: Iterable[Key], evs: Iterable[Event]) -> None:
        """Queue the cached values of newly subscribed `keys` behind everything
        already queued. Call it in the same tick as attaching the keys, so every
        later event for them follows it. Skipped per key: a value the client had
        when it unsubscribed, and a latest-mode key with a newer value pending."""
        had = {key: self._gone.pop(key, None) for key in keys}
        out = []
        for ev in evs:
            key = (ev["type"], ev["symbol"])
            if had.get(key) == ev or key in self._latest:
                continue
            out.append(ev)
        if not out:
            return
        if len(self._all) >= self._limit:
            self._overflow()
            return
        self._all.append(_Snap(t="snap", d=out))
        self._snaps += 1
        self._ready.set()

    def push(self, ev: Event) -> None:
        key = (ev["type"], ev["symbol"])
        if self._modes.get(key) == "latest":
            if key in self._latest:
                self.dropped_superseded += 1
            self._latest[key] = ev
        elif len(self._all) >= self._limit:
            self._overflow()
        else:
            self._all.append(ev)
        self._ready.set()

    def _overflow(self) -> None:
        """Mark overflowed and tell the owner once, so it can evict at once."""
        first = not self.overflowed
        self.overflowed = True
        if first and self.on_overflow is not None:
            self.on_overflow()

    def push_control(self, msg: dict[str, Any]) -> None:
        if len(self._ctrl) >= CTRL_LIMIT:
            self._overflow()
        else:
            self._ctrl.append(msg)
        self._ready.set()

    async def next_frames(self, max_events: int = 500) -> tuple[list[dict[str, Any]], list[Event]]:
        """Wait until anything is queued, then take everything available now
        (events capped at `max_events`)."""
        await self._ready.wait()
        ctrl = list(self._ctrl)
        self._ctrl.clear()
        batch: list[Event] = []
        pending = self._all
        if self._snaps:
            self._take_ordered(ctrl, batch, max_events)
        else:
            for _ in range(min(len(pending), max_events)):
                batch.append(pending.popleft())
        room = max_events - len(batch)
        # Latest values wait while a snapshot is queued: one for their key may be in it.
        if room > 0 and self._latest and not self._snaps:
            if len(self._latest) <= room:
                batch.extend(self._latest.values())
                self._latest.clear()
            else:
                for key in list(itertools.islice(self._latest, room)):
                    batch.append(self._latest.pop(key))
        if not self._all and not self._latest and not self._ctrl and not self.overflowed:
            self._ready.clear()
        return ctrl, batch

    def _take_ordered(
        self, ctrl: list[dict[str, Any]], batch: list[Event], max_events: int
    ) -> None:
        """The slow path while a snapshot is queued. A snapshot at the head goes
        out last among the control frames (so before `batch`); the batch stops
        at the next snapshot, which waits for the next call."""
        pending = self._all
        if pending and type(pending[0]) is _Snap:
            ctrl.append(dict(pending.popleft()))
            self._snaps -= 1
        while pending and len(batch) < max_events and type(pending[0]) is not _Snap:
            batch.append(pending.popleft())
