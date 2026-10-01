import asyncio
from typing import Any

from ttfeedhub.core.client_queue import ClientQueue

K = ("Quote", "SPX")


def ev(sym: str = "SPX", **f: Any) -> dict[str, Any]:
    return {"type": "Quote", "symbol": sym, **f}


async def test_all_mode_is_fifo_and_complete() -> None:
    q = ClientQueue(100)
    for i in range(3):
        q.push(ev(bidPrice=i))
    ctrl, batch = await q.next_frames()
    assert ctrl == [] and [e["bidPrice"] for e in batch] == [0, 1, 2]


async def test_latest_mode_keeps_only_newest() -> None:
    q = ClientQueue(100)
    q.set_mode([K], "latest")
    for i in range(3):
        q.push(ev(bidPrice=i))
    _, batch = await q.next_frames()
    assert [e["bidPrice"] for e in batch] == [2]
    assert q.dropped_superseded == 2


async def test_control_frames_come_first() -> None:
    q = ClientQueue(100)
    q.push(ev())
    q.push_control({"t": "ack"})
    ctrl, batch = await q.next_frames()
    assert ctrl == [{"t": "ack"}] and len(batch) == 1


async def test_next_frames_waits_until_something_is_queued() -> None:
    q = ClientQueue(100)
    task = asyncio.create_task(q.next_frames())
    await asyncio.sleep(0.01)
    assert not task.done()
    q.push(ev())
    _, batch = await asyncio.wait_for(task, 1)
    assert len(batch) == 1


async def test_max_events_cap_leaves_remainder_ready() -> None:
    q = ClientQueue(1000)
    for i in range(7):
        q.push(ev(bidPrice=i))
    _, first = await q.next_frames(max_events=5)
    _, second = await asyncio.wait_for(q.next_frames(max_events=5), 1)
    assert len(first) == 5 and len(second) == 2
    assert q.depth == 0


async def test_overflow_flags_eviction() -> None:
    q = ClientQueue(2)
    for _ in range(3):
        q.push(ev())
    assert q.overflowed


async def test_clear_keys_drops_pending_latest() -> None:
    q = ClientQueue(100)
    q.set_mode([K], "latest")
    q.push(ev())
    q.clear_keys([K])
    assert q.depth == 0


async def test_switch_latest_to_all_never_goes_backwards() -> None:
    q = ClientQueue(100)
    q.set_mode([K], "latest")
    q.push(ev(bidPrice=1))
    q.set_mode([K], "all")
    q.push(ev(bidPrice=2))
    _, batch = await q.next_frames()
    assert [e["bidPrice"] for e in batch] == [1, 2]


async def frames(q: ClientQueue, max_events: int = 500) -> list[tuple[str, Any]]:
    """Drain the queue as the writer would: control frames, then the event batch."""
    out: list[tuple[str, Any]] = []
    while q.depth:
        ctrl, batch = await q.next_frames(max_events)
        out += [(m["t"], e["bidPrice"]) for m in ctrl if m["t"] == "snap" for e in m["d"]]
        out += [("ev", e["bidPrice"]) for e in batch]
    return out


async def test_snapshot_is_delivered_after_events_already_queued() -> None:
    q = ClientQueue(100)
    q.push(ev(bidPrice=1))
    q.push_snapshot([K], [ev(bidPrice=2)])
    q.push(ev(bidPrice=3))
    assert await frames(q) == [("ev", 1), ("snap", 2), ("ev", 3)]


async def test_snapshot_at_head_goes_out_with_the_events_after_it() -> None:
    q = ClientQueue(100)
    q.push_control({"t": "ack"})
    q.push_snapshot([K], [ev(bidPrice=1)])
    q.push(ev(bidPrice=2))
    ctrl, batch = await q.next_frames()
    assert [m["t"] for m in ctrl] == ["ack", "snap"] and batch == [ev(bidPrice=2)]
    assert q.depth == 0


async def test_latest_snapshot_is_dropped_when_a_newer_value_is_pending() -> None:
    q = ClientQueue(100)
    q.set_mode([K], "latest")
    q.push(ev(bidPrice=5))
    q.push_snapshot([K], [ev(bidPrice=4)])
    assert await frames(q) == [("ev", 5)]


async def test_latest_values_never_overtake_a_pending_snapshot() -> None:
    q = ClientQueue(100)
    q.push(ev("SPY", bidPrice=1))  # an all-mode key ahead of the snapshot
    q.set_mode([K], "latest")
    q.push_snapshot([K], [ev(bidPrice=2)])
    q.push(ev(bidPrice=3))
    assert await frames(q, max_events=1) == [("ev", 1), ("snap", 2), ("ev", 3)]


async def test_snapshot_equal_to_the_last_value_before_unsub_is_not_resent() -> None:
    q = ClientQueue(100)
    last = ev(bidPrice=1)
    q.push(last)
    q.clear_keys([K], [last])
    q.push_snapshot([K], [dict(last)])
    assert await frames(q) == [("ev", 1)]


async def test_newer_snapshot_after_unsub_is_sent_after_stale_events() -> None:
    q = ClientQueue(100)
    q.push(ev(bidPrice=1))
    q.clear_keys([K], [ev(bidPrice=1)])
    q.push_snapshot([K], [ev(bidPrice=2)])
    assert await frames(q) == [("ev", 1), ("snap", 2)]


async def test_latest_value_dropped_at_unsub_is_resent_on_resubscribe() -> None:
    q = ClientQueue(100)
    q.set_mode([K], "latest")
    q.push(ev(bidPrice=1))
    q.clear_keys([K], [ev(bidPrice=1)])  # pending value discarded, never delivered
    q.set_mode([K], "latest")
    q.push_snapshot([K], [ev(bidPrice=1)])
    assert await frames(q) == [("snap", 1)]
