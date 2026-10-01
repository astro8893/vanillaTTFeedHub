import asyncio
import contextlib
from typing import Any

import orjson

from ttfeedhub.core.client_queue import ClientQueue
from ttfeedhub.server.stream import CLOSE_SLOW_CONSUMER, write_loop


class StubWS:
    def __init__(self) -> None:
        self.sent: list[Any] = []
        self.close_code: int | None = None

    async def send_frame(self, data: bytes, opcode: Any) -> None:
        self.sent.append(orjson.loads(data))

    async def close(self, *, code: int = 1000, message: bytes = b"") -> bool:
        self.close_code = code
        return True


def ev(i: int) -> dict[str, Any]:
    return {"type": "Quote", "symbol": "SPX", "bidPrice": i}


async def test_sends_control_then_events_immediately() -> None:
    ws, q = StubWS(), ClientQueue(10)
    task = asyncio.create_task(write_loop(ws, q, lambda: None))  # type: ignore[arg-type]
    q.push_control({"t": "ack", "id": 1})
    q.push(ev(1))
    await asyncio.sleep(0.01)
    assert ws.sent == [{"t": "ack", "id": 1}, {"t": "ev", "d": [ev(1)]}]
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_evicts_slow_consumer() -> None:
    ws, q = StubWS(), ClientQueue(2)
    evicted = []
    for i in range(3):
        q.push(ev(i))
    await asyncio.wait_for(write_loop(ws, q, lambda: evicted.append(1)), 1)  # type: ignore[arg-type]
    assert ws.close_code == CLOSE_SLOW_CONSUMER and evicted == [1]


class HangingWS(StubWS):
    async def send_frame(self, data: bytes, opcode: Any) -> None:
        await asyncio.Event().wait()


async def test_send_timeout_evicts(monkeypatch: Any) -> None:
    from ttfeedhub.server import stream

    monkeypatch.setattr(stream, "SEND_TIMEOUT_S", 0.05)
    ws, q = HangingWS(), ClientQueue(10)
    evicted: list[int] = []
    q.push(ev(1))
    await asyncio.wait_for(write_loop(ws, q, lambda: evicted.append(1)), 1)  # type: ignore[arg-type]
    assert ws.close_code == CLOSE_SLOW_CONSUMER and evicted == [1]


async def test_control_overflow_notifies_once() -> None:
    from ttfeedhub.core.client_queue import CTRL_LIMIT

    calls: list[int] = []
    q = ClientQueue(10, on_overflow=lambda: calls.append(1))
    for i in range(CTRL_LIMIT + 5):
        q.push_control({"t": "status", "i": i})
    assert q.overflowed and calls == [1] and q.depth == CTRL_LIMIT
