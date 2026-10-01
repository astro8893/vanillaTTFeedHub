import importlib.util
import io
import time
from pathlib import Path
from typing import Any

import pytest

from tests.fakes.fake_dxlink import FakeDxLink, quote_row
from tests.helpers import TOKENS


def _load_smoke() -> Any:
    path = Path(__file__).parents[2] / "scripts" / "smoke_live.py"
    spec = importlib.util.spec_from_file_location("smoke_live", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


async def test_smoke_reports_candle_failure_and_stream_candle(
    app_client: Any, fake_dx: FakeDxLink, capsys: pytest.CaptureFixture[str]
) -> None:
    fake_dx.auto_rows[("Quote", "SPX")] = quote_row("SPX", 1.0, 2.0)
    minute = int(time.time() * 1000) // 60_000 * 60_000
    fake_dx.candles["SPY{=1m}"] = [(minute, 1.0, 2.0, 0.5, 1.5, 10.0)]
    # no "SPY{=1d}" history: /v1/candles times out (504)
    url = str(app_client.make_url("")).rstrip("/")
    rc = await _load_smoke().run(url, TOKENS["ui"], 0.3, candle_wait_s=2.0)
    out = capsys.readouterr()
    assert rc != 0
    assert "candles" in out.err and "error" in out.err
    assert "stream Candle:SPY{=1m} event within 2s: yes" in out.out


def test_smoke_reads_token_from_stdin_or_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mod = _load_smoke()
    monkeypatch.setattr("sys.stdin", io.StringIO("tok-stdin\n"))
    assert mod.read_token("-") == "tok-stdin"
    f = tmp_path / "t"
    f.write_text("tok-file\n", encoding="utf-8")
    assert mod.read_token(str(f)) == "tok-file"
