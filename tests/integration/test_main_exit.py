import json
import logging
import socket
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import aiohttp
import pytest

import ttfeedhub.__main__ as entry
from ttfeedhub.config import Settings, hash_token


class _TradeScopeApi(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        body = json.dumps(
            {"access_token": "access-0123456789", "expires_in": 900, "scope": "read trade"}
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: object) -> None:
        pass


@pytest.fixture
def trade_scope_api() -> Iterator[str]:
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _TradeScopeApi)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    srv.server_close()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_scope_error_exit_closes_the_http_session(
    tmp_path: Path,
    trade_scope_api: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    (tmp_path / "tt_client_secret").write_text("client-secret-0123456789")
    (tmp_path / "tt_refresh_token").write_text("refresh-token-0123456789")
    clients = tmp_path / "clients.toml"
    clients.write_text(f'[clients.a]\ntoken_sha256 = "{hash_token("tok-a-0123456789")}"\n')
    settings = Settings(
        host="127.0.0.1",
        port=_free_port(),
        api_base=trade_scope_api,
        secrets_dir=tmp_path,
        clients_file=clients,
    )
    monkeypatch.setattr(entry, "load_settings", lambda: settings)
    monkeypatch.setattr(entry, "setup_logging", lambda level: None)
    made: list[aiohttp.ClientSession] = []
    real = aiohttp.ClientSession

    def recording(*a: Any, **kw: Any) -> aiohttp.ClientSession:
        made.append(real(*a, **kw))
        return made[-1]

    monkeypatch.setattr(aiohttp, "ClientSession", recording)
    with caplog.at_level(logging.ERROR):
        assert entry.main() == 3
    assert made and all(s.closed for s in made)
    assert any("refusing to start" in r.getMessage() for r in caplog.records)
