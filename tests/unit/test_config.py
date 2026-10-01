import importlib.util
from pathlib import Path

import pytest

from ttfeedhub.config import (
    ConfigError,
    hash_token,
    load_clients,
    load_settings,
    read_secret,
)
from ttfeedhub.logsafe import redact


def test_defaults() -> None:
    s = load_settings({})
    assert s.port == 8700
    assert s.session_budget == 5
    assert s.api_base == "https://api.tastytrade.com"
    assert s.host == "127.0.0.1"  # loopback unless TTFH_HOST says otherwise
    assert load_settings({"TTFH_HOST": "0.0.0.0"}).host == "0.0.0.0"  # the container


def test_env_overrides() -> None:
    s = load_settings(
        {"TTFH_PORT": "9000", "TTFH_STREAM_SYMBOLS_PER_SOCKET": "1200", "TTFH_LINGER_S": "2.5"}
    )
    assert (s.port, s.stream_symbols_per_socket, s.linger_s) == (9000, 1200, 2.5)


def test_rejects_bad_values() -> None:
    with pytest.raises(ConfigError):
        load_settings({"TTFH_PORT": "abc"})
    with pytest.raises(ConfigError):
        load_settings({"TTFH_API_BASE": "http://api.tastytrade.com"})
    with pytest.raises(ConfigError):
        load_settings({"TTFH_STREAM_SYMBOLS_PER_SOCKET": "0"})


def test_read_secret_registers_for_redaction(tmp_path: Path) -> None:
    (tmp_path / "tt_refresh_token").write_text("  refresh-secret-777 \n")
    assert read_secret(tmp_path, "tt_refresh_token") == "refresh-secret-777"
    assert "refresh-secret-777" not in redact("got refresh-secret-777")


def test_read_secret_missing_or_empty(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="missing"):
        read_secret(tmp_path, "nope")
    (tmp_path / "empty").write_text("\n")
    with pytest.raises(ConfigError, match="empty"):
        read_secret(tmp_path, "empty")


def test_load_clients_keyed_by_hash(tmp_path: Path) -> None:
    h = hash_token("tok-abc")
    p = tmp_path / "clients.toml"
    p.write_text(
        f'[clients.ui]\ntoken_sha256 = "{h}"\nmax_keys = 500\nevent_types = ["Quote", "Greeks"]\n'
    )
    spec = load_clients(p)[h]
    assert spec.name == "ui"
    assert spec.max_keys == 500
    assert spec.max_connections == 2
    assert spec.event_types == frozenset({"Quote", "Greeks"})
    assert spec.snapshots_per_min == 60


HEX = "a" * 64


@pytest.mark.parametrize(
    ("body", "match"),
    [
        (f'[clients.X]\ntoken_sha256="{HEX}"', "name"),
        (f'[clients."a\\n"]\ntoken_sha256="{HEX}"', "name"),  # trailing newline
        (f'[clients.a]\ntoken_sha256="{HEX}\\n"', "64 hex"),
        ('[clients.a]\ntoken_sha256="short"', "64 hex"),
        (f'[clients.a]\ntoken_sha256="{HEX}"\nevent_types=["Order"]', "unknown event"),
        (f'[clients.a]\ntoken_sha256="{HEX}"\nmax_keys=0', "max_keys"),
        (f'[clients.a]\ntoken_sha256="{HEX}"\nsnapshots_per_min=0', "snapshots_per_min"),
        (f'[clients.a]\ntoken_sha256="{HEX}"\nsnapshots_per_min=6001', "snapshots_per_min"),
        (f'[clients.a]\ntoken_sha256="{HEX}"\n[clients.b]\ntoken_sha256="{HEX}"', "duplicate"),
        ("", "no clients"),
        ("not = [valid", "clients file"),
    ],
)
def test_load_clients_validation(tmp_path: Path, body: str, match: str) -> None:
    p = tmp_path / "c.toml"
    p.write_text(body)
    with pytest.raises(ConfigError, match=match):
        load_clients(p)


def _load_script():  # type: ignore[no-untyped-def]
    path = Path(__file__).parents[2] / "scripts" / "new_client.py"
    spec = importlib.util.spec_from_file_location("new_client", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_new_client_script(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    mod = _load_script()
    f = tmp_path / "clients.toml"
    assert mod.main(["ui", "--file", str(f), "--max-keys", "300", "--types", "Quote,Candle"]) == 0
    token = capsys.readouterr().out.strip()
    spec = load_clients(f)[hash_token(token)]
    assert spec.name == "ui"
    assert spec.max_keys == 300
    assert spec.event_types == {"Quote", "Candle"}
    assert token not in f.read_text()
    assert mod.main(["ui", "--file", str(f)]) == 1
    assert mod.main(["Bad Name", "--file", str(f)]) == 1


def test_new_client_rejects_invalid_max_keys(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    mod = _load_script()
    f = tmp_path / "clients.toml"
    ret = mod.main(["x", "--file", str(f), "--max-keys", "0"])
    assert ret == 1
    assert not f.exists()
    out = capsys.readouterr()
    assert out.out.strip() == ""


def test_new_client_rejects_empty_types(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    mod = _load_script()
    f = tmp_path / "clients.toml"
    ret = mod.main(["x", "--file", str(f), "--types", ""])
    assert ret == 1
    assert not f.exists()
    out = capsys.readouterr()
    assert out.out.strip() == ""


def test_load_clients_rejects_invalid_event_types(tmp_path: Path) -> None:
    p = tmp_path / "c.toml"
    h = "a" * 64
    p.write_text(f'[clients.a]\ntoken_sha256="{h}"\nevent_types = []')
    with pytest.raises(ConfigError, match="non-empty"):
        load_clients(p)
    p.write_text(f'[clients.a]\ntoken_sha256="{h}"\nevent_types = 5')
    with pytest.raises(ConfigError):
        load_clients(p)


def test_new_client_snapshots_per_min(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    mod = _load_script()
    f = tmp_path / "clients.toml"
    assert mod.main(["bot", "--file", str(f), "--snapshots-per-min", "120"]) == 0
    token = capsys.readouterr().out.strip()
    assert load_clients(f)[hash_token(token)].snapshots_per_min == 120
    assert mod.main(["bot2", "--file", str(f), "--snapshots-per-min", "0"]) == 1


def test_new_client_rejects_trailing_newline_name(tmp_path: Path) -> None:
    mod = _load_script()
    f = tmp_path / "clients.toml"
    assert mod.main(["ui\n", "--file", str(f)]) == 1
    assert not f.exists()
