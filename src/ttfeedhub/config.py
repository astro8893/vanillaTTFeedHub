"""Settings (env), secrets (files) and the client registry (TOML)."""

from __future__ import annotations

import hashlib
import os
import re
import tomllib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .logsafe import register_secret

EVENT_TYPES: frozenset[str] = frozenset(
    {"Quote", "Trade", "TimeAndSale", "Greeks", "Summary", "Candle"}
)
NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")


class ConfigError(Exception):
    """Invalid or missing configuration; the hub refuses to start."""


@dataclass(frozen=True, slots=True)
class Settings:
    # loopback by default; the image sets TTFH_HOST=0.0.0.0 (no host port: reachable only on ttfeed)
    host: str = "127.0.0.1"
    port: int = 8700
    api_base: str = "https://api.tastytrade.com"
    secrets_dir: Path = Path("/run/secrets")
    clients_file: Path = Path("/etc/ttfeedhub/clients.toml")
    log_level: str = "INFO"
    stream_symbols_per_socket: int = 5000
    max_stream_sockets: int = 3
    max_history_sockets: int = 2
    linger_s: float = 60.0
    max_clients: int = 16
    queue_limit_all: int = 100_000
    handshake_timeout_s: float = 30.0
    silence_timeout_s: float = 60.0
    keepalive_s: float = 30.0
    history_timeout_s: float = 20.0
    history_idle_close_s: float = 60.0
    candle_cache_bytes: int = 256 * 1024 * 1024
    one_shot_wait_s: float = 3.0
    reaper_interval_s: float = 5.0
    heartbeat_s: float = 2.0
    auth_retry_s: float = 300.0
    # Cyclic-GC generation-0 threshold while the hub runs (0 keeps Python's 700).
    # Event dicts are acyclic and freed by refcount, so frequent gen-0 passes over
    # them find nothing; they only cost CPU on the hot path and add tail latency.
    gc_threshold0: int = 50_000

    @property
    def session_budget(self) -> int:
        return self.max_stream_sockets + self.max_history_sockets


_ENV: dict[str, tuple[str, Callable[[str], Any]]] = {
    "TTFH_HOST": ("host", str),
    "TTFH_PORT": ("port", int),
    "TTFH_API_BASE": ("api_base", str),
    "TTFH_SECRETS_DIR": ("secrets_dir", Path),
    "TTFH_CLIENTS_FILE": ("clients_file", Path),
    "TTFH_LOG_LEVEL": ("log_level", str),
    "TTFH_STREAM_SYMBOLS_PER_SOCKET": ("stream_symbols_per_socket", int),
    "TTFH_LINGER_S": ("linger_s", float),
    "TTFH_GC_THRESHOLD0": ("gc_threshold0", int),
}


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    source = os.environ if env is None else env
    kwargs: dict[str, Any] = {}
    for var, (name, conv) in _ENV.items():
        if var in source:
            try:
                kwargs[name] = conv(source[var])
            except ValueError as e:
                raise ConfigError(f"{var}: {e}") from None
    s = Settings(**kwargs)
    if not s.api_base.startswith("https://"):
        raise ConfigError("TTFH_API_BASE must be an https:// URL")
    if not 1 <= s.stream_symbols_per_socket <= 100_000:
        raise ConfigError("TTFH_STREAM_SYMBOLS_PER_SOCKET must be 1..100000")
    if not 1 <= s.port <= 65535:
        raise ConfigError("TTFH_PORT must be 1..65535")
    if s.gc_threshold0 < 0:
        raise ConfigError("TTFH_GC_THRESHOLD0 must be >= 0")
    return s


def read_secret(secrets_dir: Path, name: str) -> str:
    path = secrets_dir / name
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        raise ConfigError(f"missing secret file: {path}") from None
    if not value:
        raise ConfigError(f"empty secret file: {path}")
    register_secret(value, pinned=True)
    return value


@dataclass(frozen=True, slots=True)
class ClientSpec:
    name: str
    token_sha256: str
    max_keys: int = 2000
    max_connections: int = 2
    event_types: frozenset[str] = EVENT_TYPES
    candles_per_min: int = 60
    snapshots_per_min: int = 60


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _bounded_int(raw: Mapping[str, Any], key: str, default: int, lo: int, hi: int) -> int:
    value = raw.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool) or not lo <= value <= hi:
        raise ConfigError(f"{key} must be an integer in {lo}..{hi}")
    return value


def load_clients(path: Path) -> dict[str, ClientSpec]:
    """Client registry keyed by token SHA-256. Tokens themselves are never stored."""
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ConfigError(f"missing clients file: {path}") from None
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"clients file {path}: {e}") from None
    out: dict[str, ClientSpec] = {}
    for name, raw in (data.get("clients") or {}).items():
        if not isinstance(name, str) or not NAME_RE.fullmatch(name):
            raise ConfigError(f"bad client name {name!r} (lowercase, [a-z0-9_-], max 32)")
        if not isinstance(raw, dict):
            raise ConfigError(f"client {name}: expected a table")
        digest = str(raw.get("token_sha256", "")).lower()
        if not _HEX64.fullmatch(digest):
            raise ConfigError(f"client {name}: token_sha256 must be 64 hex chars")
        event_types_raw = raw.get("event_types", sorted(EVENT_TYPES))
        if isinstance(event_types_raw, list):
            if not event_types_raw:
                raise ConfigError(f"client {name}: event_types must be non-empty")
            if not all(isinstance(t, str) for t in event_types_raw):
                raise ConfigError(f"client {name}: event_types must be a list of strings")
            types = frozenset(event_types_raw)
        else:
            raise ConfigError(f"client {name}: event_types must be a list of strings")
        if not types <= EVENT_TYPES:
            raise ConfigError(f"client {name}: unknown event types {sorted(types - EVENT_TYPES)}")
        if digest in out:
            raise ConfigError(f"client {name}: duplicate token")
        out[digest] = ClientSpec(
            name=name,
            token_sha256=digest,
            max_keys=_bounded_int(raw, "max_keys", 2000, 1, 50_000),
            max_connections=_bounded_int(raw, "max_connections", 2, 1, 16),
            event_types=types,
            candles_per_min=_bounded_int(raw, "candles_per_min", 60, 1, 6000),
            snapshots_per_min=_bounded_int(raw, "snapshots_per_min", 60, 1, 6000),
        )
    if not out:
        raise ConfigError(f"clients file {path} defines no clients")
    return out
