"""Create a client token for TTfeedhub.

Prints the token ONCE on stdout; appends only its SHA-256 to clients.toml.

    python scripts/new_client.py my-dashboard --file clients.toml --max-keys 2000
"""

from __future__ import annotations

import argparse
import json
import secrets
import sys
import tempfile
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ttfeedhub.config import EVENT_TYPES, NAME_RE, ConfigError, hash_token, load_clients


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("name")
    p.add_argument("--file", type=Path, default=Path("clients.toml"))
    p.add_argument("--max-keys", type=int, default=2000)
    p.add_argument("--max-connections", type=int, default=2)
    p.add_argument("--types", default=",".join(sorted(EVENT_TYPES)))
    p.add_argument("--candles-per-min", type=int, default=60)
    p.add_argument("--snapshots-per-min", type=int, default=60)
    a = p.parse_args(argv)
    if not NAME_RE.fullmatch(a.name):
        print("name must be lowercase [a-z0-9_-], max 32 chars", file=sys.stderr)
        return 1
    types = [t.strip() for t in a.types.split(",") if t.strip()]
    if not types:
        print("event_types cannot be empty", file=sys.stderr)
        return 1
    if not set(types) <= EVENT_TYPES:
        print(f"unknown event types: {sorted(set(types) - EVENT_TYPES)}", file=sys.stderr)
        return 1
    try:
        existing = tomllib.loads(a.file.read_text(encoding="utf-8")) if a.file.exists() else {}
    except tomllib.TOMLDecodeError as e:
        print(f"error reading existing clients file: {e}", file=sys.stderr)
        return 1
    if a.name in (existing.get("clients") or {}):
        print(f"client {a.name!r} already exists in {a.file}", file=sys.stderr)
        return 1
    token = secrets.token_urlsafe(32)
    block = (
        f"\n[clients.{a.name}]\n"
        f'token_sha256 = "{hash_token(token)}"\n'
        f"max_keys = {a.max_keys}\n"
        f"max_connections = {a.max_connections}\n"
        f"event_types = [{', '.join(json.dumps(t) for t in types)}]\n"
        f"candles_per_min = {a.candles_per_min}\n"
        f"snapshots_per_min = {a.snapshots_per_min}\n"
    )
    # Validate the would-be file content before writing to the real file
    existing_text = a.file.read_text(encoding="utf-8") if a.file.exists() else ""
    would_be_content = existing_text + block
    temp_file = None
    try:
        # Use a temp file in the same directory as the target file for atomic validation
        temp_dir = a.file.parent
        with tempfile.NamedTemporaryFile(
            mode="w", dir=temp_dir, delete=False, encoding="utf-8", suffix=".toml"
        ) as tmp:
            tmp.write(would_be_content)
            temp_file = Path(tmp.name)
        load_clients(temp_file)
    except ConfigError as e:
        print(f"validation failed: {e}", file=sys.stderr)
        return 1
    except tomllib.TOMLDecodeError as e:
        print(f"validation failed: {e}", file=sys.stderr)
        return 1
    finally:
        if temp_file and temp_file.exists():
            temp_file.unlink()
    # Validation passed, write to the real file
    with a.file.open("a", encoding="utf-8") as f:
        f.write(block)
    print(token)
    print(f"# token for {a.name!r} shown once; its hash was appended to {a.file}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
