#!/usr/bin/env bash
# Latency benchmark in a Linux container (uvloop), matching production.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 docker run --rm -v "$ROOT":/w -w /w python:3.14-slim sh -c \
  "pip install -q --root-user-action=ignore 'aiohttp>=3.11,<4' 'orjson>=3.10,<4' 'uvloop>=0.21' pytest pytest-asyncio \
   && python -m pytest -m bench -s -p no:cacheprovider tests/bench"
