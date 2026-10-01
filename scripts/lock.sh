#!/usr/bin/env bash
# Regenerate requirements.lock (hash-pinned runtime deps) for the Linux image.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd -W 2>/dev/null || pwd)"
MSYS_NO_PATHCONV=1 docker run --rm -v "$ROOT":/w -w /w python:3.14-slim sh -c \
  "pip install -q --root-user-action=ignore pip-tools && pip-compile -q --generate-hashes --strip-extras --allow-unsafe -o requirements.lock pyproject.toml"
