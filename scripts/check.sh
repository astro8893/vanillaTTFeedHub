#!/usr/bin/env bash
# Quality gate: lint, format, types, security lint, dependency audit, tests.
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PY:-.venv/Scripts/python}"
[ -x "$PY" ] || PY=.venv/bin/python
"$PY" -m ruff check src client tests scripts
"$PY" -m ruff format --check src client tests scripts
"$PY" -m mypy
"$PY" -m bandit -q -r src client
"$PY" -m pip_audit --skip-editable --progress-spinner off
"$PY" -m pytest -q
