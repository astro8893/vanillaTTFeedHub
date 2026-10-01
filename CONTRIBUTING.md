# Contributing

Thanks for helping. A few ground rules:

- **Run the quality gate before you open a pull request:**
  ```bash
  python -m venv .venv && .venv/Scripts/python -m pip install -U pip && .venv/Scripts/python -m pip install -e ".[dev]"   # .venv/bin/python on Linux/macOS
  bash scripts/check.sh   # ruff, ruff format, mypy --strict, bandit, pip-audit, pytest
  ```
  Everything must pass.
- **Tests use fakes, never the network.** `tests/fakes/` has a fake DXLink
  server and a fake tastytrade API; the test suite needs no account, no
  credentials and no internet. Make fakes behave like the real service,
  quirks included (see KI-001 in `docs/KNOWN_ISSUES.md`).
- **Bug fixes come with a test** that fails without the fix.
- **Never commit secrets:** `secrets/`, `clients.toml` and `.env` are
  gitignored; keep it that way. Don't paste tokens into issues or logs.
- `scripts/smoke_live.py` and `ttfeedhub.tools.socket_limit` talk to live
  tastytrade with your own account; they are not part of the test suite.
- New known issues or lessons go in `docs/KNOWN_ISSUES.md` with the next
  free `KI-NNN` id.
