"""python -m ttfeedhub"""

from __future__ import annotations

import asyncio
import gc
import logging
import sys
from collections.abc import AsyncIterator

import aiohttp
from aiohttp import web

from .config import ClientSpec, ConfigError, Settings, load_clients, load_settings, read_secret
from .hub import Hub
from .logsafe import setup_logging
from .server.app import build_app
from .tokens import USER_AGENT, ScopeError, TokenManager

log = logging.getLogger("ttfeedhub")


def _install_uvloop() -> None:
    try:
        import uvloop  # type: ignore[import-not-found,unused-ignore]
    except ImportError:
        return
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def _make_app(
    settings: Settings, clients: dict[str, ClientSpec], secret: str, refresh: str
) -> web.Application:
    http = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30), headers={"User-Agent": USER_AGENT}
    )
    tokens = TokenManager(
        http, api_base=settings.api_base, client_secret=secret, refresh_token=refresh
    )
    app = build_app(Hub(settings, clients, tokens, http))

    async def _http(_: web.Application) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await http.close()

    # First, so it is unwound last (after hub.stop) and also when hub.start()
    # fails (e.g. ScopeError), where aiohttp skips on_cleanup.
    app.cleanup_ctx.insert(0, _http)
    # Move everything allocated so far (modules, config, the app) out of the
    # collector's view, so full collections don't rescan it.
    gc.collect()
    gc.freeze()
    return app


def main() -> int:
    try:
        settings = load_settings()
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2
    setup_logging(settings.log_level)
    try:
        secret = read_secret(settings.secrets_dir, "tt_client_secret")
        refresh = read_secret(settings.secrets_dir, "tt_refresh_token")
        clients = load_clients(settings.clients_file)
    except ConfigError as e:
        log.error("config error: %s", e)
        return 2
    _install_uvloop()
    log.info(
        "TTfeedhub starting on %s:%d with %d clients", settings.host, settings.port, len(clients)
    )
    try:
        web.run_app(
            _make_app(settings, clients, secret, refresh),
            host=settings.host,
            port=settings.port,
            access_log=None,
            shutdown_timeout=3,
            print=None,
        )
    except ScopeError as e:
        log.error("refusing to start: %s", e)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
