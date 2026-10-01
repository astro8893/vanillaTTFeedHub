"""Input validation for client-supplied keys."""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from ..types import Key

SYMBOL_RE = re.compile(r"^[A-Za-z0-9./:{}=,_$^-]{1,64}$")
BASE_SYMBOL_RE = re.compile(r"^[A-Za-z0-9./:_$^-]{1,48}$")
MAX_SUBS_PER_MSG = 1000


def reject(etype: object, symbol: object, reason: str) -> dict[str, Any]:
    return {
        "type": etype[:32] if isinstance(etype, str) else None,
        "symbol": symbol[:64] if isinstance(symbol, str) else None,
        "reason": reason,
    }


def _check(items: Iterable[Any], allowed: frozenset[str]) -> tuple[list[Key], list[dict[str, Any]]]:
    keys: list[Key] = []
    rejected: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            rejected.append(reject(None, None, "malformed"))
            continue
        etype, symbol = item.get("type"), item.get("symbol")
        if not isinstance(etype, str) or etype not in allowed:
            rejected.append(reject(etype, symbol, "type"))
        elif not isinstance(symbol, str) or not SYMBOL_RE.fullmatch(symbol):
            rejected.append(reject(etype, symbol, "symbol"))
        else:
            keys.append((etype, symbol))
    return list(dict.fromkeys(keys)), rejected


def parse_subs(raw: Any, allowed: frozenset[str]) -> tuple[list[Key], list[dict[str, Any]]] | None:
    """None when `raw` isn't a list of at most MAX_SUBS_PER_MSG items."""
    if not isinstance(raw, list) or len(raw) > MAX_SUBS_PER_MSG:
        return None
    return _check(raw, allowed)


def parse_key_strings(
    items: list[str], allowed: frozenset[str]
) -> tuple[list[Key], list[dict[str, Any]]]:
    """Parse 'Type:Symbol' strings (split at the first ':')."""
    subs: list[dict[str, str]] = []
    rejected: list[dict[str, Any]] = []
    for it in items:
        etype, sep, symbol = it.partition(":")
        if not sep:
            rejected.append(reject(None, it, "format"))
        else:
            subs.append({"type": etype, "symbol": symbol})
    keys, rej = _check(subs, allowed)
    return keys, rejected + rej
