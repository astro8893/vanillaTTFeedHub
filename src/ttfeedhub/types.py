"""Shared type aliases."""

from typing import Any

Key = tuple[str, str]  # (event type, dxfeed symbol)
Event = dict[str, Any]  # decoded event: {"type", "symbol", <fields...>, "rt"}
