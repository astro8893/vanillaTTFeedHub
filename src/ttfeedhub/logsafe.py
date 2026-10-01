"""Secret-redacting logging.

Every secret or token the hub loads is registered here and scrubbed from all
log output (messages, arguments and tracebacks) and from client-facing errors.
"""

from __future__ import annotations

import logging
import re
from collections import OrderedDict

_BEARER = re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+")
_TOKEN_FIELD = re.compile(
    r'(?i)("?(?:token|access_token|refresh_token|client_secret)"?\s*[:=]\s*"?)[^",\s}&]+'
)
_MIN_SECRET_LEN = 8
_MAX_RECENT = 32
_pinned: set[str] = set()  # configured secrets: kept forever
_recent: OrderedDict[str, None] = OrderedDict()  # rotating tokens: most recent _MAX_RECENT


def register_secret(value: str, *, pinned: bool = False) -> None:
    """Scrub `value` from all future log output (ignores trivially short values).

    Pinned values (the configured secrets) are kept forever. Rotating tokens are
    kept while they are among the most recent _MAX_RECENT, so the registry
    doesn't grow without bound over months of token refreshes."""
    if len(value) < _MIN_SECRET_LEN:
        return
    if pinned:
        _pinned.add(value)
        _recent.pop(value, None)
    elif value not in _pinned:
        _recent[value] = None
        _recent.move_to_end(value)
        while len(_recent) > _MAX_RECENT:
            _recent.popitem(last=False)


def redact(text: str) -> str:
    for secret in (*_pinned, *_recent):
        if secret in text:
            text = text.replace(secret, "***")
    text = _BEARER.sub(r"\1***", text)
    return _TOKEN_FIELD.sub(r"\1***", text)


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = None
        if record.exc_info:
            record.exc_text = redact(logging.Formatter().formatException(record.exc_info))
        return True


def setup_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.addFilter(RedactingFilter())
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())
