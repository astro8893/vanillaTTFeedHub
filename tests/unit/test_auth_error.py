"""KI-002: which DXLink ERROR frames mean the quote token must be replaced."""

import pytest

from tests.fakes.fake_dxlink import AUTH_FAILED_MSG, SESSION_LIMIT_MSG, TOKEN_EXPIRED_MSG
from ttfeedhub.upstream.connection import (
    QuoteTokenRejected,
    SessionLimitError,
    UpstreamError,
    _classify_error,
)


def err(code: str | None, message: str | None) -> dict[str, object]:
    msg: dict[str, object] = {"type": "ERROR", "channel": 0}
    if code is not None:
        msg["error"] = code
    if message is not None:
        msg["message"] = message
    return msg


@pytest.mark.parametrize("code", ["UNAUTHORIZED", "TOKEN_EXPIRED", "unauthorized"])
def test_auth_error_code_means_token_rejected(code: str) -> None:
    e = _classify_error(err(code, "something odd"))
    assert isinstance(e, QuoteTokenRejected)
    assert str(e) == "DXLink error: something odd"


@pytest.mark.parametrize(
    "message",
    [TOKEN_EXPIRED_MSG, AUTH_FAILED_MSG, AUTH_FAILED_MSG.upper(), "your token has EXPIRED"],
)
@pytest.mark.parametrize("code", ["UNKNOWN", None])
def test_auth_error_text_is_a_fallback(code: str | None, message: str) -> None:
    assert isinstance(_classify_error(err(code, message)), QuoteTokenRejected)


def test_session_limit_is_not_a_token_problem() -> None:
    # the session-limit ERROR carries error=UNAUTHORIZED too; it must not cost a token
    e = _classify_error(err("UNAUTHORIZED", SESSION_LIMIT_MSG))
    assert isinstance(e, SessionLimitError) and not isinstance(e, QuoteTokenRejected)


@pytest.mark.parametrize("code", ["BAD_ACTION", "INVALID_MESSAGE", "TIMEOUT", None])
def test_other_errors_keep_the_token(code: str | None) -> None:
    e = _classify_error(err(code, "bad message"))
    assert type(e) is UpstreamError
