import io
import logging

from ttfeedhub.logsafe import RedactingFilter, redact, register_secret


def test_registered_secret_is_scrubbed() -> None:
    register_secret("super-secret-value-123")
    assert redact("x super-secret-value-123 y") == "x *** y"


def test_short_values_are_not_registered() -> None:
    register_secret("abc")
    assert redact("abc") == "abc"


def test_bearer_header_is_scrubbed() -> None:
    assert redact("Authorization: Bearer abc.def-ghi") == "Authorization: Bearer ***"


def test_token_fields_are_scrubbed() -> None:
    assert redact('{"token": "abcd1234"}') == '{"token": "***"}'
    assert redact("refresh_token=zzz999&x=1") == "refresh_token=***&x=1"


def test_filter_scrubs_formatted_args_and_tracebacks() -> None:
    register_secret("leaky-secret-0001")
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.addFilter(RedactingFilter())
    lg = logging.getLogger("t.logsafe")
    lg.addHandler(handler)
    lg.propagate = False
    try:
        lg.warning("value=%s", "leaky-secret-0001")
        try:
            raise RuntimeError("boom leaky-secret-0001")
        except RuntimeError:
            lg.exception("failed")
    finally:
        lg.removeHandler(handler)
    out = buf.getvalue()
    assert "leaky-secret-0001" not in out
    assert "value=***" in out
    assert "RuntimeError" in out


def test_registry_is_bounded_but_pinned_secrets_stay() -> None:
    register_secret("pinned-config-secret-01", pinned=True)
    for i in range(40):
        register_secret(f"rotating-token-{i:04d}")
    assert redact("pinned-config-secret-01") == "***"
    assert redact("rotating-token-0039") == "***"  # recent ones are scrubbed
    assert redact("rotating-token-0000") == "rotating-token-0000"  # oldest dropped


def test_read_secret_pins(tmp_path: object) -> None:
    from pathlib import Path

    from ttfeedhub.config import read_secret

    d = Path(str(tmp_path))
    (d / "tt_client_secret").write_text("client-secret-pinned-77")
    read_secret(d, "tt_client_secret")
    for i in range(40):
        register_secret(f"later-token-{i:04d}")
    assert redact("client-secret-pinned-77") == "***"
