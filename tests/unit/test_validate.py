from ttfeedhub.server.validate import BASE_SYMBOL_RE, SYMBOL_RE, parse_key_strings, parse_subs

ALL = frozenset({"Quote", "Candle"})


def test_trailing_newline_is_rejected() -> None:
    assert SYMBOL_RE.fullmatch("SPX") and not SYMBOL_RE.fullmatch("SPX\n")
    assert BASE_SYMBOL_RE.fullmatch("SPX") and not BASE_SYMBOL_RE.fullmatch("SPX\n")
    parsed = parse_subs([{"type": "Quote", "symbol": "SPX\n"}], ALL)
    assert parsed is not None
    keys, rejected = parsed
    assert keys == [] and rejected[0]["reason"] == "symbol"
    keys, rejected = parse_key_strings(["Quote:SPX\n", "Quote:SPY"], ALL)
    assert keys == [("Quote", "SPY")] and rejected[0]["reason"] == "symbol"
