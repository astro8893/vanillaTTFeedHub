from ttfeedhub.core.registry import Registry

K1, K2 = ("Quote", "SPX"), ("Quote", "SPY")


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_first_subscriber_triggers_upstream_add() -> None:
    r = Registry(60, Clock())
    assert r.add("a", [K1, K2]) == [K1, K2]
    assert r.add("b", [K1]) == []
    assert r.refcount(K1) == 2


def test_duplicate_add_by_same_client_counts_once() -> None:
    r = Registry(60, Clock())
    r.add("a", [K1])
    r.add("a", [K1])
    r.remove("a", [K1])
    assert r.refcount(K1) == 0
    assert K1 in r.active()  # lingering


def test_linger_then_expire() -> None:
    c = Clock()
    r = Registry(60, c)
    r.add("a", [K1])
    r.remove("a", [K1])
    c.t = 59.9
    assert r.expired() == []
    c.t = 60.0
    assert r.expired() == [K1]
    assert K1 not in r.active()


def test_resubscribe_during_linger_is_not_new_upstream() -> None:
    c = Clock()
    r = Registry(60, c)
    r.add("a", [K1])
    r.remove("a", [K1])
    assert r.add("b", [K1]) == []
    c.t = 100
    assert r.expired() == []


def test_remove_client_releases_all() -> None:
    r = Registry(60, Clock())
    r.add("a", [K1, K2])
    r.remove_client("a")
    assert r.client_keys("a") == frozenset()
    assert r.active() == {K1, K2}
    assert len(r) == 2


def test_discard_skips_linger() -> None:
    r = Registry(60, Clock())
    r.add("a", [K1])
    r.discard("a", [K1])
    assert K1 not in r.active()


def test_remove_unknown_is_noop() -> None:
    r = Registry(60, Clock())
    r.remove("ghost", [K1])
    r.remove_client("ghost")
    assert r.active() == set()


C1, C1_ALIAS = ("Candle", "SPX{=1m,tho=true}"), ("Candle", "SPX{=m,tho=true}")
C1_CANON = ("Candle", "SPX{=m,tho=true}")


def test_candle_aliases_share_one_upstream_key() -> None:
    r = Registry(60, Clock())
    assert r.add("a", [C1]) == [C1_CANON]
    assert r.add("b", [C1_ALIAS]) == []
    assert r.add("a", [C1_ALIAS]) == []  # a now holds both aliases
    assert r.client_keys("a") == {C1, C1_ALIAS}
    assert r.active() == {C1_CANON}
    r.remove("a", [C1])
    assert r.refcount(C1_CANON) == 2  # a still holds the other alias
    r.remove("b", [C1_ALIAS])
    r.remove("a", [C1_ALIAS])
    assert r.refcount(C1_CANON) == 0
    assert C1_CANON in r.active()  # lingering under the canonical key


def test_discarding_an_alias_releases_the_canonical_key() -> None:
    r = Registry(60, Clock())
    r.add("a", [C1])
    r.discard("a", [C1])
    assert r.active() == set()
