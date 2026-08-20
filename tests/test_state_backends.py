"""One conformance suite, run against both ``StateProvider`` backends.

The memory and Redis backends are swapped by configuration alone, so a
behavioural difference between them is a production bug that only shows up after
switching profile. Every test here therefore runs against both — the parametrised
``state`` fixture is the point of the module. Redis is exercised through
fakeredis, so the suite stays hermetic.

Backend-specific behaviour (TTL expiry mechanics, Redis error translation) lives
in the dedicated classes at the bottom.
"""

import time

import fakeredis
import pytest

from jasil.backends.state_memory import MemoryState
from jasil.backends.state_redis import RedisState
from jasil.providers import StateBackendUnavailableError, StateProvider, TieredFailureOutcome


@pytest.fixture(params=["memory", "redis"])
def state(request) -> StateProvider:
    if request.param == "memory":
        return MemoryState()
    return RedisState(fakeredis.FakeStrictRedis(decode_responses=False))


class TestProviderConformance:
    def test_both_backends_satisfy_the_protocol(self, state):
        assert isinstance(state, StateProvider)

    def test_getting_a_missing_key_returns_none(self, state):
        assert state.get("absent") is None

    def test_a_value_round_trips_as_bytes(self, state):
        state.set("k", b"value")

        assert state.get("k") == b"value"

    def test_setting_an_existing_key_overwrites_it(self, state):
        state.set("k", b"first")

        state.set("k", b"second")

        assert state.get("k") == b"second"

    def test_deleting_removes_the_key(self, state):
        state.set("k", b"value")

        state.delete("k")

        assert state.get("k") is None

    def test_deleting_a_missing_key_is_a_no_op(self, state):
        state.delete("absent")

    def test_incr_starts_from_zero(self, state):
        assert state.incr("counter") == 1

    def test_incr_accumulates(self, state):
        state.incr("counter")

        assert state.incr("counter") == 2

    def test_incr_accepts_an_amount(self, state):
        assert state.incr("counter", amount=5) == 5

    def test_set_if_absent_claims_a_free_key(self, state):
        assert state.set_if_absent("lock", b"owner") is True

    def test_set_if_absent_refuses_a_taken_key(self, state):
        """The single-winner guarantee callers rely on for claim-style keys."""
        state.set_if_absent("lock", b"first")

        assert state.set_if_absent("lock", b"second") is False
        assert state.get("lock") == b"first"

    def test_get_and_delete_returns_the_value_then_clears_it(self, state):
        state.set("once", b"value")

        assert state.get_and_delete("once") == b"value"
        assert state.get("once") is None

    def test_get_and_delete_on_a_missing_key_returns_none(self, state):
        assert state.get_and_delete("absent") is None

    def test_delete_prefix_removes_only_matching_keys(self, state):
        state.set("session:a", b"1")
        state.set("session:b", b"2")
        state.set("other:c", b"3")

        deleted = state.delete_prefix("session:")

        assert deleted == 2
        assert state.get("other:c") == b"3"

    def test_delete_prefix_on_no_matches_returns_zero(self, state):
        assert state.delete_prefix("nothing:") == 0

    def test_iter_keys_yields_only_matching_keys(self, state):
        state.set("session:a", b"1")
        state.set("session:b", b"2")
        state.set("other:c", b"3")

        assert sorted(state.iter_keys("session:")) == ["session:a", "session:b"]

    def test_iter_keys_yields_strings(self, state):
        """Callers index by key name, so both backends must decode."""
        state.set("session:a", b"1")

        assert all(isinstance(key, str) for key in state.iter_keys("session:"))

    def test_iter_keys_on_no_matches_is_empty(self, state):
        assert list(state.iter_keys("nothing:")) == []

    @pytest.mark.parametrize("prefix", ["tenant:a*b:", "user:[1]:", "q?:", "back\\slash:"])
    def test_a_prefix_is_matched_literally_and_not_as_a_glob(self, state, prefix):
        """Redis reads a MATCH pattern as a glob; the memory backend uses ``startswith``.

        A host that builds a prefix from a tenant or user id hands this method a
        value it does not control the characters of. Unescaped, ``*`` and ``?``
        widen the match onto keys the caller never named and ``[...]`` narrows it
        onto ones that do not exist.
        """
        state.set(f"{prefix}kept", b"1")
        state.set("tenant:aXb:other", b"2")
        state.set("user:1:other", b"3")
        state.set("qZ:other", b"4")
        state.set("backslash:other", b"5")

        assert sorted(state.iter_keys(prefix)) == [f"{prefix}kept"]

    def test_delete_prefix_cannot_be_widened_into_the_whole_keyspace(self, state):
        """``delete_prefix("*")`` deletes keys starting with a literal asterisk. Nothing else."""
        state.set("session:a", b"1")
        state.set("other:b", b"2")

        assert state.delete_prefix("*") == 0
        assert state.get("session:a") == b"1"
        assert state.get("other:b") == b"2"

    def test_delete_prefix_removes_a_key_holding_a_metacharacter(self, state):
        state.set("tenant:a*b:one", b"1")
        state.set("tenant:aXb:two", b"2")

        assert state.delete_prefix("tenant:a*b:") == 1
        assert state.get("tenant:aXb:two") == b"2"

    def test_a_ttl_bearing_value_is_readable_before_it_expires(self, state):
        state.set("k", b"value", ttl_seconds=60)

        assert state.get("k") == b"value"


class TestTieredFailureConformance:
    """The atomic lockout both backends implement — memory under a lock, Redis in Lua."""

    TIERS = ((3, 60), (5, 300))

    def _record(self, state) -> TieredFailureOutcome:
        return state.record_tiered_failure("counter", "gate", self.TIERS, 900)

    def test_the_first_failure_counts_without_locking(self, state):
        outcome = self._record(state)

        assert outcome == TieredFailureOutcome(1, None, False)

    def test_failures_accumulate_below_the_first_threshold(self, state):
        self._record(state)

        assert self._record(state).count == 2

    def test_reaching_a_threshold_locks(self, state):
        for _ in range(2):
            self._record(state)

        outcome = self._record(state)

        assert outcome.count == 3
        assert outcome.newly_locked is True
        assert outcome.locked_until_epoch > int(time.time())

    def test_a_locked_caller_does_not_inflate_the_counter(self, state):
        """Otherwise a client hammering while locked would climb into the next
        tier and extend its own lockout indefinitely."""
        for _ in range(3):
            self._record(state)

        outcome = self._record(state)

        assert outcome.count == 3
        assert outcome.newly_locked is False

    def test_the_highest_crossed_tier_wins(self, state):
        """Tiers are ascending and the last match applies, so a caller that
        somehow reaches tier two gets the longer lockout, not the shorter."""
        outcome = state.record_tiered_failure("c", "g", ((1, 60), (1, 300)), 900)

        assert outcome.locked_until_epoch - int(time.time()) > 60


class TestMemoryBackendExpiry:
    """Expiry mechanics, driven by a fake clock rather than by sleeping."""

    @pytest.fixture
    def clock(self, monkeypatch):
        current = {"now": 1000.0}
        monkeypatch.setattr("jasil.backends.state_memory.time.monotonic", lambda: current["now"])
        return current

    def test_a_value_expires_once_its_ttl_elapses(self, clock):
        state = MemoryState()
        state.set("k", b"value", ttl_seconds=10)

        clock["now"] += 11

        assert state.get("k") is None

    def test_a_value_without_a_ttl_never_expires(self, clock):
        state = MemoryState()
        state.set("k", b"value")

        clock["now"] += 10_000

        assert state.get("k") == b"value"

    def test_an_expired_key_frees_its_slot_for_set_if_absent(self, clock):
        state = MemoryState()
        state.set_if_absent("lock", b"first", ttl_seconds=10)

        clock["now"] += 11

        assert state.set_if_absent("lock", b"second") is True

    def test_incr_preserves_an_existing_expiry(self, clock):
        """Re-arming the TTL on every increment would let a steady stream of
        failures keep a rate-limit window alive forever."""
        state = MemoryState()
        state.incr("counter", ttl_seconds=10)

        state.incr("counter")
        clock["now"] += 11

        assert state.get("counter") is None

    def test_expired_keys_are_not_listed(self, clock):
        state = MemoryState()
        state.set("session:a", b"1", ttl_seconds=10)
        state.set("session:b", b"2")

        clock["now"] += 11

        assert list(state.iter_keys("session:")) == ["session:b"]


class TestRedisBackendErrorTranslation:
    """Redis outages must reach callers as the neutral provider error.

    Domain code catches ``StateBackendUnavailableError`` so it never imports or
    knows about redis-py.
    """

    @pytest.fixture
    def failing_state(self):
        from redis.exceptions import ConnectionError as RedisConnectionError

        class _Broken(fakeredis.FakeStrictRedis):
            def get(self, *args, **kwargs):
                raise RedisConnectionError("connection refused")

        return RedisState(_Broken(decode_responses=False))

    def test_a_redis_outage_surfaces_as_a_provider_error(self, failing_state):
        with pytest.raises(StateBackendUnavailableError):
            failing_state.get("k")

    def test_the_original_redis_error_is_chained(self, failing_state):
        """Kept as ``__cause__`` so the operator-facing traceback still names the
        real fault."""
        with pytest.raises(StateBackendUnavailableError) as excinfo:
            failing_state.get("k")

        assert excinfo.value.__cause__ is not None

    def test_the_memory_backend_never_raises_the_provider_error(self):
        assert MemoryState().get("absent") is None
