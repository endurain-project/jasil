"""One conformance suite, run against both ``StateProvider`` backends.

The memory and Redis backends are swapped by configuration alone, so a
behavioural difference between them is a production bug that only shows up after
switching profile. Every test here therefore runs against both — the parametrised
``state`` fixture is the point of the module. Redis is exercised through
fakeredis, so the suite stays hermetic.

Backend-specific behaviour (TTL expiry mechanics, Redis error translation) lives
in the dedicated classes at the bottom.
"""

import math
import threading
import time

import fakeredis
import pytest

import jasil._core.state_keys as state_keys
from jasil.backends.state_memory import MemoryState
from jasil.backends.state_redis import RedisState
from jasil.providers import SlotReservation, StateBackendUnavailableError, StateProvider, TieredFailureOutcome


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


class TestArgumentConformance:
    """Arguments one backend would honour and the other misread are refused by both."""

    @pytest.mark.parametrize("ttl", [0, -5])
    @pytest.mark.parametrize(
        "operation",
        [
            lambda state, ttl: state.set("k", b"v", ttl_seconds=ttl),
            lambda state, ttl: state.set_if_absent("k", b"v", ttl_seconds=ttl),
            lambda state, ttl: state.incr("k", ttl_seconds=ttl),
        ],
        ids=["set", "set_if_absent", "incr"],
    )
    def test_a_ttl_that_is_not_positive_is_refused(self, state, operation, ttl):
        """Memory read it as already expired and Redis as no expiry: a computed TTL of 0 became a permanent key."""
        with pytest.raises(ValueError, match="ttl_seconds must be positive"):
            operation(state, ttl)

        assert state.get("k") is None

    def test_an_empty_prefix_is_refused_by_delete_prefix(self, state):
        """On Redis it would delete every key in the database, the event stream included."""
        state.set("k", b"v")

        with pytest.raises(ValueError, match="prefix"):
            state.delete_prefix("")

        assert state.get("k") == b"v"

    def test_incrementing_a_value_that_is_not_an_integer_is_a_value_error(self, state):
        state.set("k", b"abc")

        with pytest.raises(ValueError):
            state.incr("k")

    @pytest.mark.parametrize(("stored", "amount"), [(2**63 - 1, 1), (-(2**63), -1)])
    def test_an_increment_past_the_64_bit_range_is_a_value_error(self, state, stored, amount):
        """Redis counters are signed 64-bit integers; the memory backend must not count on past them."""
        state.set("k", str(stored).encode())

        with pytest.raises(ValueError):
            state.incr("k", amount)

        assert state.get("k") == str(stored).encode()

    @pytest.mark.parametrize("stored", [2**63, -(2**63) - 1])
    def test_a_stored_value_outside_the_64_bit_range_is_a_value_error(self, state, stored):
        state.set("k", str(stored).encode())

        with pytest.raises(ValueError):
            state.incr("k")

    @pytest.mark.parametrize("amount", [2**63, -(2**63) - 1])
    def test_an_amount_outside_the_64_bit_range_is_a_value_error(self, state, amount):
        with pytest.raises(ValueError):
            state.incr("k", amount)

        assert state.get("k") is None


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

    def test_a_count_past_the_64_bit_range_is_refused_and_left_unchanged(self, state):
        state.set("counter", str(2**63 - 1).encode())

        with pytest.raises(ValueError):
            self._record(state)

        assert state.get("counter") == str(2**63 - 1).encode()

    def test_the_highest_crossed_tier_wins(self, state):
        """Tiers are ascending and the last match applies, so a caller that
        somehow reaches tier two gets the longer lockout, not the shorter."""
        outcome = state.record_tiered_failure("c", "g", ((1, 60), (1, 300)), 900)

        assert outcome.locked_until_epoch - int(time.time()) > 60


class TestTtlConformance:
    """A TTL passed to the compound operations reaches the key on both backends."""

    def test_incr_with_a_ttl_still_counts(self, state):
        state.incr("counter", ttl_seconds=60)

        assert state.incr("counter", ttl_seconds=60) == 2

    def test_set_if_absent_with_a_ttl_claims_then_refuses(self, state):
        assert state.set_if_absent("lock", b"first", ttl_seconds=60) is True
        assert state.set_if_absent("lock", b"second", ttl_seconds=60) is False
        assert state.get("lock") == b"first"


@pytest.fixture
def clock(monkeypatch):
    """Drive both clocks the backends read: the memory backend's monotonic clock, and the wall clock the Redis
    backend sends its deadlines in and fakeredis expires keys by."""
    offset = {"seconds": 0.0}
    monkeypatch.setattr(time, "time", lambda: 1_800_000_000.0 + offset["seconds"])
    monkeypatch.setattr(time, "monotonic", lambda: 1000.0 + offset["seconds"])
    return offset


def _whole_index(state, key, page_size):
    """Walk an index page by page with the keyset cursor, as a caller would."""
    walked, before = [], None
    while page := state.index_range(key, limit=page_size, before=before):
        walked += page
        member, score = page[-1]
        before = (score, member)
    return walked


class TestCompareConformance:
    def test_delete_if_equals_deletes_the_value_it_names(self, state):
        state.set("claim", b"job-1")

        assert state.delete_if_equals("claim", b"job-1") is True
        assert state.get("claim") is None

    def test_delete_if_equals_leaves_another_holders_value(self, state):
        state.set("claim", b"job-2")

        assert state.delete_if_equals("claim", b"job-1") is False
        assert state.get("claim") == b"job-2"

    def test_delete_if_equals_on_an_absent_key_deletes_nothing(self, state):
        assert state.delete_if_equals("claim", b"job-1") is False

    def test_delete_if_equals_ignores_an_expired_value(self, clock, state):
        state.set("claim", b"job-1", ttl_seconds=10)
        clock["seconds"] += 11

        assert state.delete_if_equals("claim", b"job-1") is False

    def test_expire_if_equals_restarts_the_expiry_of_the_value_it_names(self, clock, state):
        state.set("claim", b"job-1", ttl_seconds=10)
        clock["seconds"] += 8

        assert state.expire_if_equals("claim", b"job-1", ttl_seconds=10) is True
        clock["seconds"] += 8
        assert state.get("claim") == b"job-1"

    def test_expire_if_equals_leaves_another_holders_expiry(self, clock, state):
        state.set("claim", b"job-2", ttl_seconds=10)

        assert state.expire_if_equals("claim", b"job-1", ttl_seconds=100) is False
        clock["seconds"] += 11
        assert state.get("claim") is None

    def test_expire_if_equals_on_an_absent_key_creates_nothing(self, state):
        assert state.expire_if_equals("claim", b"job-1", ttl_seconds=10) is False
        assert state.get("claim") is None

    @pytest.mark.parametrize("ttl", [0, -1])
    def test_expire_if_equals_refuses_a_ttl_that_is_not_positive(self, state, ttl):
        state.set("claim", b"job-1")

        with pytest.raises(ValueError, match="ttl_seconds"):
            state.expire_if_equals("claim", b"job-1", ttl_seconds=ttl)


class TestIndexConformance:
    def test_members_list_by_score_then_member_both_descending(self, state):
        for member, score in [("a", 1.0), ("b", 2.0), ("é", 2.0), ("z", 2.0), ("d", 3.0)]:
            state.index_add("idx", member, score, ttl_seconds=60)

        assert state.index_range("idx", limit=10) == [("d", 3.0), ("é", 2.0), ("z", 2.0), ("b", 2.0), ("a", 1.0)]

    @pytest.mark.parametrize("page_size", [1, 2, 3, 10])
    def test_the_cursor_pages_through_ties_without_skipping_or_repeating(self, state, page_size):
        members = [("m1", 5.0), ("m2", 5.0), ("m3", 5.0), ("é", 5.0), ("n", 4.0), ("o", 4.0), ("p", 1.0)]
        for member, score in members:
            state.index_add("idx", member, score, ttl_seconds=60)

        assert _whole_index(state, "idx", page_size) == state.index_range("idx", limit=100)
        assert len(_whole_index(state, "idx", page_size)) == len(members)

    def test_the_cursor_need_not_name_a_member_still_in_the_index(self, state):
        for member, score in [("a", 1.0), ("c", 2.0), ("e", 2.0)]:
            state.index_add("idx", member, score, ttl_seconds=60)

        assert state.index_range("idx", limit=10, before=(2.0, "d")) == [("c", 2.0), ("a", 1.0)]
        assert state.index_range("idx", limit=10, before=(1.5, "zzz")) == [("a", 1.0)]
        assert state.index_range("idx", limit=10, before=(1.0, "a")) == []

    def test_limit_caps_the_page(self, state):
        for n in range(5):
            state.index_add("idx", f"m{n}", float(n), ttl_seconds=60)

        assert state.index_range("idx", limit=2) == [("m4", 4.0), ("m3", 3.0)]

    def test_re_adding_a_member_replaces_its_score(self, state):
        state.index_add("idx", "a", 1.0, ttl_seconds=60)
        state.index_add("idx", "b", 2.0, ttl_seconds=60)

        state.index_add("idx", "a", 3.0, ttl_seconds=60)

        assert state.index_range("idx", limit=10) == [("a", 3.0), ("b", 2.0)]

    def test_a_score_round_trips_exactly(self, state):
        score = 1_731_000_000.123456
        state.index_add("idx", "a", score, ttl_seconds=60)

        [(_member, stored)] = state.index_range("idx", limit=1)
        assert stored == score
        assert state.index_range("idx", limit=1, before=(stored, "b")) == [("a", score)]

    def test_remove_drops_a_member(self, state):
        state.index_add("idx", "a", 1.0, ttl_seconds=60)
        state.index_add("idx", "b", 2.0, ttl_seconds=60)

        state.index_remove("idx", "b")

        assert state.index_range("idx", limit=10) == [("a", 1.0)]

    def test_removing_the_last_member_removes_the_index(self, state):
        state.index_add("idx", "a", 1.0, ttl_seconds=60)

        state.index_remove("idx", "a")

        assert list(state.iter_keys("idx")) == []

    def test_removing_from_an_absent_index_or_member_is_a_no_op(self, state):
        state.index_remove("idx", "a")
        state.index_add("idx", "a", 1.0, ttl_seconds=60)
        state.index_remove("idx", "b")

        assert state.index_range("idx", limit=10) == [("a", 1.0)]

    def test_an_absent_index_has_no_members(self, state):
        assert state.index_range("idx", limit=10) == []

    @pytest.mark.parametrize("ttl", [0, -1])
    def test_index_add_refuses_a_ttl_that_is_not_positive(self, state, ttl):
        with pytest.raises(ValueError, match="ttl_seconds"):
            state.index_add("idx", "a", 1.0, ttl_seconds=ttl)

    @pytest.mark.parametrize("score", [float("nan"), float("inf"), float("-inf")])
    def test_index_add_refuses_a_score_that_is_not_finite(self, state, score):
        with pytest.raises(ValueError, match="finite"):
            state.index_add("idx", "a", score, ttl_seconds=60)

    def test_index_range_refuses_a_cursor_score_that_is_not_finite(self, state):
        with pytest.raises(ValueError, match="finite"):
            state.index_range("idx", limit=1, before=(float("nan"), "a"))

    @pytest.mark.parametrize("limit", [0, -1])
    def test_index_range_refuses_a_limit_below_one(self, state, limit):
        with pytest.raises(ValueError, match="limit"):
            state.index_range("idx", limit=limit)

    def test_the_index_key_is_listed_once_and_deleted_by_prefix_as_one_key(self, state):
        state.index_add("owner:1", "a", 1.0, ttl_seconds=60)
        state.index_add("owner:1", "b", 2.0, ttl_seconds=60)
        state.set("owner:2", b"x")

        assert sorted(state.iter_keys("owner:")) == ["owner:1", "owner:2"]
        assert state.delete_prefix("owner:") == 2
        assert state.index_range("owner:1", limit=10) == []

    def test_delete_removes_an_index(self, state):
        state.index_add("idx", "a", 1.0, ttl_seconds=60)

        state.delete("idx")

        assert state.index_range("idx", limit=10) == []

    def test_an_index_deleted_and_rebuilt_keeps_none_of_its_old_members(self, state):
        state.index_add("idx", "a", 1.0, ttl_seconds=60)
        state.delete_prefix("idx")

        state.index_add("idx", "b", 2.0, ttl_seconds=60)

        assert state.index_range("idx", limit=10) == [("b", 2.0)]

    @pytest.mark.parametrize("prefix", ["j", "jasil:", "jasil:state:index-expiry:", "jasil:state:index-expiry:jobs"])
    def test_delete_prefix_refuses_a_prefix_reaching_the_reserved_index_namespace(self, clock, state, prefix):
        """On Redis it would delete index deadlines and keep their indexes, whose members would then never expire."""
        state.index_add("jobs:idx", "short", 2.0, ttl_seconds=10)
        state.index_add("jobs:idx", "long", 1.0, ttl_seconds=100)

        with pytest.raises(ValueError, match="reserved"):
            state.delete_prefix(prefix)

        clock["seconds"] += 11
        assert state.index_range("jobs:idx", limit=10) == [("long", 1.0)]

    def test_delete_prefix_accepts_a_jasil_prefix_beside_the_reserved_index_namespace(self, state):
        state.set("jasil:other:k", b"v")

        assert state.delete_prefix("jasil:other:") == 1


class TestIndexKeyTypeConformance:
    """A key holds a plain value or an index, and each kind of operation refuses the other kind the same way."""

    @pytest.mark.parametrize(
        "operation",
        [
            lambda state: state.index_add("k", "a", 1.0, ttl_seconds=60),
            lambda state: state.index_remove("k", "a"),
            lambda state: state.index_range("k", limit=1),
        ],
    )
    def test_an_index_operation_refuses_a_plain_key(self, state, operation):
        state.set("k", b"x")

        with pytest.raises(ValueError):
            operation(state)
        assert state.get("k") == b"x"

    @pytest.mark.parametrize(
        "operation",
        [
            lambda state: state.get("k"),
            lambda state: state.incr("k"),
            lambda state: state.get_and_delete("k"),
            lambda state: state.delete_if_equals("k", b"x"),
            lambda state: state.expire_if_equals("k", b"x", ttl_seconds=60),
            lambda state: state.record_tiered_failure("counter", "k", ((1, 60),), 900),
        ],
    )
    def test_a_plain_value_operation_refuses_an_index_key(self, state, operation):
        state.index_add("k", "a", 1.0, ttl_seconds=60)

        with pytest.raises(ValueError):
            operation(state)
        assert state.index_range("k", limit=1) == [("a", 1.0)]

    def test_set_if_absent_finds_an_index_key_taken(self, state):
        state.index_add("k", "a", 1.0, ttl_seconds=60)

        assert state.set_if_absent("k", b"x") is False
        assert state.index_range("k", limit=1) == [("a", 1.0)]

    def test_set_replaces_an_index_with_a_plain_value(self, state):
        state.index_add("k", "a", 1.0, ttl_seconds=60)

        state.set("k", b"x")

        assert state.get("k") == b"x"

    def test_an_expired_plain_value_frees_its_key_for_an_index(self, clock, state):
        state.set("k", b"x", ttl_seconds=10)
        clock["seconds"] += 11

        state.index_add("k", "a", 1.0, ttl_seconds=60)

        assert state.index_range("k", limit=1) == [("a", 1.0)]


class TestIndexExpiryConformance:
    def test_an_expired_member_is_never_listed(self, clock, state):
        state.index_add("idx", "short", 2.0, ttl_seconds=10)
        state.index_add("idx", "long", 1.0, ttl_seconds=100)

        clock["seconds"] += 11

        assert state.index_range("idx", limit=10) == [("long", 1.0)]
        assert state.index_range("idx", limit=10, before=(3.0, "")) == [("long", 1.0)]

    def test_re_adding_a_member_restarts_its_expiry(self, clock, state):
        state.index_add("idx", "a", 1.0, ttl_seconds=10)
        clock["seconds"] += 8

        state.index_add("idx", "a", 1.0, ttl_seconds=10)
        clock["seconds"] += 8

        assert state.index_range("idx", limit=10) == [("a", 1.0)]

    def test_an_index_is_gone_once_its_last_member_expired(self, clock, state):
        state.index_add("idx", "a", 1.0, ttl_seconds=10)
        state.index_add("idx", "b", 2.0, ttl_seconds=20)

        clock["seconds"] += 21

        assert list(state.iter_keys("idx")) == []
        assert state.delete_prefix("idx") == 0
        assert state.set_if_absent("idx", b"x") is True

    def test_removing_the_longest_lived_member_shortens_the_index_lifetime(self, clock, state):
        state.index_add("idx", "a", 1.0, ttl_seconds=10)
        state.index_add("idx", "b", 2.0, ttl_seconds=100)

        state.index_remove("idx", "b")
        clock["seconds"] += 11

        assert list(state.iter_keys("idx")) == []


class TestIndexConcurrency:
    """Each index call is atomic: concurrent adds and removes lose no update."""

    WRITERS = 8
    MEMBERS_PER_WRITER = 40

    def _run_writers(self, states):
        def write(writer, state):
            for n in range(self.MEMBERS_PER_WRITER):
                state.index_add("idx", f"w{writer}-{n}", float(n), ttl_seconds=60)
                if n % 2:
                    state.index_remove("idx", f"w{writer}-{n}")

        threads = [threading.Thread(target=write, args=(w, states[w])) for w in range(self.WRITERS)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def _expected(self):
        return {f"w{w}-{n}" for w in range(self.WRITERS) for n in range(0, self.MEMBERS_PER_WRITER, 2)}

    def test_threads_sharing_a_memory_backend(self):
        state = MemoryState()

        self._run_writers([state] * self.WRITERS)

        assert {member for member, _score in state.index_range("idx", limit=1000)} == self._expected()

    def test_connections_sharing_a_redis_server(self):
        server = fakeredis.FakeServer()
        states = [RedisState(fakeredis.FakeStrictRedis(server=server)) for _ in range(self.WRITERS)]

        self._run_writers(states)

        assert {member for member, _score in states[0].index_range("idx", limit=1000)} == self._expected()


class TestSlotConformance:
    """A rate gate admits one caller per interval, each in a slot of its own, on the backend's single clock."""

    def test_the_first_reservation_may_act_at_once(self, clock, state):
        assert state.reserve_slot("gate", 1.0, max_wait_seconds=0) == SlotReservation(True, 0.0)

    def test_consecutive_reservations_are_spaced_by_the_interval(self, clock, state):
        waits = [state.reserve_slot("gate", 0.5, max_wait_seconds=10).wait_seconds for _ in range(3)]

        assert waits == pytest.approx([0.0, 0.5, 1.0])

    def test_a_sub_second_interval_keeps_its_precision(self, clock, state):
        waits = [state.reserve_slot("gate", 0.01, max_wait_seconds=10).wait_seconds for _ in range(4)]

        assert waits == pytest.approx([0.0, 0.01, 0.02, 0.03])

    def test_a_gate_idle_for_its_interval_admits_at_once(self, clock, state):
        state.reserve_slot("gate", 1.0, max_wait_seconds=10)
        state.reserve_slot("gate", 1.0, max_wait_seconds=10)
        clock["seconds"] += 5

        assert state.reserve_slot("gate", 1.0, max_wait_seconds=0) == SlotReservation(True, 0.0)

    def test_a_wait_past_the_bound_reserves_nothing(self, clock, state):
        state.reserve_slot("gate", 1.0, max_wait_seconds=0)

        refused = state.reserve_slot("gate", 1.0, max_wait_seconds=0.5)

        assert refused == SlotReservation(False, pytest.approx(1.0))
        assert state.reserve_slot("gate", 1.0, max_wait_seconds=0.5) == refused
        assert state.reserve_slot("gate", 1.0, max_wait_seconds=1.0) == SlotReservation(True, pytest.approx(1.0))

    def test_gates_under_different_keys_are_independent(self, clock, state):
        state.reserve_slot("gate:a", 1.0, max_wait_seconds=0)

        assert state.reserve_slot("gate:b", 1.0, max_wait_seconds=0) == SlotReservation(True, 0.0)

    def test_an_idle_gate_expires_after_its_last_slot(self, clock, state):
        state.reserve_slot("gate", 1.0, max_wait_seconds=10)
        state.reserve_slot("gate", 1.0, max_wait_seconds=10)  # the last slot is one second ahead
        clock["seconds"] += 1.5
        assert list(state.iter_keys("gate")) == ["gate"]

        clock["seconds"] += 1.0

        assert list(state.iter_keys("gate")) == []

    def test_an_index_key_is_refused(self, state):
        state.index_add("gate", "a", 1.0, ttl_seconds=60)

        with pytest.raises(ValueError):
            state.reserve_slot("gate", 1.0, max_wait_seconds=0)
        assert state.index_range("gate", limit=1) == [("a", 1.0)]

    def test_a_plain_value_that_is_not_a_slot_is_refused(self, state):
        state.set("gate", b"abc")

        with pytest.raises(ValueError):
            state.reserve_slot("gate", 1.0, max_wait_seconds=0)
        assert state.get("gate") == b"abc"

    @pytest.mark.parametrize("interval", [0, -1.0, math.inf, math.nan, 86_400.5])
    def test_an_interval_that_is_not_positive_finite_and_at_most_a_day_is_refused(self, state, interval):
        with pytest.raises(ValueError, match="interval_seconds"):
            state.reserve_slot("gate", interval, max_wait_seconds=0)

        assert state.get("gate") is None

    @pytest.mark.parametrize("max_wait", [-0.1, math.inf, math.nan])
    def test_a_max_wait_that_is_negative_or_not_finite_is_refused(self, state, max_wait):
        with pytest.raises(ValueError, match="max_wait_seconds"):
            state.reserve_slot("gate", 1.0, max_wait_seconds=max_wait)

        assert state.get("gate") is None


class TestSlotConcurrency:
    """Concurrent callers each get a slot of their own: none shares one, none is skipped."""

    CALLERS = 8

    def _reserve_concurrently(self, states):
        waits = []
        barrier = threading.Barrier(self.CALLERS)

        def reserve(state):
            barrier.wait()
            waits.append(state.reserve_slot("gate", 0.1, max_wait_seconds=60).wait_seconds)

        threads = [threading.Thread(target=reserve, args=(state,)) for state in states]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return sorted(waits)

    def test_threads_sharing_a_memory_backend(self, clock):
        waits = self._reserve_concurrently([MemoryState()] * self.CALLERS)

        assert waits == pytest.approx([0.1 * n for n in range(self.CALLERS)])

    def test_connections_sharing_a_redis_server(self, clock):
        server = fakeredis.FakeServer()
        states = [RedisState(fakeredis.FakeStrictRedis(server=server)) for _ in range(self.CALLERS)]

        waits = self._reserve_concurrently(states)

        assert waits == pytest.approx([0.1 * n for n in range(self.CALLERS)])


class TestSendConformance:
    """``reserve_send`` books the slot a deferred caller will come back for, and never lets two sends out closer
    together than the interval, however late a booked caller comes back."""

    def test_without_a_booking_it_spaces_sends_as_reserve_slot_does(self, clock, state):
        waits = [state.reserve_send("gate", 0.5, max_wait_seconds=10) for _ in range(3)]

        assert waits == [SlotReservation(True, 0.0), SlotReservation(True, 0.5), SlotReservation(True, 1.0)]

    def test_without_a_booking_a_wait_past_the_bound_writes_nothing(self, clock, state):
        state.reserve_send("gate", 1.0, max_wait_seconds=0)
        written = state.get("gate")

        assert state.reserve_send("gate", 1.0, max_wait_seconds=0.5) == SlotReservation(False, pytest.approx(1.0))
        assert state.get("gate") == written

    def test_it_shares_the_slot_pointer_of_reserve_slot(self, clock, state):
        """A process of the previous release reserves on the same key, so the two keep one order of slots."""
        state.reserve_slot("gate", 1.0, max_wait_seconds=0)

        assert state.reserve_send("gate", 1.0, max_wait_seconds=0) == SlotReservation(False, pytest.approx(1.0))

    def test_deferred_callers_each_book_a_slot_of_their_own(self, clock, state):
        results = [state.reserve_send("gate", 1.0, max_wait_seconds=0, booking=f"job-{n}") for n in range(100)]

        assert results[0] == SlotReservation(True, 0.0)  # the gate was idle, so the first one sends at once
        assert results[1:] == [SlotReservation(False, pytest.approx(float(n))) for n in range(1, 100)]

    def test_each_booked_caller_sends_once_when_it_comes_back_on_time(self, clock, state):
        for n in range(10):
            state.reserve_send("gate", 1.0, max_wait_seconds=0, booking=f"job-{n}")

        for n in range(1, 10):
            clock["seconds"] = float(n)
            assert state.reserve_send("gate", 1.0, max_wait_seconds=0, booking=f"job-{n}") == SlotReservation(True, 0.0)

    def test_a_booked_caller_back_before_its_slot_keeps_it_and_books_nothing(self, clock, state):
        state.reserve_send("gate", 1.0, max_wait_seconds=0)
        state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="early")
        clock["seconds"] += 0.25

        again = state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="early")

        assert again == SlotReservation(False, pytest.approx(0.75))
        assert state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="next") == SlotReservation(
            False, pytest.approx(1.75)
        )

    def test_a_booked_slot_within_the_wait_bound_is_waited_for(self, clock, state):
        state.reserve_send("gate", 1.0, max_wait_seconds=0)
        state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="b")

        assert state.reserve_send("gate", 1.0, max_wait_seconds=5, booking="b") == SlotReservation(True, 1.0)

    def test_a_used_booking_is_gone(self, clock, state):
        state.reserve_send("gate", 1.0, max_wait_seconds=0)
        state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="b")
        clock["seconds"] = 1.0
        state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="b")

        assert state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="b") == SlotReservation(
            False, pytest.approx(1.0)
        )

    def test_an_expired_booking_books_a_fresh_slot(self, clock, state):
        state.reserve_send("gate", 3600, max_wait_seconds=0)
        state.reserve_send("gate", 3600, max_wait_seconds=0, booking="lost")  # slot at 3600 s
        state.reserve_send("gate", 3600, max_wait_seconds=0, booking="kept")  # slot at 7200 s
        clock["seconds"] = 3600 + state_keys.to_seconds(state_keys.booking_ttl_microseconds(3600 * 1_000_000)) + 1

        assert state.reserve_send("gate", 3600, max_wait_seconds=0, booking="lost") == SlotReservation(
            False, pytest.approx(10_800 - clock["seconds"])
        )

    def test_booked_callers_back_late_together_still_send_an_interval_apart(self, clock, state):
        state.reserve_send("gate", 1.0, max_wait_seconds=0)
        for name in ("a", "b", "c"):
            state.reserve_send("gate", 1.0, max_wait_seconds=0, booking=name)
        clock["seconds"] = 10.0

        first = state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="a")
        second = state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="b")
        third = state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="c")

        assert first == SlotReservation(True, 0.0)
        assert second == SlotReservation(True, pytest.approx(1.0))  # waits at most one interval
        assert third == SlotReservation(False, pytest.approx(2.0))  # would wait longer: comes back, keeps its slot
        clock["seconds"] = 12.0
        assert state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="c") == SlotReservation(True, 0.0)

    def test_a_late_booked_send_holds_back_the_next_unbooked_one(self, clock, state):
        state.reserve_send("gate", 1.0, max_wait_seconds=0)
        state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="late")
        clock["seconds"] = 10.0
        state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="late")
        clock["seconds"] = 10.25

        assert state.reserve_send("gate", 1.0, max_wait_seconds=0) == SlotReservation(False, pytest.approx(0.75))
        assert state.reserve_send("gate", 1.0, max_wait_seconds=5) == SlotReservation(True, pytest.approx(0.75))

    def test_bookings_and_the_last_send_stay_out_of_the_callers_keys(self, clock, state):
        state.reserve_send("gate", 1.0, max_wait_seconds=0)
        state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="b")

        assert list(state.iter_keys("gate")) == ["gate"]

    def test_an_index_key_is_refused(self, state):
        state.index_add("gate", "a", 1.0, ttl_seconds=60)

        with pytest.raises(ValueError):
            state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="b")

    @pytest.mark.parametrize("booking", ["", "x" * (state_keys.MAX_BOOKING_LENGTH + 1)])
    def test_a_booking_that_is_empty_or_too_long_is_refused(self, state, booking):
        with pytest.raises(ValueError, match="booking"):
            state.reserve_send("gate", 1.0, max_wait_seconds=0, booking=booking)

        assert state.get("gate") is None

    @pytest.mark.parametrize("interval", [0, -1.0, math.inf, math.nan, 86_400.5])
    def test_an_interval_out_of_range_is_refused(self, state, interval):
        with pytest.raises(ValueError, match="interval_seconds"):
            state.reserve_send("gate", interval, max_wait_seconds=0)

    @pytest.mark.parametrize("max_wait", [-0.1, math.inf, math.nan])
    def test_a_max_wait_out_of_range_is_refused(self, state, max_wait):
        with pytest.raises(ValueError, match="max_wait_seconds"):
            state.reserve_send("gate", 1.0, max_wait_seconds=max_wait)


class TestSendConcurrency:
    """Concurrent deferred callers each book a distinct slot, and exactly one of them sends at once."""

    CALLERS = 8

    def _book_concurrently(self, states):
        results = []
        barrier = threading.Barrier(self.CALLERS)

        def book(state, name):
            barrier.wait()
            results.append(state.reserve_send("gate", 0.1, max_wait_seconds=0, booking=name))

        threads = [threading.Thread(target=book, args=(state, f"job-{n}")) for n, state in enumerate(states)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return sorted(results, key=lambda result: result.wait_seconds)

    def _assert_distinct(self, results):
        assert [result.reserved for result in results] == [True] + [False] * (self.CALLERS - 1)
        assert [result.wait_seconds for result in results] == pytest.approx([0.1 * n for n in range(self.CALLERS)])

    def test_threads_sharing_a_memory_backend(self, clock):
        self._assert_distinct(self._book_concurrently([MemoryState()] * self.CALLERS))

    def test_connections_sharing_a_redis_server(self, clock):
        server = fakeredis.FakeServer()
        states = [RedisState(fakeredis.FakeStrictRedis(server=server)) for _ in range(self.CALLERS)]

        self._assert_distinct(self._book_concurrently(states))


class TestRedisSlotLayout:
    @pytest.fixture
    def client(self):
        return fakeredis.FakeStrictRedis(decode_responses=False)

    def test_the_gate_expires_one_interval_after_its_last_slot(self, clock, client):
        state = RedisState(client)
        state.reserve_slot("gate", 2.0, max_wait_seconds=10)
        state.reserve_slot("gate", 2.0, max_wait_seconds=10)

        assert client.pttl("gate") == 4_000

    def test_the_slot_is_read_from_the_server_clock(self, clock, client, monkeypatch):
        """Replicas with skewed clocks must agree on one gate, so the script reads Redis ``TIME``, not the caller's."""
        monkeypatch.setattr("jasil.backends.state_redis.time", None)  # the backend module must not read its clock

        assert RedisState(client).reserve_slot("gate", 1.0, max_wait_seconds=0) == SlotReservation(True, 0.0)
        assert client.get("gate") == b"1800000000000000"


class TestRedisSendLayout:
    @pytest.fixture
    def client(self):
        return fakeredis.FakeStrictRedis(decode_responses=False)

    def test_a_booking_is_kept_under_the_reserved_namespace_for_its_wait_and_a_margin(self, clock, client):
        state = RedisState(client)
        state.reserve_send("gate", 100.0, max_wait_seconds=0)
        state.reserve_send("gate", 100.0, max_wait_seconds=0, booking="job-1")

        key = state_keys.booking_key("gate", "job-1")
        assert key.startswith(state_keys.GATE_PREFIX)
        assert client.get(key) == b"1800000100000000"
        assert client.pttl(key) == math.ceil(state_keys.booking_ttl_microseconds(100 * 1_000_000) / 1000)

    def test_the_last_send_expires_one_interval_after_it(self, clock, client):
        state = RedisState(client)
        state.reserve_send("gate", 2.0, max_wait_seconds=0)

        assert client.pttl(state_keys.last_send_key("gate")) == 2_000

    def test_the_booked_slot_is_read_from_the_server_clock(self, clock, client, monkeypatch):
        monkeypatch.setattr("jasil.backends.state_redis.time", None)  # the backend module must not read its clock
        state = RedisState(client)
        state.reserve_send("gate", 1.0, max_wait_seconds=0)

        assert state.reserve_send("gate", 1.0, max_wait_seconds=0, booking="b") == SlotReservation(False, 1.0)


class TestRedisIndexLayout:
    """An index is a sorted set of scores plus a sorted set of deadlines under a reserved prefix."""

    @pytest.fixture
    def client(self):
        return fakeredis.FakeStrictRedis(decode_responses=False)

    def test_both_keys_expire_with_the_latest_member(self, clock, client):
        state = RedisState(client)
        state.index_add("idx", "a", 1.0, ttl_seconds=10)
        state.index_add("idx", "b", 2.0, ttl_seconds=100)

        assert client.pttl("idx") == 100_000
        assert client.pttl("jasil:state:index-expiry:idx") == 100_000

    def test_removing_the_latest_member_rearms_both_keys(self, clock, client):
        state = RedisState(client)
        state.index_add("idx", "a", 1.0, ttl_seconds=10)
        state.index_add("idx", "b", 2.0, ttl_seconds=100)
        clock["seconds"] += 4

        state.index_remove("idx", "b")

        assert client.pttl("idx") == 6_000
        assert client.pttl("jasil:state:index-expiry:idx") == 6_000

    def test_a_read_drops_expired_members_from_both_keys(self, clock, client):
        state = RedisState(client)
        state.index_add("idx", "a", 1.0, ttl_seconds=10)
        state.index_add("idx", "b", 2.0, ttl_seconds=100)
        clock["seconds"] += 11

        state.index_range("idx", limit=10)

        assert client.zrange("idx", 0, -1) == [b"b"]
        assert client.zrange("jasil:state:index-expiry:idx", 0, -1) == [b"b"]

    def test_deadlines_an_index_left_behind_are_discarded(self, client):
        state = RedisState(client)
        state.index_add("idx", "a", 1.0, ttl_seconds=100)
        state.delete("idx")

        state.index_add("idx", "b", 2.0, ttl_seconds=10)

        assert client.zrange("jasil:state:index-expiry:idx", 0, -1) == [b"b"]


class TestMemoryIndexSweep:
    def test_a_write_sweeps_index_members_that_expired_unread(self, clock):
        state = MemoryState()
        state.index_add("idx", "a", 1.0, ttl_seconds=10)

        clock["seconds"] += 61
        state.set("other", b"1")

        assert state._indexes == {}


class TestRedisTtlIsArmed:
    @pytest.fixture
    def client(self):
        return fakeredis.FakeStrictRedis(decode_responses=False)

    def test_incr_arms_the_expiry_when_asked(self, client):
        RedisState(client).incr("counter", ttl_seconds=60)

        assert 0 < client.ttl("counter") <= 60

    def test_incr_without_a_ttl_leaves_the_key_persistent(self, client):
        RedisState(client).incr("counter")

        assert client.ttl("counter") == -1

    @pytest.mark.parametrize(("stored", "amount"), [(b"abc", 1), (str(2**63 - 1).encode(), 1)])
    @pytest.mark.parametrize("expiry", [None, 600])
    def test_a_refused_incr_leaves_the_expiry_as_it_was(self, client, stored, amount, expiry):
        """A refused increment leaves the counter unchanged, as on the memory backend: its expiry too."""
        client.set("counter", stored, ex=expiry)

        with pytest.raises(ValueError):
            RedisState(client).incr("counter", amount, ttl_seconds=60)

        assert client.get("counter") == stored
        if expiry is None:
            assert client.ttl("counter") == -1
        else:
            assert 60 < client.ttl("counter") <= 600  # some of the 600 seconds may have elapsed; 60 would be re-armed

    def test_set_if_absent_arms_the_expiry_when_asked(self, client):
        RedisState(client).set_if_absent("lock", b"owner", ttl_seconds=60)

        assert 0 < client.ttl("lock") <= 60


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

    def test_delete_prefix_counts_only_the_keys_that_were_still_alive(self, clock):
        """Redis never lists an expired key, so the count must not include one the sweep has not dropped yet."""
        state = MemoryState()
        state.set("p:gone", b"x", ttl_seconds=10)
        state.set("p:kept", b"x")

        clock["now"] += 11

        assert state.delete_prefix("p:") == 1
        assert state.get("p:kept") is None

    @pytest.mark.parametrize("stored", [b" 5", b"1_0", b"5 ", b"+5", b"05", b"\xd9\xa5", b"1.5", b""])
    def test_incr_refuses_what_redis_would_not_parse(self, stored):
        state = MemoryState()
        state.set("k", stored)

        with pytest.raises(ValueError, match="not an integer"):
            state.incr("k")

    def test_incr_parses_negative_integers(self):
        state = MemoryState()
        state.set("k", b"-3")

        assert state.incr("k", 5) == 2

    def test_incr_preserves_an_existing_expiry(self, clock):
        """Re-arming the TTL on every increment would let a steady stream of
        failures keep a rate-limit window alive forever."""
        state = MemoryState()
        state.incr("counter", ttl_seconds=10)

        state.incr("counter")
        clock["now"] += 11

        assert state.get("counter") is None

    def test_an_elapsed_lockout_lifts_and_can_lock_again(self, monkeypatch):
        """The gate is wall-clock; once it passes the caller is let back in, and
        the surviving counter means the next failure re-locks at once."""
        state = MemoryState()
        tiers = ((1, 60),)
        locked = state.record_tiered_failure("counter", "gate", tiers, 900)
        assert locked.newly_locked is True
        real_time = time.time
        monkeypatch.setattr("jasil.backends.state_memory.time.time", lambda: real_time() + 61)

        outcome = state.record_tiered_failure("counter", "gate", tiers, 900)

        assert outcome.count == 2
        assert outcome.newly_locked is True
        assert outcome.locked_until_epoch > locked.locked_until_epoch

    def test_a_write_sweeps_keys_that_expired_unread(self, clock):
        """Rate-limit keys are written once and never read again; left in place they would grow without bound."""
        state = MemoryState()
        for n in range(100):
            state.set(f"rate:{n}", b"1", ttl_seconds=10)
        state.set("kept", b"1")

        clock["now"] += 61
        state.set("new", b"1")

        assert sorted(state._data) == ["kept", "new"]

    def test_the_sweep_runs_at_most_once_a_minute(self, clock):
        state = MemoryState()
        clock["now"] += 61
        state.set("first", b"1")  # sweeps an empty store, and starts the next minute
        state.set("short", b"1", ttl_seconds=1)

        clock["now"] += 2
        state.set("second", b"1")

        assert "short" in state._data

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

    def test_incrementing_with_a_ttl_never_sends_a_separate_expire(self):
        """INCRBY and EXPIRE travel together: a failure between two commands would leave a counter that never expires."""
        sent: list[str] = []

        class _Spy(fakeredis.FakeStrictRedis):
            def expire(self, *args, **kwargs):
                sent.append("expire")
                return super().expire(*args, **kwargs)

            def incrby(self, *args, **kwargs):
                sent.append("incrby")
                return super().incrby(*args, **kwargs)

        client = _Spy(decode_responses=False)

        assert RedisState(client).incr("k", ttl_seconds=30) == 1
        assert sent == []
        assert 0 < client.ttl("k") <= 30

    def test_scanning_lists_a_key_once_and_skips_one_that_is_not_text(self):
        class _Repeating(fakeredis.FakeStrictRedis):
            def scan_iter(self, *args, **kwargs):
                yield from list(super().scan_iter(*args, **kwargs)) * 2

        client = _Repeating(decode_responses=False)
        client.set("p:a", b"1")
        client.set(b"p:\xff\xfe", b"1")

        assert sorted(RedisState(client).iter_keys("p:")) == ["p:a"]

    def test_a_key_holding_another_type_is_a_value_error_not_an_outage(self):
        """Only a server that cannot serve is unavailable; a refused value is the caller's problem."""
        client = fakeredis.FakeStrictRedis(decode_responses=False)
        client.lpush("k", b"x")

        with pytest.raises(ValueError, match="WRONGTYPE"):
            RedisState(client).get("k")

    def test_a_server_condition_is_still_an_outage(self):
        from redis.exceptions import ReadOnlyError

        class _Replica(fakeredis.FakeStrictRedis):
            def set(self, *args, **kwargs):
                raise ReadOnlyError("READONLY You can't write against a read only replica.")

        with pytest.raises(StateBackendUnavailableError):
            RedisState(_Replica(decode_responses=False)).set("k", b"v")

    @pytest.mark.parametrize(
        "message",
        [
            "MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk.",
            "BUSY Redis is busy running a script.",
            "NOREPLICAS Not enough good replicas to write.",
        ],
    )
    def test_a_server_condition_redis_py_leaves_unclassified_is_an_outage(self, message):
        """redis-py gives these the plain ``ResponseError``; they say nothing about the key's value."""
        from redis.exceptions import ResponseError

        class _Refusing(fakeredis.FakeStrictRedis):
            def set(self, *args, **kwargs):
                raise ResponseError(message)

        with pytest.raises(StateBackendUnavailableError):
            RedisState(_Refusing(decode_responses=False)).set("k", b"v")

    def test_incrementing_a_value_that_is_not_an_integer_is_a_value_error(self):
        client = fakeredis.FakeStrictRedis(decode_responses=False)
        client.set("k", b"text")

        with pytest.raises(ValueError, match="not an integer"):
            RedisState(client).incr("k")

    def test_the_memory_backend_never_raises_the_provider_error(self):
        assert MemoryState().get("absent") is None
