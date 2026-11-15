"""In-process ``StateProvider`` backend."""

import heapq
import math
import re
import threading
import time
from collections.abc import Iterator

import jasil._core.state_keys as state_keys
from jasil.providers import TieredFailureOutcome

#: An index: each member's score and its expiry deadline on the monotonic clock.
type _Index = dict[str, tuple[float, float]]

_WRONG_TYPE = "the key holds an index, not a plain value"
_NOT_AN_INDEX = "the key holds a plain value, not an index"

#: How often a write also drops every expired key. Expiry is otherwise lazy, on read, and a key that is never
#: read again (a rate-limit counter for a client that left) would stay in memory for the life of the process.
SWEEP_INTERVAL_SECONDS = 60


# What Redis accepts as an integer: an optional minus sign and no leading zeros, with nothing around it.
_INTEGER = re.compile(rb"-?(?:0|[1-9][0-9]*)")
# Redis counters are signed 64-bit integers.
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1


def _parse_integer(raw: bytes) -> int:
    """Read a stored counter as Redis does, refusing what ``int()`` would let through (spaces, underscores).

    A value outside the signed 64-bit range is refused too, as Redis refuses it.
    """
    if _INTEGER.fullmatch(raw) is None or not _INT64_MIN <= (value := int(raw)) <= _INT64_MAX:
        raise ValueError("value is not an integer or out of range")
    return value


class MemoryState:
    """``StateProvider`` backed by a process-local dict with per-key TTL expiry.

    An expired key or index member is dropped when it is read, and every write sweeps
    the expired ones at most once per :data:`SWEEP_INTERVAL_SECONDS`. Indexes live in
    a dict of their own; a key is never in both.

    Correct for the ``local`` profile (single process). Not shared across
    workers/replicas — the deployment fail-fast rejects using it under a
    distributed or multi-worker profile. Access is guarded by a lock because
    FastAPI runs sync handlers in a threadpool.
    """

    def __init__(self) -> None:
        self._data: dict[str, tuple[bytes, float | None]] = {}
        self._indexes: dict[str, _Index] = {}
        self._lock = threading.Lock()
        self._next_sweep = time.monotonic() + SWEEP_INTERVAL_SECONDS

    def _sweep_if_due(self) -> None:
        """Drop expired keys and index members, at most once per :data:`SWEEP_INTERVAL_SECONDS`; call under the lock."""
        now = time.monotonic()
        if now < self._next_sweep:
            return
        self._next_sweep = now + SWEEP_INTERVAL_SECONDS
        for key in [key for key, (_value, expiry) in self._data.items() if expiry is not None and expiry <= now]:
            del self._data[key]
        for key in list(self._indexes):
            self._live_index(key)

    @staticmethod
    def _is_expired(expiry: float | None) -> bool:
        return expiry is not None and expiry <= time.monotonic()

    def _stored_value(self, key: str) -> bytes | None:
        """Return the unexpired plain value for *key*, evicting it if it has expired."""
        entry = self._data.get(key)
        if entry is None:
            return None
        value, expiry = entry
        if self._is_expired(expiry):
            del self._data[key]
            return None
        return value

    def _live_value(self, key: str) -> bytes | None:
        """Return the unexpired plain value for *key*, as :meth:`_stored_value`, refusing a key that holds an index.

        Raises:
            ValueError: When an index with a live member is stored under *key*.
        """
        if self._live_index(key) is not None:
            raise ValueError(_WRONG_TYPE)
        return self._stored_value(key)

    def _live_index(self, key: str) -> _Index | None:
        """Return the index under *key* without its expired members, evicting it once none is left."""
        index = self._indexes.get(key)
        if index is None:
            return None
        now = time.monotonic()
        for member in [member for member, (_score, expiry) in index.items() if expiry <= now]:
            del index[member]
        if not index:
            del self._indexes[key]
            return None
        return index

    def _index_for(self, key: str) -> _Index | None:
        """Return the live index under *key*, refusing a key that holds a plain value.

        Raises:
            ValueError: When a live plain value is stored under *key*.
        """
        if self._stored_value(key) is not None:
            raise ValueError(_NOT_AN_INDEX)
        return self._live_index(key)

    def get(self, key: str) -> bytes | None:
        """Return the value under ``key``, or ``None`` when absent or expired."""
        with self._lock:
            return self._live_value(key)

    def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
        """Store ``value`` under ``key``, expiring after ``ttl_seconds`` when given."""
        _check_ttl(ttl_seconds)
        with self._lock:
            self._sweep_if_due()
            expiry = time.monotonic() + ttl_seconds if ttl_seconds is not None else None
            self._indexes.pop(key, None)  # a plain value replaces an index, as Redis SET does
            self._data[key] = (value, expiry)

    def delete(self, key: str) -> None:
        """Delete ``key`` if present, whether it holds a plain value or an index."""
        with self._lock:
            self._data.pop(key, None)
            self._indexes.pop(key, None)

    def incr(self, key: str, amount: int = 1, ttl_seconds: int | None = None) -> int:
        """Add ``amount`` to the stored integer; keep the existing expiry unless ``ttl_seconds`` is given."""
        _check_ttl(ttl_seconds)
        if not _INT64_MIN <= amount <= _INT64_MAX:
            raise ValueError("value is not an integer or out of range")
        with self._lock:
            self._sweep_if_due()
            current_bytes = self._live_value(key)
            current = _parse_integer(current_bytes) if current_bytes is not None else 0
            new_value = current + amount
            if not _INT64_MIN <= new_value <= _INT64_MAX:
                raise ValueError("increment or decrement would overflow")
            if ttl_seconds is not None:
                expiry: float | None = time.monotonic() + ttl_seconds
            elif current_bytes is not None:
                expiry = self._data[key][1]  # preserve the existing expiry
            else:
                expiry = None
            self._data[key] = (str(new_value).encode(), expiry)
            return new_value

    def set_if_absent(self, key: str, value: bytes, ttl_seconds: int | None = None) -> bool:
        """Store ``value`` only when no unexpired value exists; return True when stored."""
        _check_ttl(ttl_seconds)
        with self._lock:
            self._sweep_if_due()
            # A live index takes the key, as any existing key refuses Redis SET NX.
            if self._live_index(key) is not None or self._stored_value(key) is not None:
                return False
            expiry = time.monotonic() + ttl_seconds if ttl_seconds is not None else None
            self._data[key] = (value, expiry)
            return True

    def get_and_delete(self, key: str) -> bytes | None:
        """Return the unexpired value under ``key`` and delete it."""
        with self._lock:
            value = self._live_value(key)
            if value is not None:
                del self._data[key]
            return value

    def delete_if_equals(self, key: str, expected: bytes) -> bool:
        """Delete ``key`` only while its unexpired value equals ``expected``; return True when deleted."""
        with self._lock:
            if self._live_value(key) != expected:
                return False
            del self._data[key]
            return True

    def expire_if_equals(self, key: str, expected: bytes, ttl_seconds: int) -> bool:
        """Restart the expiry of ``key`` only while its unexpired value equals ``expected``; return True when restarted."""
        _check_ttl(ttl_seconds)
        with self._lock:
            if self._live_value(key) != expected:
                return False
            self._data[key] = (expected, time.monotonic() + ttl_seconds)
            return True

    def delete_prefix(self, prefix: str) -> int:
        """Delete every key, plain or index, starting with ``prefix``; ``prefix`` must not be empty."""
        state_keys.check_delete_prefix(prefix)
        with self._lock:
            # An expired key the sweep has not dropped yet is not a key any more: Redis never counts it.
            matching = [key for key in self._data if key.startswith(prefix)]
            deleted = sum(1 for key in matching if self._stored_value(key) is not None)
            for key in matching:
                self._data.pop(key, None)
            matching = [key for key in self._indexes if key.startswith(prefix)]
            deleted += sum(1 for key in matching if self._live_index(key) is not None)
            for key in matching:
                self._indexes.pop(key, None)
            return deleted

    def iter_keys(self, prefix: str) -> Iterator[str]:
        """Return an iterator over a snapshot of the unexpired keys, plain or index, starting with ``prefix``."""
        with self._lock:
            # Snapshot live matching keys under the lock; the ``list()`` copies guard against the
            # eviction that ``_stored_value`` and ``_live_index`` perform while scanning.
            live_keys = [
                key for key in list(self._data) if key.startswith(prefix) and self._stored_value(key) is not None
            ]
            live_keys += [
                key for key in list(self._indexes) if key.startswith(prefix) and self._live_index(key) is not None
            ]
        return iter(live_keys)

    def index_add(self, key: str, member: str, score: float, ttl_seconds: int) -> None:
        """Insert or replace ``member`` with ``score``, expiring ``ttl_seconds`` from now."""
        _check_ttl(ttl_seconds)
        _check_score(score)
        with self._lock:
            self._sweep_if_due()
            index = self._index_for(key)
            if index is None:
                index = self._indexes[key] = {}
            index[member] = (float(score), time.monotonic() + ttl_seconds)

    def index_remove(self, key: str, member: str) -> None:
        """Remove ``member`` from the index, dropping the index once it is empty."""
        with self._lock:
            index = self._index_for(key)
            if index is not None:
                index.pop(member, None)
                if not index:
                    del self._indexes[key]

    def index_range(self, key: str, *, limit: int, before: tuple[float, str] | None = None) -> list[tuple[str, float]]:
        """Return up to ``limit`` live members after ``before``, highest ``(score, member)`` first; O(n) in the index."""
        _check_limit(limit)
        if before is not None:
            _check_score(before[0])
        with self._lock:
            index = self._index_for(key)
            if index is None:
                return []
            entries = [(score, member) for member, (score, _expiry) in index.items()]
        if before is not None:
            entries = [entry for entry in entries if entry < before]
        return [(member, score) for score, member in heapq.nlargest(limit, entries)]

    def record_tiered_failure(
        self,
        counter_key: str,
        gate_key: str,
        tiers: tuple[tuple[int, int], ...],
        counter_ttl_seconds: int,
    ) -> TieredFailureOutcome:
        """Count one occurrence and close the gate, atomically under the instance lock."""
        now = int(time.time())
        with self._lock:
            self._sweep_if_due()
            # Already locked: return the current count without incrementing, so a
            # locked-out caller cannot keep inflating the counter.
            gate_bytes = self._live_value(gate_key)
            if gate_bytes is not None:
                gate_until = _parse_integer(gate_bytes)
                if gate_until > now:
                    counter_bytes = self._live_value(counter_key)
                    count = _parse_integer(counter_bytes) if counter_bytes is not None else 0
                    return TieredFailureOutcome(count, gate_until, False)
                self._data.pop(gate_key, None)  # expired gate

            counter_bytes = self._live_value(counter_key)
            count = (_parse_integer(counter_bytes) if counter_bytes is not None else 0) + 1
            if count > _INT64_MAX:
                raise ValueError("increment or decrement would overflow")
            self._data[counter_key] = (str(count).encode(), time.monotonic() + counter_ttl_seconds)

            lock_seconds = 0
            for threshold, tier_lock_seconds in tiers:  # ascending; last match wins
                if count >= threshold:
                    lock_seconds = tier_lock_seconds

            if lock_seconds > 0:
                gate_until = now + lock_seconds
                self._data[gate_key] = (str(gate_until).encode(), time.monotonic() + lock_seconds)
                return TieredFailureOutcome(count, gate_until, True)
            return TieredFailureOutcome(count, None, False)


def _check_ttl(ttl_seconds: int | None) -> None:
    if ttl_seconds is not None and ttl_seconds <= 0:
        raise ValueError(f"ttl_seconds must be positive, or None for no expiry, not {ttl_seconds}")


def _check_score(score: float) -> None:
    if not math.isfinite(score):
        raise ValueError(f"an index score must be a finite number, not {score}")


def _check_limit(limit: int) -> None:
    if limit < 1:
        raise ValueError(f"limit must be at least 1, not {limit}")
