"""In-process ``StateProvider`` backend."""

import threading
import time
from collections.abc import Iterator

from jasil.providers import TieredFailureOutcome

#: How often a write also drops every expired key. Expiry is otherwise lazy, on read, and a key that is never
#: read again (a rate-limit counter for a client that left) would stay in memory for the life of the process.
SWEEP_INTERVAL_SECONDS = 60


class MemoryState:
    """``StateProvider`` backed by a process-local dict with per-key TTL expiry.

    An expired key is dropped when it is read, and every write sweeps the expired
    keys at most once per :data:`SWEEP_INTERVAL_SECONDS`.

    Correct for the ``local`` profile (single process). Not shared across
    workers/replicas — the deployment fail-fast rejects using it under a
    distributed or multi-worker profile. Access is guarded by a lock because
    FastAPI runs sync handlers in a threadpool.
    """

    def __init__(self) -> None:
        self._data: dict[str, tuple[bytes, float | None]] = {}
        self._lock = threading.Lock()
        self._next_sweep = time.monotonic() + SWEEP_INTERVAL_SECONDS

    def _sweep_if_due(self) -> None:
        """Drop every expired key, at most once per :data:`SWEEP_INTERVAL_SECONDS`; call under the lock."""
        now = time.monotonic()
        if now < self._next_sweep:
            return
        self._next_sweep = now + SWEEP_INTERVAL_SECONDS
        for key in [key for key, (_value, expiry) in self._data.items() if expiry is not None and expiry <= now]:
            del self._data[key]

    @staticmethod
    def _is_expired(expiry: float | None) -> bool:
        return expiry is not None and expiry <= time.monotonic()

    def _live_value(self, key: str) -> bytes | None:
        """Return the unexpired value for *key*, evicting it if it has expired."""
        entry = self._data.get(key)
        if entry is None:
            return None
        value, expiry = entry
        if self._is_expired(expiry):
            del self._data[key]
            return None
        return value

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
            self._data[key] = (value, expiry)

    def delete(self, key: str) -> None:
        """Delete ``key`` if present."""
        with self._lock:
            self._data.pop(key, None)

    def incr(self, key: str, amount: int = 1, ttl_seconds: int | None = None) -> int:
        """Add ``amount`` to the stored integer; keep the existing expiry unless ``ttl_seconds`` is given."""
        _check_ttl(ttl_seconds)
        with self._lock:
            self._sweep_if_due()
            current_bytes = self._live_value(key)
            current = int(current_bytes.decode()) if current_bytes is not None else 0
            new_value = current + amount
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
            if self._live_value(key) is not None:
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

    def delete_prefix(self, prefix: str) -> int:
        """Delete every key starting with ``prefix``; ``prefix`` must not be empty."""
        _check_prefix(prefix)
        with self._lock:
            matching = [key for key in self._data if key.startswith(prefix)]
            for key in matching:
                del self._data[key]
            return len(matching)

    def iter_keys(self, prefix: str) -> Iterator[str]:
        """Return an iterator over a snapshot of the unexpired keys starting with ``prefix``."""
        with self._lock:
            # Snapshot live matching keys under the lock; ``list(self._data)`` guards
            # against the eviction that ``_live_value`` performs while scanning.
            live_keys = [
                key for key in list(self._data) if key.startswith(prefix) and self._live_value(key) is not None
            ]
        return iter(live_keys)

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
                gate_until = int(gate_bytes.decode())
                if gate_until > now:
                    counter_bytes = self._live_value(counter_key)
                    count = int(counter_bytes.decode()) if counter_bytes is not None else 0
                    return TieredFailureOutcome(count, gate_until, False)
                self._data.pop(gate_key, None)  # expired gate

            counter_bytes = self._live_value(counter_key)
            count = (int(counter_bytes.decode()) if counter_bytes is not None else 0) + 1
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


def _check_prefix(prefix: str) -> None:
    if not prefix:
        raise ValueError("delete_prefix needs a non-empty prefix")
