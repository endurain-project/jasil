"""Async in-process ``AsyncStateProvider`` backend.

This is the awaitable twin of :mod:`jasil.backends.state_memory`, following the
rule that every async backend mirrors its synchronous counterpart
method-for-method and name-for-name, with only the execution model changed.

**Why asyncio.Lock instead of threading.Lock?**  The sync backend uses
:class:`threading.Lock` because FastAPI runs sync handlers in a threadpool, so
multiple OS threads can race on the dict. An async handler, by contrast, runs
on the event loop — only one coroutine executes Python bytecode at a time, so
most operations on the dict are already atomic from the scheduler's point of
view. We keep :class:`asyncio.Lock` anyway, for two reasons. First, a
multi-step read-modify-write (``record_tiered_failure``, ``incr``,
``set_if_absent``, ``get_and_delete``) spans multiple bytecode instructions;
without a lock, an ``await`` — even one injected by a debug tool or a future
refactor — would open a race. Second, the lock makes the contract identical to
the sync backend so a reader of one file does not have to reason about whether
the other is thread-safe: they are both explicitly guarded.

``iter_keys`` returns an :class:`~collections.abc.AsyncIterator` as required by
:class:`~jasil.providers_async.AsyncStateProvider`.  The snapshot approach from
the sync backend (materialise under the lock, iterate outside it) translates
directly: we acquire the lock, collect the live keys, release it, then yield
from the snapshot.  No ``await`` can interleave inside the snapshot because we
hold the lock for that entire phase.
"""

import asyncio
import time
from collections.abc import AsyncIterator

from jasil.providers import TieredFailureOutcome


class AsyncMemoryState:
    """``AsyncStateProvider`` backed by a process-local dict with per-key TTL expiry.

    Correct for the ``local`` profile (single process, async host). Not shared
    across workers/replicas — the deployment fail-fast rejects using it under a
    distributed or multi-worker profile. Access is guarded by an
    :class:`asyncio.Lock` to make compound operations (``incr``,
    ``record_tiered_failure``, etc.) atomic across coroutine yields.
    """

    def __init__(self) -> None:
        self._data: dict[str, tuple[bytes, float | None]] = {}
        # asyncio.Lock must be created inside the event loop to avoid a
        # DeprecationWarning on Python 3.10+ and an outright error on 3.12+.
        self._lock = asyncio.Lock()

    @staticmethod
    def _is_expired(expiry: float | None) -> bool:
        return expiry is not None and expiry <= time.monotonic()

    def _live_value(self, key: str) -> bytes | None:
        """Return the unexpired value for *key*, evicting it if it has expired.

        Must only be called while the caller holds ``self._lock``.
        """
        entry = self._data.get(key)
        if entry is None:
            return None
        value, expiry = entry
        if self._is_expired(expiry):
            del self._data[key]
            return None
        return value

    async def get(self, key: str) -> bytes | None:
        """Return the value stored at *key*, or ``None`` if absent or expired.

        Args:
            key: The state key to look up.

        Returns:
            The stored bytes, or ``None``.
        """
        async with self._lock:
            return self._live_value(key)

    async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
        """Store *value* at *key*, optionally with a TTL.

        Args:
            key: The state key to write.
            value: Raw bytes to store.
            ttl_seconds: Seconds until the key expires; ``None`` means no expiry.

        Returns:
            None.
        """
        async with self._lock:
            expiry = time.monotonic() + ttl_seconds if ttl_seconds is not None else None
            self._data[key] = (value, expiry)

    async def delete(self, key: str) -> None:
        """Remove *key* from the store; no-op if absent.

        Args:
            key: The state key to remove.

        Returns:
            None.
        """
        async with self._lock:
            self._data.pop(key, None)

    async def incr(self, key: str, amount: int = 1, ttl_seconds: int | None = None) -> int:
        """Increment the integer stored at *key* by *amount*, returning the new value.

        If the key is absent or expired it is treated as zero. The TTL is only
        (re)set when *ttl_seconds* is explicitly provided; otherwise an existing
        expiry is preserved, mirroring the sync backend.

        Args:
            key: The state key to increment.
            amount: How much to add; defaults to 1.
            ttl_seconds: New TTL to apply after the increment; ``None`` preserves
                any existing expiry and sets none on a new key.

        Returns:
            The new integer value.
        """
        async with self._lock:
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

    async def set_if_absent(self, key: str, value: bytes, ttl_seconds: int | None = None) -> bool:
        """Store *value* at *key* only if the key is absent or expired.

        Args:
            key: The state key to conditionally write.
            value: Raw bytes to store on success.
            ttl_seconds: TTL to apply if the key is written; ``None`` means no expiry.

        Returns:
            ``True`` if the value was stored, ``False`` if the key already existed.
        """
        async with self._lock:
            if self._live_value(key) is not None:
                return False
            expiry = time.monotonic() + ttl_seconds if ttl_seconds is not None else None
            self._data[key] = (value, expiry)
            return True

    async def get_and_delete(self, key: str) -> bytes | None:
        """Return the value at *key* and atomically remove it.

        Args:
            key: The state key to retrieve and delete.

        Returns:
            The stored bytes, or ``None`` if the key was absent or expired.
        """
        async with self._lock:
            value = self._live_value(key)
            if value is not None:
                del self._data[key]
            return value

    async def delete_prefix(self, prefix: str) -> int:
        """Delete all keys that start with *prefix*.

        Args:
            prefix: The key prefix to match for deletion.

        Returns:
            Number of keys deleted.
        """
        async with self._lock:
            matching = [key for key in self._data if key.startswith(prefix)]
            for key in matching:
                del self._data[key]
            return len(matching)

    async def iter_keys(self, prefix: str) -> AsyncIterator[str]:  # type: ignore[override]
        """Yield all live keys whose names start with *prefix*.

        The snapshot is materialised under the lock (so concurrent modifications
        do not affect it), then yielded outside the lock so the caller's
        ``async for`` body can itself call state methods without deadlocking.

        Args:
            prefix: Only keys that start with this string are yielded.

        Returns:
            An async iterator of matching key strings.
        """
        async with self._lock:
            # Snapshot under the lock; ``list(self._data)`` guards against the
            # eviction that ``_live_value`` performs while scanning.
            live_keys = [
                key for key in list(self._data) if key.startswith(prefix) and self._live_value(key) is not None
            ]

        # Yield outside the lock: the snapshot is already complete and holding
        # the lock across yields would block every concurrent state operation
        # for the entire iteration.
        for key in live_keys:
            yield key

    async def record_tiered_failure(
        self,
        counter_key: str,
        gate_key: str,
        tiers: tuple[tuple[int, int], ...],
        counter_ttl_seconds: int,
    ) -> TieredFailureOutcome:
        """Record a failure and return the current lockout outcome.

        Mirrors the sync backend's tiered-failure semantics exactly: if the gate
        is active the counter is *not* incremented (so a locked-out caller cannot
        keep inflating it), an expired gate is cleared, otherwise the counter is
        bumped and the highest crossed tier sets the gate.

        Args:
            counter_key: State key holding the running failure count.
            gate_key: State key holding the lockout-expiry timestamp.
            tiers: Ascending ``(threshold, lock_seconds)`` pairs; last match wins.
            counter_ttl_seconds: TTL to apply to the counter key.

        Returns:
            A :class:`~jasil.providers.TieredFailureOutcome` describing the
            current count, gate expiry, and whether this call newly locked the key.
        """
        now = int(time.time())
        async with self._lock:
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
