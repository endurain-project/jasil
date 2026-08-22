"""Async twins of the platform providers — the awaitable face of every capability.

Pure module, exactly like :mod:`jasil.providers`: only stdlib typing and the
event envelope. No infrastructure (``redis.asyncio`` / ``aioboto3`` / sqlalchemy)
and no domain imports, so any module can depend on these providers without
pulling in a backend. Concrete backends live in ``jasil.backends`` and are
selected by the async composition root
(``jasil.container_async.build_async_platform``).

**These are twins, not replacements.** Every protocol here mirrors its
synchronous counterpart method-for-method and name-for-name; only the return
types differ. The synchronous providers are unchanged and remain the right
choice for a blocking host — a process may even hold both platforms at once (a
sync worker beside an async API). What must never happen is a *single* provider
that decides which it is at call time: a method that sometimes returns a value
and sometimes an awaitable makes both faces worse, so the two hierarchies stay
disjoint.

:class:`~jasil.providers.ClockProvider` has no twin here. It performs no I/O, so
an awaitable ``now()`` would buy nothing and cost every caller an ``await``; the
async platform carries the same synchronous clock.

The shared value types — :class:`~jasil.providers.TieredFailureOutcome`,
:class:`~jasil.providers.GeocodedPlace`, and
:class:`~jasil.providers.StateBackendUnavailableError` — are deliberately *not*
duplicated. They are plain data and a plain exception, identical under either
execution model, and re-exported here only so an async host has one import.
"""

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Protocol, runtime_checkable

from jasil.events import Event
from jasil.providers import GeocodedPlace, StateBackendUnavailableError, TieredFailureOutcome

__all__ = [
    "AsyncEventBusProvider",
    "AsyncEventRecorder",
    "AsyncGeocodingProvider",
    "AsyncLockProvider",
    "AsyncStateProvider",
    "AsyncStorageProvider",
    "GeocodedPlace",
    "StateBackendUnavailableError",
    "TieredFailureOutcome",
]


@runtime_checkable
class AsyncStateProvider(Protocol):
    """Ephemeral keyed state (counters, TTL flags, small blobs), awaitable.

    The async twin of :class:`~jasil.providers.StateProvider`, with the same
    semantics for every method — including the atomic primitives
    (``set_if_absent``, ``get_and_delete``, ``record_tiered_failure``) that
    login-throttling and single-use-token stores depend on for correctness.

    ``iter_keys`` is the one shape that changes beyond the ``await``: it returns
    an :class:`~collections.abc.AsyncIterator` rather than a plain iterator, so a
    backend can page a large keyspace without materialising it. Callers consume
    it with ``async for``.
    """

    async def get(self, key: str) -> bytes | None: ...
    async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None: ...
    async def set_if_absent(self, key: str, value: bytes, ttl_seconds: int | None = None) -> bool: ...
    async def get_and_delete(self, key: str) -> bytes | None: ...
    async def delete(self, key: str) -> None: ...
    async def delete_prefix(self, prefix: str) -> int: ...
    def iter_keys(self, prefix: str) -> AsyncIterator[str]: ...
    async def incr(self, key: str, amount: int = 1, ttl_seconds: int | None = None) -> int: ...
    async def record_tiered_failure(
        self,
        counter_key: str,
        gate_key: str,
        tiers: tuple[tuple[int, int], ...],
        counter_ttl_seconds: int,
    ) -> TieredFailureOutcome: ...


@runtime_checkable
class AsyncStorageProvider(Protocol):
    """Opaque byte-blob storage addressed by a key within a named *area*, awaitable.

    The async twin of :class:`~jasil.providers.StorageProvider`; the area/key
    model, the traversal validation, and the ``expires_in`` caveat are all
    identical, so read that protocol for the contract.

    Worth restating because it is the one thing an async caller may assume
    wrongly: **awaitable does not mean natively async**. The local-filesystem and
    S3 backends have no async client underneath, so they run their blocking call
    on a worker thread. That keeps the event loop free — which is the whole
    point — but it is a thread pool, not epoll, and it is documented rather than
    hidden.
    """

    async def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str: ...
    async def get(self, area: str, key: str) -> bytes | None: ...
    async def exists(self, area: str, key: str) -> bool: ...
    async def delete(self, area: str, key: str) -> None: ...
    async def list_keys(self, area: str, prefix: str = "") -> list[str]: ...
    async def url(self, area: str, key: str, expires_in: int = 3600) -> str: ...


@runtime_checkable
class AsyncEventBusProvider(Protocol):
    """Publish/subscribe — inline on the loop, or durable via Redis Streams.

    The async twin of :class:`~jasil.providers.EventBusProvider`. Handlers are
    coroutine functions, and the in-process backend awaits them *inline* inside
    ``publish`` rather than scheduling them as tasks: that preserves the
    synchronous bus's ordering guarantee — when ``publish`` returns, every
    subscriber has run — which the event recorder's ``record_processing=False``
    optimisation depends on, and which a host relying on read-your-writes after a
    publish would otherwise silently lose.
    """

    async def publish(self, event: Event) -> None: ...
    def subscribe(self, event_type: str, handler: Callable[[Event], Awaitable[None]]) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


@runtime_checkable
class AsyncEventRecorder(Protocol):
    """Records each event's lifecycle to durable storage for observability.

    The async twin of :class:`~jasil.providers.EventRecorder`, injected into the
    async event bus by the async composition root when event logging is enabled.
    Recording must never disrupt event processing, so implementations swallow and
    log their own storage errors.

    :meth:`track` returns an *async* context manager. It marks the event
    ``processing`` on entry (unless ``record_processing`` is False, for an inline
    single-process dispatch where the intermediate state is never observed) and
    ``completed`` / ``failed`` on exit, re-raising any handler exception so the
    bus keeps its own error semantics.
    """

    async def record_published(self, event: Event) -> None: ...
    async def record_queued(self, event: Event) -> None: ...
    def track(
        self,
        event: Event,
        *,
        worker_id: str,
        handler_name: str | None,
        record_processing: bool = True,
    ) -> AbstractAsyncContextManager[None]: ...


@runtime_checkable
class AsyncLockProvider(Protocol):
    """Best-effort mutual exclusion for scheduled/backfill work, awaitable.

    The async twin of :class:`~jasil.providers.LockProvider`. ``try_acquire``
    returns an async context manager yielding whether the lock was taken, so the
    call site keeps the same ``async with ... as acquired:`` shape.
    """

    def try_acquire(self, name: str, ttl_seconds: int | None = None) -> AbstractAsyncContextManager[bool]: ...


@runtime_checkable
class AsyncGeocodingProvider(Protocol):
    """Reverse-geocoding — turn a coordinate into a place name, awaitable.

    The async twin of :class:`~jasil.providers.GeocodingProvider`, and it carries
    the same two obligations. This is the platform's only *outbound* call to a
    third party, so the egress hardening (host validation, address denylist, no
    redirects, response size cap, rate limiting) lives behind this interface
    rather than in a domain module — and it must not raise for an upstream
    failure, because geocoding is best-effort enrichment that may never fail the
    import or backfill that triggered it. Return ``None`` when nothing resolves.
    """

    async def reverse(self, latitude: float, longitude: float) -> GeocodedPlace | None: ...
