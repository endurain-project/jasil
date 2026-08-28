"""Composable provider interfaces and complete platform capabilities.

Pure module: only stdlib typing and the event envelope. No infrastructure
(redis / boto3 / sqlalchemy) and no domain imports, so any module can depend on
these providers without pulling in a backend. Concrete backends live in
``jasil.backends`` and are selected by the composition root
(``jasil.container.build_platform``).
"""

from collections.abc import Callable, Iterator, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, Protocol, runtime_checkable

from jasil.events import Event


class StateBackendUnavailableError(RuntimeError):
    """Raised by a :class:`StateProvider` when its backing store is unreachable.

    Lets domain stores react to an infrastructure outage (e.g. surface a 503 or
    swallow a best-effort cleanup) without knowing or importing anything about
    the concrete backend (Redis). The in-memory backend never raises it.
    """


class StorageBackendUnavailableError(RuntimeError):
    """Raised when a storage backend cannot complete an operation."""


class StorageSizeLimitError(ValueError):
    """Raised when a streaming write exceeds its configured byte limit."""


class StorageUploadSessionError(RuntimeError):
    """Raised when a resumable upload session or part reference is not active."""


@dataclass(frozen=True)
class UploadSession:
    """Durable handle and portable limits for one resumable upload."""

    area: str
    key: str
    session_id: str
    max_bytes: int | None
    min_part_size: int
    max_part_size: int
    max_parts: int


@dataclass(frozen=True)
class PartRef:
    """Backend-validated reference to one uploaded part.

    ``validator`` is an opaque equality token. It may be a digest, an object
    storage ETag, or another backend value; callers must not interpret it.
    """

    part_number: int
    size: int
    validator: str


@dataclass(frozen=True)
class ObjectStat:
    """Metadata available without reading an object's content.

    ``content_type`` and ``etag`` are optional because portable local
    filesystems expose neither. An ETag is an opaque backend validator, not
    necessarily a digest of the object bytes.
    """

    size: int
    modified_epoch: float
    content_type: str | None = None
    etag: str | None = None


@dataclass(frozen=True)
class ServeFile:
    """Serve a local file directly, using sendfile or a reverse-proxy mapping."""

    path: Path


@dataclass(frozen=True)
class ServeRedirect:
    """Redirect the client to a backend-generated object URL."""

    url: str


@dataclass(frozen=True)
class ServeStream:
    """Proxy an object through the host using a read-once binary stream."""

    stream: BinaryIO


type ServePlan = ServeFile | ServeRedirect | ServeStream


@dataclass(frozen=True)
class TieredFailureOutcome:
    """Result of an atomic tiered-lockout increment.

    Attributes:
        count: The failure counter value after this attempt.
        locked_until_epoch: Wall-clock epoch (seconds) the lock is active until,
            or ``None`` when not locked.
        newly_locked: True only when *this* call created (or renewed) the lock.
    """

    count: int
    locked_until_epoch: int | None
    newly_locked: bool


@runtime_checkable
class StateProvider(Protocol):
    """Ephemeral keyed state (counters, TTL flags, small blobs).

    The single seam through which domain code reads and writes short-lived
    shared state, so a store never needs to know whether it is backed by a
    process-local dict (``local``) or Redis (``distributed``). Beyond plain
    key/value access it exposes the few *atomic* primitives that login-throttling
    and single-use-token stores need (``set_if_absent``, ``get_and_delete``,
    ``record_tiered_failure``) so their correctness does not depend on the
    backend.
    """

    def get(self, key: str) -> bytes | None: ...
    def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None: ...
    def set_if_absent(self, key: str, value: bytes, ttl_seconds: int | None = None) -> bool: ...
    def get_and_delete(self, key: str) -> bytes | None: ...
    def delete(self, key: str) -> None: ...
    def delete_prefix(self, prefix: str) -> int: ...
    def iter_keys(self, prefix: str) -> Iterator[str]: ...
    def incr(self, key: str, amount: int = 1, ttl_seconds: int | None = None) -> int: ...
    def record_tiered_failure(
        self,
        counter_key: str,
        gate_key: str,
        tiers: tuple[tuple[int, int], ...],
        counter_ttl_seconds: int,
    ) -> TieredFailureOutcome: ...


@runtime_checkable
class StorageObjects(Protocol):
    """Whole-object persistence, metadata, deletion, and enumeration."""

    def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str: ...
    def get(self, area: str, key: str) -> bytes | None: ...
    def stat(self, area: str, key: str) -> ObjectStat | None: ...
    def exists(self, area: str, key: str) -> bool: ...
    def delete(self, area: str, key: str) -> None: ...
    def list_keys(self, area: str, prefix: str = "") -> list[str]: ...
    def iter_objects(self, area: str, prefix: str = "") -> Iterator[tuple[str, float]]: ...


@runtime_checkable
class StorageStreams(Protocol):
    """Bounded-memory object reads and writes."""

    def save_stream(
        self,
        area: str,
        key: str,
        source: BinaryIO,
        *,
        max_bytes: int | None = None,
        content_type: str | None = None,
    ) -> int: ...
    def open_stream(
        self,
        area: str,
        key: str,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO: ...


@runtime_checkable
class StorageDelivery(Protocol):
    """Backend-efficient delivery without coupling to a web framework."""

    def serve(
        self,
        area: str,
        key: str,
        *,
        download_as: str | None = None,
        content_type: str | None = None,
        expires_in: int = 3600,
    ) -> ServePlan: ...
    def url(
        self,
        area: str,
        key: str,
        expires_in: int = 3600,
        *,
        download_as: str | None = None,
        content_type: str | None = None,
    ) -> str: ...


@runtime_checkable
class StorageManagement(Protocol):
    """Object-set mutation and backend readiness operations."""

    def delete_prefix(self, area: str, prefix: str) -> int: ...
    def copy(self, src_area: str, src_key: str, dst_area: str, dst_key: str) -> None: ...
    def check_writable(self) -> None: ...


@runtime_checkable
class ResumableUploads(Protocol):
    """Durable multipart upload lifecycle and abandoned-session cleanup.

    ``upload_part`` reads ``source`` once without seeking or closing it. The
    required ``size`` must exactly match the source; short and long sources raise
    :class:`ValueError` without committing that part. A failed attempt may have
    consumed the source, so retry with a newly opened source and the same session
    and part number. Limits reported by :class:`UploadSession` are JASIL's
    built-in portability limits, shared by every built-in backend so callers
    never branch on the selected backend.
    """

    def begin_upload(
        self,
        area: str,
        key: str,
        *,
        max_bytes: int | None = None,
        content_type: str | None = None,
    ) -> UploadSession: ...
    def upload_part(
        self,
        session: UploadSession,
        part_number: int,
        source: BinaryIO,
        *,
        size: int,
    ) -> PartRef: ...
    def complete_upload(self, session: UploadSession, parts: Sequence[PartRef]) -> int: ...
    def abort_upload(self, session: UploadSession) -> None: ...
    def cleanup_uploads(self, *, older_than_epoch: float) -> int: ...


@runtime_checkable
class StorageProvider(
    StorageObjects,
    StorageStreams,
    StorageDelivery,
    StorageManagement,
    ResumableUploads,
    Protocol,
):
    """Complete object-storage capability assembled by :class:`Platform`.

    An area is a domain-owned namespace (e.g. ``"avatars"``, ``"exports"``) so
    one backend serves every subsystem. The database stores only the key (the
    area is a fixed constant of the calling domain); ``url`` is computed at
    serialization time so migrating local -> S3 needs no data migration.

    ``save`` and ``get`` are the simple whole-object path for small blobs.
    ``save_stream`` and ``open_stream`` are the bounded-memory path for large
    objects. Streaming writes are atomic: exceeding ``max_bytes`` raises
    :class:`StorageSizeLimitError` and never exposes a partial new object.
    Streaming reads are read-once and non-seekable on every backend. ``offset``
    and ``length`` select a byte range without requiring the returned stream to
    seek; a missing object raises :class:`FileNotFoundError`.

    Resumable writes use ``begin_upload``, one or more ``upload_part`` calls,
    then ``complete_upload`` or ``abort_upload``. Sessions are portable across
    processes sharing the configured backend and report their part constraints.
    ``cleanup_uploads`` aborts sessions initiated before a caller-owned cutoff.

    ``list_keys`` is the convenient materialized listing for small namespaces.
    ``iter_objects`` lazily yields ``(key, modified_epoch)`` for reconciliation
    jobs that may scan millions of objects and need an age guard.

    ``serve`` selects the backend's efficient delivery primitive without
    importing a web framework: local storage returns :class:`ServeFile`, S3
    returns :class:`ServeRedirect`, and a backend with neither capability may
    return :class:`ServeStream`. It verifies that the object exists first.

    ``stat`` returns size and modification time plus backend metadata when
    available. ``copy`` keeps object bytes inside the backend. ``delete_prefix``
    deletes one non-empty key root and its slash-delimited descendants, not
    adjacent lexical prefixes (``"pkg/1"`` never selects ``"pkg/10"``).

    ``check_writable`` performs a write-and-delete readiness probe and raises
    :class:`StorageBackendUnavailableError` when the configured store cannot
    persist objects.

    ``expires_in`` is **best-effort, and the one place the backends genuinely
    differ**: ``s3://`` returns a presigned URL that honours expiry and response
    header overrides, while ``local://`` returns a plain path for the host's own
    web server and cannot enforce any of them. Do not rely on these controls
    unless you know the deployment uses object storage; on local disk they are
    host policy. The local backend logs a warning the first time it is handed a
    control it cannot honour.
    """


@runtime_checkable
class EventBusProvider(Protocol):
    """Publish/subscribe — synchronous in-process, or durable via Redis Streams."""

    def publish(self, event: Event) -> None: ...
    def subscribe(self, event_type: str, handler: Callable[[Event], None]) -> None: ...
    def start(self) -> None: ...
    def stop(self) -> None: ...


@runtime_checkable
class EventRecorder(Protocol):
    """Records each event's lifecycle to durable storage for observability.

    Injected into the event bus by the composition root when event logging is
    enabled. Recording must never disrupt event processing, so implementations
    swallow and log their own storage errors. :meth:`record_published` and
    :meth:`record_queued` insert the initial row for a bus-delivered or
    durable-delivered event respectively. :meth:`track` wraps handler
    execution — it marks the event *processing* on entry (unless
    ``record_processing`` is False, for a synchronous single-process dispatch
    where the intermediate state is never observed) and *completed* / *failed* on
    exit, re-raising any handler exception so the bus keeps its own error
    semantics (propagate in-process, leave pending on Redis Streams).
    """

    def record_published(self, event: Event) -> None: ...
    def record_queued(self, event: Event) -> None: ...
    def track(
        self,
        event: Event,
        *,
        worker_id: str,
        handler_name: str | None,
        record_processing: bool = True,
    ) -> AbstractContextManager[None]: ...


@runtime_checkable
class LockProvider(Protocol):
    """Best-effort mutual exclusion for scheduled/backfill work."""

    def try_acquire(self, name: str, ttl_seconds: int | None = None) -> AbstractContextManager[bool]: ...


@runtime_checkable
class ClockProvider(Protocol):
    """Injectable time source for testability."""

    def now(self) -> datetime: ...
    def monotonic(self) -> float: ...


@dataclass(frozen=True)
class GeocodedPlace:
    """A place resolved from coordinates. Any field may be ``None``.

    Attributes:
        city: Populated place name, when the provider resolved one.
        town: Sub-locality (district/town), when the provider resolved one.
        country: Country name, when the provider resolved one.
    """

    city: str | None = None
    town: str | None = None
    country: str | None = None


@runtime_checkable
class GeocodingProvider(Protocol):
    """Reverse-geocoding — turn a coordinate into a place name.

    The one seam through which domain code performs reverse geocoding, so a
    caller never knows which upstream service is configured, nor that a network
    call happens at all. That matters more here than for the other providers:
    this is the platform's only *outbound* call to a third party, so the
    egress hardening it needs (host validation, address denylist, no redirects,
    rate limiting) lives behind this interface instead of in a domain module.

    Implementations must not raise for an upstream failure — geocoding is
    best-effort enrichment, and a provider outage must never fail the import or
    backfill that triggered it. Return ``None`` when nothing resolves.
    """

    def reverse(self, latitude: float, longitude: float) -> GeocodedPlace | None: ...
