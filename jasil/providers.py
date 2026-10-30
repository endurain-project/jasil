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

from jasil._core.realtime_envelope import MAX_KEY_LENGTH
from jasil.events import Event

#: Cap on the length of a :class:`RealtimeProvider` hint key, in characters
#: (code points, not bytes).
MAX_REALTIME_KEY_LENGTH = MAX_KEY_LENGTH


class StateBackendUnavailableError(RuntimeError):
    """Raised by a :class:`StateProvider` when its backing store is unreachable.

    Lets domain stores react to an infrastructure outage (e.g. surface a 503 or
    swallow a best-effort cleanup) without knowing or importing anything about
    the concrete backend (Redis). The in-memory backend never raises it.
    """


class StorageBackendUnavailableError(RuntimeError):
    """Raised when a storage backend cannot complete an operation."""


class StorageKeyConflictError(ValueError):
    """Raised when plain local storage cannot represent a key and its descendants."""


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
    """Result of :meth:`StateProvider.record_tiered_failure`.

    Attributes:
        count: The counter value after this call.
        locked_until_epoch: Wall-clock epoch (seconds) the gate is closed until,
            or ``None`` when it is open.
        newly_locked: True only when *this* call closed (or extended) the gate.
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
    key/value access it exposes the few *atomic* primitives whose correctness
    would otherwise depend on the backend: one-winner claims and single-use values
    (``set_if_absent``, ``get_and_delete``) and an escalating gate over repeated
    events (``record_tiered_failure``).

    ``ttl_seconds`` is a positive number of seconds, or ``None`` for no expiry; zero or a negative
    value raises ``ValueError``. ``delete_prefix`` needs a non-empty prefix. A value the operation
    cannot apply to (``incr`` of a non-integer) raises ``ValueError`` on every backend; only a backend
    that cannot serve raises :class:`StateBackendUnavailableError`.
    """

    def get(self, key: str) -> bytes | None:
        """Return the value stored under ``key``.

        Args:
            key: The state key.

        Returns:
            The stored bytes, or ``None`` when the key is absent or has expired.

        Raises:
            StateBackendUnavailableError: When the backing store is unreachable.
        """

    def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
        """Store ``value`` under ``key``, replacing any existing value.

        Args:
            key: The state key.
            value: The bytes to store.
            ttl_seconds: Seconds until the key expires, or ``None`` for no expiry.

        Raises:
            ValueError: When ``ttl_seconds`` is zero or negative.
            StateBackendUnavailableError: When the backing store is unreachable.
        """

    def set_if_absent(self, key: str, value: bytes, ttl_seconds: int | None = None) -> bool:
        """Store ``value`` under ``key`` only when no live value exists, atomically.

        Args:
            key: The state key.
            value: The bytes to store.
            ttl_seconds: Seconds until the key expires, or ``None`` for no expiry.

        Returns:
            True when this call stored the value (it won the claim); False when the key already held one.

        Raises:
            ValueError: When ``ttl_seconds`` is zero or negative.
            StateBackendUnavailableError: When the backing store is unreachable.
        """

    def get_and_delete(self, key: str) -> bytes | None:
        """Return the value under ``key`` and delete it in one atomic step.

        Of several concurrent callers, at most one receives the value.

        Args:
            key: The state key.

        Returns:
            The stored bytes, or ``None`` when the key is absent or has expired.

        Raises:
            StateBackendUnavailableError: When the backing store is unreachable.
        """

    def delete(self, key: str) -> None:
        """Delete ``key``; deleting an absent key is not an error.

        Args:
            key: The state key.

        Raises:
            StateBackendUnavailableError: When the backing store is unreachable.
        """

    def delete_prefix(self, prefix: str) -> int:
        """Delete every key that starts with ``prefix``.

        Args:
            prefix: A non-empty key prefix, matched literally.

        Returns:
            The number of keys deleted.

        Raises:
            ValueError: When ``prefix`` is empty.
            StateBackendUnavailableError: When the backing store is unreachable.
        """

    def iter_keys(self, prefix: str) -> Iterator[str]:
        """Return an iterator over the live keys that start with ``prefix``.

        Args:
            prefix: The key prefix, matched literally; an empty prefix matches every key. On Redis that is
                every key in the database, the event stream and other applications' keys included, so name a
                prefix.

        Returns:
            An iterator over the matching keys, each once, in no guaranteed order.

        Raises:
            StateBackendUnavailableError: When the backing store is unreachable.
        """

    def incr(self, key: str, amount: int = 1, ttl_seconds: int | None = None) -> int:
        """Add ``amount`` to the integer stored under ``key`` atomically.

        A missing key counts as 0.

        Args:
            key: The counter key.
            amount: The amount to add; may be negative.
            ttl_seconds: Seconds until the key expires, restarted by this call, or ``None`` to leave any
                existing expiry unchanged.

        Returns:
            The counter value after the addition.

        Raises:
            ValueError: When ``ttl_seconds`` is zero or negative, or the stored value is not an integer.
            StateBackendUnavailableError: When the backing store is unreachable.
        """

    def record_tiered_failure(
        self,
        counter_key: str,
        gate_key: str,
        tiers: tuple[tuple[int, int], ...],
        counter_ttl_seconds: int,
    ) -> TieredFailureOutcome:
        """Count one occurrence and close a gate for longer as the count climbs.

        An escalating throttle over any repeated event — failed attempts, abusive
        requests, repeated errors; what the events *are* is the caller's. The
        call is atomic across processes. While the gate is closed it returns the
        current count **without** incrementing, so a caller that keeps arriving
        cannot push the count into a higher tier and extend its own penalty.

        Args:
            counter_key: Key of the running count, which expires
                ``counter_ttl_seconds`` after the latest occurrence.
            gate_key: Key holding the instant the gate stays closed until.
            tiers: ``(threshold, close_seconds)`` pairs in ascending threshold
                order. When the new count reaches a threshold the gate closes for
                that tier's seconds; the highest tier reached wins.
            counter_ttl_seconds: How long the count survives without a new
                occurrence.
        """


@runtime_checkable
class StorageObjects(Protocol):
    """Whole-object persistence, metadata, deletion, and enumeration."""

    def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str:
        """Store ``data`` under ``area``/``key``, replacing any existing object.

        Args:
            area: The domain-owned namespace.
            key: The object key within the area.
            data: The object bytes.
            content_type: The object's media type, recorded by backends that keep one and ignored by the others.

        Returns:
            ``key``.

        Raises:
            ValueError: When ``area`` or ``key`` is not a valid storage address.
            StorageKeyConflictError: When local plain storage cannot represent ``key`` beside existing keys.
            StorageBackendUnavailableError: When the backend cannot complete the write.
        """

    def get(self, area: str, key: str) -> bytes | None:
        """Return the whole object as bytes.

        Args:
            area: The domain-owned namespace.
            key: The object key within the area.

        Returns:
            The object bytes, or ``None`` when the object does not exist.

        Raises:
            ValueError: When ``area`` or ``key`` is not a valid storage address.
            StorageBackendUnavailableError: When the backend cannot complete the read.
        """

    def stat(self, area: str, key: str) -> ObjectStat | None:
        """Return an object's metadata without reading its content.

        Args:
            area: The domain-owned namespace.
            key: The object key within the area.

        Returns:
            The object's :class:`ObjectStat`, or ``None`` when the object does not exist.

        Raises:
            ValueError: When ``area`` or ``key`` is not a valid storage address.
            StorageBackendUnavailableError: When the backend cannot complete the lookup.
        """

    def exists(self, area: str, key: str) -> bool:
        """Report whether an object exists.

        Args:
            area: The domain-owned namespace.
            key: The object key within the area.

        Returns:
            True when the object exists.

        Raises:
            ValueError: When ``area`` or ``key`` is not a valid storage address.
            StorageBackendUnavailableError: When the backend cannot complete the lookup.
        """

    def delete(self, area: str, key: str) -> None:
        """Delete an object; deleting a missing object is not an error.

        Args:
            area: The domain-owned namespace.
            key: The object key within the area.

        Raises:
            ValueError: When ``area`` or ``key`` is not a valid storage address.
            StorageBackendUnavailableError: When the backend cannot complete the delete.
        """

    def list_keys(self, area: str, prefix: str = "") -> list[str]:
        """Return the keys in an area that start with ``prefix``, sorted.

        Materializes the whole listing; use :meth:`iter_objects` for large namespaces.

        Args:
            area: The domain-owned namespace.
            prefix: A lexical key filter; empty selects every key in the area.

        Returns:
            The matching keys in ascending order.

        Raises:
            ValueError: When ``area`` or ``prefix`` is not valid.
            StorageBackendUnavailableError: When the backend cannot complete the listing.
        """

    def iter_objects(self, area: str, prefix: str = "") -> Iterator[tuple[str, float]]:
        """Lazily yield ``(key, modified_epoch)`` for the objects in an area that start with ``prefix``.

        Args:
            area: The domain-owned namespace.
            prefix: A lexical key filter; empty selects every key in the area.

        Returns:
            An iterator of keys with their modification time in epoch seconds, in no guaranteed order.

        Raises:
            ValueError: When ``area`` or ``prefix`` is not valid.
            StorageBackendUnavailableError: When the backend cannot complete the listing.
        """


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
    ) -> int:
        """Store the remaining bytes of ``source`` under ``area``/``key`` without buffering them all.

        The write is atomic: on any failure no partial new object becomes visible. ``source`` is read to
        end of stream and is not closed.

        Args:
            area: The domain-owned namespace.
            key: The object key within the area.
            source: The readable binary stream to copy from.
            max_bytes: The most bytes to accept, or ``None`` for no limit.
            content_type: The object's media type, recorded by backends that keep one and ignored by the others.

        Returns:
            The number of bytes stored.

        Raises:
            ValueError: When ``area`` or ``key`` is not a valid storage address, or ``max_bytes`` is negative.
            StorageSizeLimitError: When ``source`` yields more than ``max_bytes``.
            StorageKeyConflictError: When local plain storage cannot represent ``key`` beside existing keys.
            StorageBackendUnavailableError: When the backend cannot complete the write.
        """

    def open_stream(
        self,
        area: str,
        key: str,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """Open an object for reading as a read-once, non-seekable binary stream.

        The caller closes the stream.

        Args:
            area: The domain-owned namespace.
            key: The object key within the area.
            offset: The byte offset to start reading at.
            length: The most bytes to read from ``offset``, or ``None`` to read to the end.

        Returns:
            The stream positioned at ``offset``; a range past the end of the object yields no bytes.

        Raises:
            FileNotFoundError: When the object does not exist.
            ValueError: When ``area`` or ``key`` is not a valid storage address, or ``offset`` or ``length`` is
                negative.
            StorageBackendUnavailableError: When the backend cannot complete the read.
        """


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
    ) -> ServePlan:
        """Choose how the host should deliver an object to a client.

        Args:
            area: The domain-owned namespace.
            key: The object key within the area.
            download_as: A filename that makes the client download the object, honoured only by backends that can
                enforce it.
            content_type: A response media type override, honoured only by backends that can enforce it.
            expires_in: Seconds the delivery stays valid, honoured only by backends that can enforce it.

        Returns:
            A :class:`ServeFile`, :class:`ServeRedirect` or :class:`ServeStream`.

        Raises:
            FileNotFoundError: When the object does not exist.
            ValueError: When an address or delivery control is invalid for the backend.
            StorageBackendUnavailableError: When the backend cannot complete the lookup.
        """

    def url(
        self,
        area: str,
        key: str,
        expires_in: int = 3600,
        *,
        download_as: str | None = None,
        content_type: str | None = None,
    ) -> str:
        """Return a URL for an object, without checking that the object exists.

        Args:
            area: The domain-owned namespace.
            key: The object key within the area.
            expires_in: Seconds the URL stays valid, honoured only by backends that can enforce it.
            download_as: A filename that makes the client download the object, honoured only by backends that can
                enforce it.
            content_type: A response media type override, honoured only by backends that can enforce it.

        Returns:
            The URL string.

        Raises:
            ValueError: When an address or delivery control is invalid for the backend.
            StorageBackendUnavailableError: When the backend cannot build the URL.
        """


@runtime_checkable
class StorageManagement(Protocol):
    """Object-set mutation and backend readiness operations."""

    def delete_prefix(self, area: str, prefix: str) -> int:
        """Delete the object at ``prefix`` and every object below ``prefix/``.

        Args:
            area: The domain-owned namespace.
            prefix: A non-empty key root; ``"pkg/1"`` never selects ``"pkg/10"``.

        Returns:
            The number of objects deleted.

        Raises:
            ValueError: When ``area`` or ``prefix`` is not valid, or ``prefix`` is empty.
            StorageBackendUnavailableError: When the backend cannot complete the delete.
        """

    def copy(self, src_area: str, src_key: str, dst_area: str, dst_key: str) -> None:
        """Copy an object inside the backend, replacing any object at the destination.

        Args:
            src_area: The source namespace.
            src_key: The source object key.
            dst_area: The destination namespace.
            dst_key: The destination object key.

        Raises:
            FileNotFoundError: When the source object does not exist.
            ValueError: When an area or key is not a valid storage address.
            StorageBackendUnavailableError: When the backend cannot complete the copy.
        """

    def check_writable(self) -> None:
        """Probe that the backend can persist objects by writing and deleting a scratch object.

        Raises:
            StorageBackendUnavailableError: When the configured store cannot persist objects.
        """


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
    ) -> UploadSession:
        """Start a resumable upload and return its durable session handle.

        Args:
            area: The domain-owned namespace.
            key: The object key the completed upload is stored under.
            max_bytes: The most bytes the finished object may hold, or ``None`` for no limit.
            content_type: The object's media type, recorded by backends that keep one and ignored by the others.

        Returns:
            The :class:`UploadSession`, carrying the portable part limits.

        Raises:
            ValueError: When ``area`` or ``key`` is not a valid storage address, or ``max_bytes`` is negative.
            StorageKeyConflictError: When local plain storage cannot represent ``key`` beside existing keys.
            StorageBackendUnavailableError: When the backend cannot create the session.
        """

    def upload_part(
        self,
        session: UploadSession,
        part_number: int,
        source: BinaryIO,
        *,
        size: int,
    ) -> PartRef:
        """Stage one part of an upload, replacing any earlier part with the same number.

        Args:
            session: The session returned by :meth:`begin_upload`.
            part_number: The part's position, from 1 to ``session.max_parts``.
            source: The readable binary stream holding the part; read once, neither seeked nor closed.
            size: The exact number of bytes ``source`` holds.

        Returns:
            The :class:`PartRef` to pass to :meth:`complete_upload`.

        Raises:
            ValueError: When ``part_number`` is out of range, or ``source`` holds fewer or more than ``size`` bytes.
            StorageSizeLimitError: When ``size`` exceeds ``session.max_part_size`` or the staged parts would exceed
                ``session.max_bytes``.
            StorageUploadSessionError: When the session is not valid or no longer active.
            StorageBackendUnavailableError: When the backend cannot stage the part.
        """

    def complete_upload(self, session: UploadSession, parts: Sequence[PartRef]) -> int:
        """Assemble the staged parts into the object and end the session.

        Args:
            session: The session returned by :meth:`begin_upload`.
            parts: A reference for every staged part, ordered by ascending part number.

        Returns:
            The size of the stored object in bytes.

        Raises:
            ValueError: When ``parts`` is empty, unordered, too long, or a part other than the last is smaller than
                ``session.min_part_size``.
            StorageSizeLimitError: When the total exceeds ``session.max_bytes``.
            StorageUploadSessionError: When the session is not active, or ``parts`` does not match the staged parts.
            StorageBackendUnavailableError: When the backend cannot complete the upload.
        """

    def abort_upload(self, session: UploadSession) -> None:
        """Discard a session and its staged parts; an already-ended session is not an error.

        Args:
            session: The session returned by :meth:`begin_upload`.

        Raises:
            StorageUploadSessionError: When the session handle is not valid.
            StorageBackendUnavailableError: When the backend cannot discard the session.
        """

    def cleanup_uploads(self, *, older_than_epoch: float) -> int:
        """Abort every session initiated before ``older_than_epoch``.

        Args:
            older_than_epoch: The cutoff, in epoch seconds; sessions started earlier are aborted.

        Returns:
            The number of sessions aborted.

        Raises:
            ValueError: When ``older_than_epoch`` is not finite.
            StorageBackendUnavailableError: When the backend cannot complete the cleanup.
        """


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
    differ**: ``s3://`` returns a presigned URL that honours expiry (1 second to 7
    days, ``ValueError`` otherwise) and response header overrides, while
    ``local://`` returns a plain path for the host's own
    web server and cannot enforce any of them. Do not rely on these controls
    unless you know the deployment uses object storage; on local disk they are
    host policy. The local backend logs a warning the first time it is handed a
    control it cannot honour.
    """


@runtime_checkable
class EventBusProvider(Protocol):
    """Publish/subscribe — synchronous in-process, or durable via Redis Streams."""

    def publish(self, event: Event) -> None:
        """Hand ``event`` to the bus for delivery to the handlers subscribed to its ``event_type``.

        Args:
            event: The event to deliver.
        """

    def subscribe(self, event_type: str, handler: Callable[[Event], None]) -> None:
        """Register ``handler`` for events of ``event_type``.

        Args:
            event_type: The event type to receive.
            handler: Called with each matching event.
        """

    def start(self) -> None:
        """Begin background delivery, for buses that have a consumer; a no-op otherwise."""

    def stop(self) -> None:
        """Stop background delivery, for buses that have a consumer; a no-op otherwise."""


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
    semantics (propagate in-process, logged and acked on Redis Streams).

    Implementations may additionally offer ``record_queued_in(event, db)``, which
    stages the ``queued`` row on the caller's session so it commits or rolls back
    with the outbox row. It is optional and deliberately not part of this
    protocol (SemVer); the publisher uses it when present and otherwise falls
    back to :meth:`record_queued`.
    """

    def record_published(self, event: Event) -> None:
        """Insert the ``published`` row for a bus-delivered event.

        Args:
            event: The event being published.
        """

    def record_queued(self, event: Event) -> None:
        """Insert the ``queued`` row for a durably delivered event.

        Args:
            event: The event being queued.
        """

    def track(
        self,
        event: Event,
        *,
        worker_id: str,
        handler_name: str | None,
        record_processing: bool = True,
    ) -> AbstractContextManager[None]:
        """Record the processing of ``event`` around the handlers run inside the context.

        Args:
            event: The event being processed.
            worker_id: The process or consumer handling the event.
            handler_name: The subscriber or subscribers that process the event, if known.
            record_processing: Whether to write the intermediate ``processing`` state.

        Returns:
            A context manager that records ``completed`` on a normal exit, or ``failed`` and re-raises on an
            exception.
        """


class AuditSinkConflictError(RuntimeError):
    """An :class:`AuditSink` already holds different content under the name being written."""


@runtime_checkable
class AuditSink(Protocol):
    """A write-once copy of audit checkpoints, kept outside the audited database.

    ``write_once`` stores ``content`` under ``name`` and never replaces it: writing the same bytes again
    is a no-op (a retry after a crash), writing different bytes raises
    :class:`AuditSinkConflictError`. That is the property that makes the copy evidence: whoever can
    rewrite the database cannot, through this interface, rewrite what was already sent. ``read`` returns
    what is stored under ``name``, or ``None``, so a seal retried after a crash can recognise its own copy.
    """

    def write_once(self, name: str, content: bytes) -> None:
        """Store ``content`` under ``name`` unless the same bytes are already there.

        Args:
            name: The checkpoint name.
            content: The checkpoint bytes.

        Raises:
            AuditSinkConflictError: When different content is already stored under ``name``.
        """

    def read(self, name: str) -> bytes | None:
        """Return what is stored under ``name``.

        Args:
            name: The checkpoint name.

        Returns:
            The stored bytes, or ``None`` when nothing is stored under ``name``.
        """


@runtime_checkable
class LockProvider(Protocol):
    """Best-effort mutual exclusion for scheduled/backfill work."""

    def try_acquire(self, name: str, ttl_seconds: int | None = None) -> AbstractContextManager[bool]:
        """Try to take the lock ``name`` without blocking, for the duration of the ``with`` block.

        Args:
            name: The logical lock name.
            ttl_seconds: How long the lock may be held, for backends whose locks expire; backends whose locks are
                not time-based ignore it.

        Returns:
            A context manager yielding True when the lock was acquired and False when another holder has it. An
            acquired lock is released on exit.
        """


@runtime_checkable
class ClockProvider(Protocol):
    """Injectable time source for testability."""

    def now(self) -> datetime:
        """Return the current date and time."""

    def monotonic(self) -> float:
        """Return a monotonic clock reading in seconds, for measuring elapsed time."""


@dataclass(frozen=True)
class GeocodedPlace:
    """A place resolved from coordinates. Any field may be ``None``.

    The fields are administrative levels, not any host's vocabulary, and every
    backend fills them the same way: a given level means the same thing whichever
    upstream service answered.

    Attributes:
        locality: The populated place — the city, town, village or hamlet the
            coordinate is in.
        sublocality: A subdivision of the locality (district, suburb, borough,
            neighbourhood).
        region: The first-level administrative area (state, province, region).
        country: Country name.
        country_code: ISO 3166-1 alpha-2 code, upper-case.
        display_name: The upstream's own single-line description of the place,
            for the services that supply one.

    The upstream is untrusted: a backend leaves a field ``None`` rather than pass on a value longer than 256
    characters (1024 for ``display_name``) or containing a control character.
    """

    locality: str | None = None
    sublocality: str | None = None
    region: str | None = None
    country: str | None = None
    country_code: str | None = None
    display_name: str | None = None


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
    best-effort enrichment, and a provider outage must never fail the caller's
    own work. Return ``None`` when nothing resolves.
    """

    def reverse(self, latitude: float, longitude: float) -> GeocodedPlace | None:
        """Resolve a coordinate to a place.

        Args:
            latitude: Latitude in decimal degrees.
            longitude: Longitude in decimal degrees.

        Returns:
            The resolved :class:`GeocodedPlace`, or ``None`` when nothing resolves or the upstream fails.
        """


@dataclass(frozen=True)
class MailMessage:
    """One outbound email, already built by the host.

    Attributes:
        to: Recipient addresses, each a bare ``addr`` or ``"Name <addr>"``.
        subject: Subject line. Must not contain CR or LF.
        text: Plain-text body. At least one of ``text`` / ``html`` is required.
        html: HTML body. With ``text`` as well, the message is
            ``multipart/alternative``.
        reply_to: Optional single ``Reply-To`` address.
    """

    to: Sequence[str]
    subject: str
    text: str | None = None
    html: str | None = None
    reply_to: str | None = None

    def __hash__(self) -> int:
        # ``to`` is usually a list, which the generated hash cannot digest.
        recipients = (self.to,) if isinstance(self.to, str) else tuple(self.to)
        return hash((recipients, self.subject, self.text, self.html, self.reply_to))


class MailError(RuntimeError):
    """A message could not be handed to the mail server.

    The message carries a failure category and, when there is one, the SMTP
    status code — never an address, a subject, a body or server-supplied text,
    because a job runner persists ``str(error)`` and logs it.

    Attributes:
        status: The SMTP status code, or ``None`` when the failure had none
            (unreachable server, TLS failure, deadline).
    """

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status

    @property
    def permanent(self) -> bool:
        """True for a 5xx reply: the server will refuse a retry the same way."""
        return self.status is not None and 500 <= self.status < 600


class MailNotConfiguredError(MailError):
    """Mail was sent while no backend is configured."""


@runtime_checkable
class MailProvider(Protocol):
    """Outbound email — hand a built message to a mail server.

    Unlike geocoding, mail is not best-effort enrichment: a silently dropped
    message is a bug, and a raised exception is what makes a durable job retry.

    ``send`` returns the ``Message-ID`` the backend assigned, angle brackets
    included, exactly as it appears in the header.

    Raises:
        MailError: On any delivery failure (:class:`MailNotConfiguredError` when
            no backend is configured).
        ValueError: On an invalid message — no recipient, no body, a malformed
            address, or CR/LF in a header value. A programming error, not a
            delivery error.
    """

    def send(self, message: MailMessage) -> str:
        """Hand ``message`` to the mail server.

        Args:
            message: The built message.

        Returns:
            The ``Message-ID`` the backend assigned, angle brackets included.
        """


@runtime_checkable
class RealtimeProvider(Protocol):
    """Cross-process "something changed, refetch" hints addressed by an opaque key.

    The host owns the live connections (a WebSocket or SSE endpoint, its auth and
    its connection registry); this provider only carries a small hint from
    whichever process noticed the change — an API replica, a dedicated job
    worker — to *every* process that may hold the connection it is for. The
    ``key`` names the audience of a hint; its meaning is entirely the host's (a
    user id, a device id, a cache name, ...) and JASIL does no per-key routing or
    filtering. Each receiving process looks the key up in its own registry and
    pushes or drops.

    **The contract — a hint is a latency optimisation, never the record:**

    - Delivery is **best-effort, at-most-once and unordered**. A hint can be
      lost (a replica reconnecting to Redis, a restart, a full socket buffer)
      or arrive after a later one. The receiver must treat it as "go and look",
      never as the data itself, and must stay correct if it never arrives.
    - **Transport failures never raise** out of :meth:`notify`: Redis being
      down is logged and the hint dropped, so a request or job never fails
      because a push could not be sent.
    - **Programmer errors do raise** ``ValueError``: a ``key`` that is not a
      ``str``, an empty ``key``, a ``key`` over
      :data:`MAX_REALTIME_KEY_LENGTH` (256) characters (code points, not
      bytes), a ``key`` or ``hint`` holding an unpaired Unicode surrogate, a
      ``hint`` that is not a JSON-serialisable ``dict``, or an envelope over the
      backend's size cap (4096 bytes by default). Every backend validates the
      same way, so the bug surfaces in a local test.
    - The ``key`` appears in the warning logged when a hint is dropped, so do
      not put a secret (a token, a session id) in it.
    - **Every started process receives every hint**, including the ones it
      published itself. There is no per-key routing, no topic, no
      acknowledgement and no history.

    Methods:
        notify: Publish a hint. Synchronous and thread-safe; works in a
            process that never calls :meth:`start` (a job worker that only
            publishes).
        subscribe: Register a local delivery callback, called as
            ``handler(key, hint)``. Register before :meth:`start`; several
            handlers may be registered. Handlers run on the provider's
            subscriber thread (on the caller's thread for the in-process
            backend), so keep them short and hand anything slow to the host's
            own loop. A handler that raises is logged and skipped.
        start: Begin receiving. Idempotent.
        stop: Stop receiving and wait for the subscriber to finish.
            Idempotent and never raises.
    """

    def notify(self, key: str, hint: dict) -> None:
        """Publish a best-effort hint for the opaque audience ``key``.

        Transport failures drop the hint without raising; publishing does not
        require :meth:`start`.

        Raises:
            ValueError: When the key or hint violates this provider's validation
                rules, or the encoded envelope exceeds the backend's byte cap.
        """

    def subscribe(self, handler: Callable[[str, dict], None]) -> None:
        """Register ``handler(key, hint)`` for all received keys before :meth:`start`.

        Callbacks run on the subscriber thread, or on the caller's thread for
        the in-process backend. Handler exceptions do not stop other handlers.
        """

    def start(self) -> None:
        """Begin receiving hints; idempotent."""

    def stop(self) -> None:
        """Stop receiving hints and wait for the subscriber to finish; idempotent and never raises."""
