"""Tamper evidence: signed, hash-chained checkpoints over fixed time windows of the audit log.

Database permissions stop everyone but the table owner from changing a row. Checkpoints are what
*detect* a change by someone who can, and they keep the evidence outside what that person can reach.

Every ``checkpoint_window_minutes`` of ``occurred_at`` is a window. Once a window is
``checkpoint_grace_minutes`` past its end it is sealed: its rows are serialized canonically in
``(occurred_at, id)`` order, hashed together with the previous checkpoint's hash (so the checkpoints form
a chain), optionally signed with Ed25519, stored in ``jasil_audit_checkpoints`` and, when configured,
copied to a write-once external sink. Each checkpoint also records, under the same signature, the
``retention_days`` and ``legal_hold`` in force when it was sealed: that, and not a row in the log anyone
able to insert could forge, is what verification accepts as the reason a window's rows may be gone. The
grace period exists because ``occurred_at`` is taken when the
row is staged but the row is only visible once its transaction commits: sealing at the window's end
would let a slow transaction look like a later insertion.

There is deliberately no per-row hash chain: it would serialize every insert across replicas.

Detection, not prevention: someone who can rewrite the rows *and* the checkpoints *and* holds the
signing key can forge a consistent history. The signature (key kept out of the database) and the
external sink are what make that hard; see ``docs/audit.md``.
"""

import base64
import binascii
import functools
import hashlib
import importlib
import json
import logging
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import groupby
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

import jasil.orm as jasil_orm
import jasil.runtime as platform_runtime
from jasil._core import optional_deps
from jasil._core.timestamps import as_utc
from jasil.audit import service
from jasil.audit.schema import VerificationReport, VerificationStatus, WindowVerification
from jasil.providers import AuditSink, AuditSinkConflictError
from jasil.settings import AuditSettings, get_settings

logger = logging.getLogger(__name__)

__all__ = ["seal_pending_windows", "verify_checkpoints"]

#: Windows sealed per maintenance run, so a long backlog (first run on an old log, or after an outage)
#: is worked off over several runs instead of holding the lock for minutes.
MAX_WINDOWS_PER_RUN = 1000
#: The longest range one :func:`verify_checkpoints` call reads; every row in it is hashed in one session.
MAX_VERIFY_RANGE = timedelta(days=366)

_MESSAGE_PREFIX = "jasil-audit-checkpoint-v1"
_FETCH_SIZE = 1000
_EARLIEST = datetime.min.replace(tzinfo=UTC)

_COLUMNS = (
    "id",
    "occurred_at",
    "action_id",
    "category",
    "severity",
    "outcome",
    "actor_type",
    "actor_id",
    "credential_type",
    "credential_id",
    "on_behalf_of_id",
    "target_type",
    "target_id",
    "target_owner_id",
    "request_id",
    "event_id",
    "source",
    "ip_address",
    "user_agent",
    "details",
    "details_version",
    "instance_id",
    "app_name",
    "app_version",
)

_unsigned_warned = False


@dataclass(frozen=True)
class _Digest:
    row_count: int
    hash: str


def _iso(moment: datetime) -> str:
    return as_utc(moment).isoformat(timespec="microseconds")


def canonical_row(row: Any) -> bytes:
    """Serialize one audit row to the bytes that are hashed.

    A JSON array of the columns in a fixed order with keys sorted inside ``details``, so the bytes depend
    on the stored values and on nothing a database or driver may reorder.
    """
    values = [_iso(row.occurred_at) if name == "occurred_at" else getattr(row, name) for name in _COLUMNS]
    return json.dumps(values, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def _window_rows(db: Session, start: datetime, end: datetime) -> Iterator[Any]:
    """Yield the window's rows ordered by ``(occurred_at, id)``.

    The database orders by ``occurred_at`` only and the ``id`` tiebreak is applied here, because ordering
    a text column is collation-dependent and a restore into a differently collated database would
    otherwise change the bytes that were hashed.
    """
    from jasil.audit.models import AuditLog

    statement = (
        select(AuditLog)
        .where(AuditLog.occurred_at >= start, AuditLog.occurred_at < end)
        .order_by(AuditLog.occurred_at)
        .execution_options(yield_per=_FETCH_SIZE)
    )
    for _, group in groupby(db.scalars(statement), key=lambda row: as_utc(row.occurred_at)):
        yield from sorted(group, key=lambda row: row.id)


def _digest(db: Session, start: datetime, end: datetime, prev_hash: str | None) -> _Digest:
    hasher = hashlib.sha256()
    count = 0
    for row in _window_rows(db, start, end):
        hasher.update(canonical_row(row) + b"\n")
        count += 1
    hasher.update((prev_hash or "").encode())
    return _Digest(count, hasher.hexdigest())


def _message(checkpoint: Any, instance_id: str) -> bytes:
    """Return the bytes a checkpoint's signature covers: its deployment, window, rows, chain link and retention."""
    return "|".join(
        (
            _MESSAGE_PREFIX,
            instance_id,
            _iso(checkpoint.window_start),
            _iso(checkpoint.window_end),
            str(checkpoint.row_count),
            checkpoint.hash,
            checkpoint.prev_hash or "",
            str(checkpoint.retention_days),
            "1" if checkpoint.legal_hold else "0",
        )
    ).encode()


def _ed25519() -> Any:
    try:
        module: Any = importlib.import_module("cryptography.hazmat.primitives.asymmetric.ed25519")
    except ImportError:
        module = None
    return optional_deps.require(module, package="cryptography", extra="audit", feature="Audit checkpoint signing")


def _key_id(public_key: Any) -> str:
    from cryptography.hazmat.primitives import serialization

    raw = public_key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    return hashlib.sha256(raw).hexdigest()[:16]


@functools.lru_cache(maxsize=4)
def _signer(signing_key: str) -> tuple[Any, str]:
    """Load the private key once; return ``(private_key, key_id)``."""
    ed25519 = _ed25519()
    try:
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(base64.b64decode(signing_key, validate=True))
    except (ValueError, binascii.Error):
        raise ValueError("audit.signing_key must be the base64 of a 32-byte Ed25519 private key") from None
    return private_key, _key_id(private_key.public_key())


def _public_key(public_key: str) -> Any:
    ed25519 = _ed25519()
    try:
        return ed25519.Ed25519PublicKey.from_public_bytes(base64.b64decode(public_key, validate=True))
    except (ValueError, binascii.Error):
        raise ValueError("the public key must be the base64 of a 32-byte Ed25519 public key") from None


def _verifiers(public_key: str | Sequence[str] | None) -> dict[str, Any]:
    """Load the verifying keys, by the key id a checkpoint records; empty when signatures are not checked.

    Raises:
        ValueError: When a key is malformed, or a sequence of keys is empty.
    """
    if public_key is None:
        return {}
    encoded = [public_key] if isinstance(public_key, str) else list(public_key)
    if not encoded:
        raise ValueError("pass at least one public key, or None to check hashes and chain only")
    loaded = [_public_key(key) for key in encoded]
    return {_key_id(key): key for key in loaded}


def _sign(settings: AuditSettings, message: bytes) -> tuple[str | None, str | None]:
    global _unsigned_warned
    if settings.signing_key is None:
        if not _unsigned_warned:
            logger.warning("audit.signing_key is not set: checkpoints are hash-chained but not signed")
            _unsigned_warned = True
        return None, None
    private_key, key_id = _signer(settings.signing_key)
    return base64.b64encode(private_key.sign(message)).decode(), key_id


def _sink_name(start: datetime, instance_id: str) -> str:
    return f"checkpoint-{instance_id}-{as_utc(start).strftime('%Y%m%dT%H%M%SZ')}.json"


def _sink_payload(checkpoint: Any, instance_id: str) -> bytes:
    document = {
        "v": 1,
        "instance_id": instance_id,
        "window_start": _iso(checkpoint.window_start),
        "window_end": _iso(checkpoint.window_end),
        "row_count": checkpoint.row_count,
        "hash": checkpoint.hash,
        "prev_hash": checkpoint.prev_hash,
        "signature": checkpoint.signature,
        "key_id": checkpoint.key_id,
        "retention_days": checkpoint.retention_days,
        "legal_hold": checkpoint.legal_hold,
    }
    return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()


def _sealed(checkpoint: Any, settings: AuditSettings, instance_id: str) -> Any:
    """Sign ``checkpoint`` (when a key is configured) and return it."""
    checkpoint.signature, checkpoint.key_id = _sign(settings, _message(checkpoint, instance_id))
    return checkpoint


def _recorded_retention(document: bytes | None) -> tuple[int, bool] | None:
    """Return the ``(retention_days, legal_hold)`` a sink copy records, or ``None`` when it records none."""
    if document is None:
        return None
    try:
        fields = json.loads(document)
    except ValueError:  # not JSON, or not UTF-8: not a copy this code wrote
        return None
    if not isinstance(fields, dict):
        return None
    retention_days, legal_hold = fields.get("retention_days"), fields.get("legal_hold")
    if type(retention_days) is not int or type(legal_hold) is not bool:
        return None
    return retention_days, legal_hold


def _copy_to_sink(sink: AuditSink, checkpoint: Any, settings: AuditSettings, instance_id: str) -> Any:
    """Write ``checkpoint`` to the sink and return the checkpoint to store.

    Sealing the same rows gives the same bytes (Ed25519 signatures are deterministic) except for the
    ``retention_days`` and ``legal_hold`` taken from the settings in force. So when the sink already holds
    the window, as after a crash between the sink write and the database commit, and the settings have
    changed since, the window is resealed under the values that copy records: that checkpoint is stored
    when it reproduces the copy byte for byte, and anything else is a conflict.

    Raises:
        AuditSinkConflictError: When the sink holds content for the window that this window does not
            reproduce.
    """
    from jasil.audit.models import AuditCheckpoint

    name = _sink_name(checkpoint.window_start, instance_id)
    try:
        sink.write_once(name, _sink_payload(checkpoint, instance_id))
        return checkpoint
    except AuditSinkConflictError:
        stored = sink.read(name)
        recorded = _recorded_retention(stored)
        if recorded is None:
            raise
        resealed = _sealed(
            AuditCheckpoint(
                window_start=checkpoint.window_start,
                window_end=checkpoint.window_end,
                row_count=checkpoint.row_count,
                hash=checkpoint.hash,
                prev_hash=checkpoint.prev_hash,
                retention_days=recorded[0],
                legal_hold=recorded[1],
            ),
            settings,
            instance_id,
        )
        if _sink_payload(resealed, instance_id) != stored:
            raise
        logger.warning(
            "Audit checkpoint %s was already in the external sink under retention_days=%d, legal_hold=%s; "
            "storing that copy",
            name,
            *recorded,
        )
        return resealed


def _window_end(start: datetime, window: timedelta) -> datetime:
    """Return the end of the window starting at ``start``: the next multiple of ``window`` since the epoch.

    A window that starts on the grid is ``window`` long. After ``checkpoint_window_minutes`` changes, the
    first window is shortened to rejoin the new grid, which divides a day, so no window straddles two daily
    partitions.
    """
    span = window.total_seconds()
    return datetime.fromtimestamp((as_utc(start).timestamp() // span + 1) * span, UTC)


def seal_pending_windows(now: datetime | None = None) -> int:
    """
    Seal every window that has passed its grace period.

    Reads each window on a short session, writes it to the external sink (if configured) with no
    transaction open, then stores the checkpoint on another short session. The sink comes first: if it is
    down the checkpoint is not stored and the window is retried on the next run, so the copy outside the
    database is never missing a checkpoint that the database has. A retry after a crash between the two
    writes stores the checkpoint the sink already holds (see :func:`_copy_to_sink`).

    Args:
        now: The instant to seal up to; the platform clock when omitted.

    Returns:
        How many checkpoints were written.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: On a database error.
        AuditSinkConflictError: When the sink already holds different content for a window.
        RuntimeError: When the newest checkpoint ends after ``now``.
    """
    from jasil.audit.models import AuditCheckpoint, AuditLog

    settings = get_settings().audit
    now = now or service.now()
    window = timedelta(minutes=settings.checkpoint_window_minutes)
    grace = timedelta(minutes=settings.checkpoint_grace_minutes)
    sink = platform_runtime.get_active_platform().audit_sink if platform_runtime.is_platform_active() else None
    written = 0
    sessionmaker = jasil_orm.get_sessionmaker()

    with sessionmaker() as db:
        last = db.scalars(select(AuditCheckpoint).order_by(AuditCheckpoint.window_start.desc()).limit(1)).first()
        if last is not None:
            start, prev_hash = as_utc(last.window_end), last.hash
            if start > now:
                # No window this clock sealed can end after it: a checkpoint inserted ahead of time, or a clock
                # that went back. Resuming from it would silently seal nothing until that moment.
                raise RuntimeError(
                    f"the newest checkpoint ends at {start.isoformat()}, after now ({now.isoformat()}); "
                    "sealing stops until it is investigated"
                )
        else:
            first = db.scalar(select(func.min(AuditLog.occurred_at)))
            if first is None:
                return 0
            epoch = as_utc(first).timestamp()
            start = datetime.fromtimestamp(epoch - epoch % window.total_seconds(), UTC)
            prev_hash = None
        instance_id = service.ensure_instance_id(service.engine_of(db))

    while written < MAX_WINDOWS_PER_RUN:
        end = _window_end(start, window)
        if end + grace > now:
            break
        with sessionmaker() as db:
            digest = _digest(db, start, end, prev_hash)
        checkpoint = _sealed(
            AuditCheckpoint(
                window_start=start,
                window_end=end,
                row_count=digest.row_count,
                hash=digest.hash,
                prev_hash=prev_hash,
                retention_days=settings.retention_days,
                legal_hold=settings.legal_hold,
            ),
            settings,
            instance_id,
        )
        if sink is not None:
            checkpoint = _copy_to_sink(sink, checkpoint, settings, instance_id)
        with sessionmaker() as db:
            db.add(checkpoint)
            service.note_maintenance(db, last_checkpoint_at=end)
            db.commit()
        written += 1
        start, prev_hash = end, digest.hash
    return written


def retention_horizon(checkpoint: Any) -> datetime | None:
    """Return how far retention may have pruned under ``checkpoint``: its ``window_end - retention_days``.

    ``None`` when that reaches before the first representable instant, which allows no prune. A stored
    retention is whatever the table holds, not only what the settings accept, so it is compared as an
    integer before any date arithmetic.
    """
    end = as_utc(checkpoint.window_end)
    if checkpoint.retention_days >= (end - _EARLIEST).days:
        return None
    return end - timedelta(days=checkpoint.retention_days)


def _pruning_horizon(db: Session, verifiers: Mapping[str, Any], instance_id: str) -> datetime | None:
    """Return the latest window end any checkpoint allows to have been pruned; ``None`` when none does.

    A checkpoint records the retention in force when it was sealed, so ``window_end - retention_days`` is
    as far as retention could reach under it, and retention prunes no further than the newest checkpoint
    allows. With keys only a checkpoint whose signature verifies under one of them counts, so the retention
    is the one the application sealed, not one whoever can write the table chose. A checkpoint sealed under legal hold, or
    with retention off, allows nothing.
    """
    from jasil.audit.models import AuditCheckpoint

    candidates = sorted(
        (
            (horizon, checkpoint)
            for checkpoint in db.scalars(
                select(AuditCheckpoint).where(AuditCheckpoint.retention_days > 0, AuditCheckpoint.legal_hold.is_(False))
            )
            if (horizon := retention_horizon(checkpoint)) is not None
        ),
        key=lambda candidate: candidate[0],
        reverse=True,
    )
    for horizon, checkpoint in candidates:
        if not verifiers or _signature_ok(verifiers, checkpoint, instance_id):
            return horizon
    return None


def verify_checkpoints(
    public_key: str | Sequence[str] | None, from_ts: datetime, to_ts: datetime
) -> VerificationReport:
    """
    Recompute the checkpoint windows in a range and report each as ``ok`` or what is wrong with it.

    For every checkpoint whose window lies inside ``[from_ts, to_ts]`` the window's rows are read and
    hashed again and compared with the stored hash and row count, the link to the previous checkpoint is
    checked, and the signature is verified when keys are given. A gap between two checkpoints that reaches
    into the range is reported as ``missing_window``, whether or not a checkpoint inside the range borders
    it, and rows dated before the first checkpoint, which no window accounts for, as one ``row_mismatch``
    window that ends where the first checkpoint starts. Time after the newest checkpoint is not sealed yet
    and is not reported. A window whose rows are all gone is accepted only if some checkpoint allows it:
    one not sealed under legal hold whose ``window_end`` minus its recorded ``retention_days`` is at or past
    the window's end, and whose signature verifies when keys are given. Its chain link and signature are
    still checked, its rows cannot be.

    The host or the observability product decides when to call this; to raise an alarm, stage or record
    ``audit.integrity.violation`` for each window that is not ``ok``.

    Args:
        public_key: Base64 of a 32-byte Ed25519 public key; or a sequence of them, every key the range was
            signed with across a rotation, each checkpoint checked against the one its key id names; or
            ``None`` to check hashes and chain only.
        from_ts: Earliest window start to verify.
        to_ts: Latest window end to verify; at most :data:`MAX_VERIFY_RANGE` (366 days) after ``from_ts``.

    Returns:
        One entry per checkpoint window and per gap, oldest first.

    Raises:
        ValueError: When a key is not valid, ``public_key`` is an empty sequence, ``from_ts`` is after
            ``to_ts``, or the range is longer than :data:`MAX_VERIFY_RANGE`.
        ImportError: When a key is given and ``cryptography`` is not installed (``jasil[audit]``).
    """
    from jasil.audit.models import AuditCheckpoint, AuditLog, Instance

    start_bound, end_bound = as_utc(from_ts), as_utc(to_ts)
    if start_bound > end_bound:
        raise ValueError(f"from_ts ({start_bound.isoformat()}) is after to_ts ({end_bound.isoformat()})")
    if end_bound - start_bound > MAX_VERIFY_RANGE:
        raise ValueError(f"the range may cover at most {MAX_VERIFY_RANGE.days} days; verify a longer one in parts")
    verifiers = _verifiers(public_key)
    results: list[WindowVerification] = []

    with jasil_orm.get_sessionmaker()() as db:
        first = db.scalars(select(AuditCheckpoint).order_by(AuditCheckpoint.window_start).limit(1)).first()
        if first is None:
            return VerificationReport(ok=True, signed=bool(verifiers), windows=[])
        first_start = as_utc(first.window_start)
        if start_bound < first_start:
            unsealed_end = min(first_start, end_bound)
            orphans = db.scalar(
                select(func.count())
                .select_from(AuditLog)
                .where(AuditLog.occurred_at >= start_bound, AuditLog.occurred_at < unsealed_end)
            )
            if orphans:
                results.append(
                    WindowVerification(
                        window_start=start_bound,
                        window_end=unsealed_end,
                        status=VerificationStatus.ROW_MISMATCH,
                        expected_rows=0,
                        actual_rows=orphans,
                    )
                )
        previous = db.scalars(
            select(AuditCheckpoint)
            .where(AuditCheckpoint.window_start < start_bound)
            .order_by(AuditCheckpoint.window_start.desc())
            .limit(1)
        ).first()
        checkpoints = db.scalars(
            select(AuditCheckpoint)
            .where(AuditCheckpoint.window_start >= start_bound, AuditCheckpoint.window_end <= end_bound)
            .order_by(AuditCheckpoint.window_start)
        ).all()
        # The deployment is part of every signature, so a checkpoint replayed from another one fails.
        instance_id = db.scalar(select(Instance.instance_id).where(Instance.id == 1)) or ""
        horizon = _pruning_horizon(db, verifiers, instance_id)

        for checkpoint in checkpoints:
            start, end = as_utc(checkpoint.window_start), as_utc(checkpoint.window_end)
            if previous is not None and as_utc(previous.window_end) != start:
                results.append(_missing(as_utc(previous.window_end), start))
            expected_prev = previous.hash if previous is not None else None
            digest = _digest(db, start, end, checkpoint.prev_hash)
            rows_match = digest.row_count == checkpoint.row_count and digest.hash == checkpoint.hash
            pruned = (
                not rows_match
                and digest.row_count == 0
                and checkpoint.row_count > 0
                and horizon is not None
                and end <= horizon
            )
            status = VerificationStatus.OK if rows_match or pruned else VerificationStatus.ROW_MISMATCH
            if status is VerificationStatus.OK and verifiers and not _signature_ok(verifiers, checkpoint, instance_id):
                status = VerificationStatus.BAD_SIGNATURE
            if status is VerificationStatus.OK and checkpoint.prev_hash != expected_prev:
                # With no predecessor in the database the first checkpoint must start the chain; one that
                # names a predecessor means the head of the chain was removed.
                status = VerificationStatus.BROKEN_CHAIN
            results.append(
                WindowVerification(
                    window_start=start,
                    window_end=end,
                    status=status,
                    pruned=pruned,
                    expected_rows=checkpoint.row_count,
                    actual_rows=None if pruned else digest.row_count,
                )
            )
            previous = checkpoint

        if previous is not None and as_utc(previous.window_end) < end_bound:
            # The gap after the last window checked: the checkpoints that filled it are gone, or no later
            # checkpoint exists and the rest of the range is simply not sealed yet.
            following = db.scalars(
                select(AuditCheckpoint)
                .where(AuditCheckpoint.window_start >= previous.window_end)
                .order_by(AuditCheckpoint.window_start)
                .limit(1)
            ).first()
            if following is not None and as_utc(following.window_start) != as_utc(previous.window_end):
                results.append(_missing(as_utc(previous.window_end), as_utc(following.window_start)))

    return VerificationReport(
        ok=all(result.status is VerificationStatus.OK for result in results),
        signed=bool(verifiers),
        windows=results,
    )


def _missing(start: datetime, end: datetime) -> WindowVerification:
    return WindowVerification(window_start=start, window_end=end, status=VerificationStatus.MISSING_WINDOW)


def _signature_ok(verifiers: Mapping[str, Any], checkpoint: Any, instance_id: str) -> bool:
    """Whether the checkpoint's signature verifies under the key its key id names, among ``verifiers``."""
    from cryptography.exceptions import InvalidSignature

    verifier = verifiers.get(checkpoint.key_id) if checkpoint.key_id is not None else None
    if checkpoint.signature is None or verifier is None:
        return False
    try:
        verifier.verify(base64.b64decode(checkpoint.signature, validate=True), _message(checkpoint, instance_id))
    except (InvalidSignature, ValueError, binascii.Error):
        return False
    return True
