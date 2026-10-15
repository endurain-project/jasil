"""Internal persistence for durable-worker lifecycle telemetry."""

from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any, cast

from sqlalchemy import CursorResult, and_, case, delete, func, or_, select
from sqlalchemy.orm import Session

from jasil._core.cursors import decode_cursor, encode_cursor
from jasil._core.pruning import PRUNE_BATCH_SIZE, PRUNE_MAX_BATCHES
from jasil._core.timestamps import age_seconds
from jasil.jobs._worker_metadata import normalize_worker_metadata
from jasil.jobs.crud import STATUS_CLAIMED
from jasil.jobs.models import JobAttempt, JobWorker, ProcessingJob
from jasil.jobs.registry import normalize_queue_selector
from jasil.jobs.schema import WorkerInfo, WorkersSummary, WorkerStatus

DEFAULT_WORKER_PAGE_SIZE = 100
MAX_WORKER_PAGE_SIZE = 500


def record_worker_start(
    instance_id: str,
    *,
    started_at: datetime,
    queues: Iterable[str] | None,
    role: str | None,
    label: str | None,
    metadata: dict[str, Any] | None,
    db: Session,
) -> None:
    """Insert or restore one worker instance row."""
    selected_queues = normalize_queue_selector(queues)
    normalized_metadata = normalize_worker_metadata(role=role, label=label, metadata=metadata)
    worker = db.get(JobWorker, instance_id)
    if worker is None:
        worker = JobWorker(instance_id=instance_id, started_at=started_at, last_heartbeat_at=started_at)
        db.add(worker)
    worker.started_at = started_at
    worker.last_heartbeat_at = started_at
    worker.stopped_at = None
    worker.queues = list(selected_queues) if selected_queues is not None else None
    worker.role = role
    worker.label = label
    worker.worker_metadata = normalized_metadata
    db.commit()


def record_worker_heartbeat(
    instance_id: str,
    *,
    started_at: datetime,
    now: datetime,
    queues: Iterable[str] | None,
    role: str | None,
    label: str | None,
    metadata: dict[str, Any] | None,
    db: Session,
) -> None:
    """Refresh a worker heartbeat, recreating a row after a transient start failure."""
    worker = db.get(JobWorker, instance_id)
    if worker is None:
        record_worker_start(
            instance_id,
            started_at=started_at,
            queues=queues,
            role=role,
            label=label,
            metadata=metadata,
            db=db,
        )
        worker = db.get(JobWorker, instance_id)
    if worker is None:
        raise RuntimeError("worker telemetry row was not persisted")
    worker.last_heartbeat_at = now
    db.commit()


def record_worker_stop(instance_id: str, *, now: datetime, db: Session) -> None:
    """Stamp a graceful stop and the final active-claim count."""
    worker = db.get(JobWorker, instance_id)
    if worker is None:
        return
    worker.last_heartbeat_at = now
    worker.stopped_at = now
    db.commit()


def get_workers_summary(
    *,
    now: datetime,
    stale_after_seconds: float,
    db: Session,
    limit: int = DEFAULT_WORKER_PAGE_SIZE,
    cursor: str | None = None,
    hours: int = 24,
    record_attempts: bool = True,
) -> WorkersSummary:
    """Read one bounded page of workers and derive global operator totals.

    ``hours`` windows the per-worker attempt counts (attempts that *ended* in it).
    With ``record_attempts`` false those counts are ``None``, not ``0``.
    """
    if stale_after_seconds <= 0:
        raise ValueError("stale_after_seconds must be greater than zero")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_WORKER_PAGE_SIZE:
        raise ValueError(f"limit must be an integer between 1 and {MAX_WORKER_PAGE_SIZE}")
    stale_before = now - timedelta(seconds=stale_after_seconds)
    total_workers, stopped, stale, running = db.execute(
        select(
            func.count(JobWorker.instance_id),
            func.coalesce(func.sum(case((JobWorker.stopped_at.is_not(None), 1), else_=0)), 0),
            func.coalesce(
                func.sum(
                    case(
                        (and_(JobWorker.stopped_at.is_(None), JobWorker.last_heartbeat_at < stale_before), 1),
                        else_=0,
                    )
                ),
                0,
            ),
            func.coalesce(
                func.sum(
                    case(
                        (and_(JobWorker.stopped_at.is_(None), JobWorker.last_heartbeat_at >= stale_before), 1),
                        else_=0,
                    )
                ),
                0,
            ),
        )
    ).one()
    # Jobs held by a worker that has stopped beating and never said goodbye: they
    # wait out their lease for the reaper, which is the number an operator
    # watching a crash wants. Active claims of *running* workers are not stuck.
    claimed_by_stale = int(
        db.execute(
            select(func.count())
            .select_from(ProcessingJob)
            .join(JobWorker, JobWorker.instance_id == ProcessingJob.locked_by)
            .where(
                ProcessingJob.status == STATUS_CLAIMED,
                JobWorker.stopped_at.is_(None),
                JobWorker.last_heartbeat_at < stale_before,
            )
        ).scalar_one()
    )
    statement = select(JobWorker)
    if cursor is not None:
        cursor_started_at, cursor_instance_id = _decode_cursor(cursor)
        statement = statement.where(
            or_(
                JobWorker.started_at < cursor_started_at,
                and_(JobWorker.started_at == cursor_started_at, JobWorker.instance_id < cursor_instance_id),
            )
        )
    rows = list(
        db.execute(statement.order_by(JobWorker.started_at.desc(), JobWorker.instance_id.desc()).limit(limit + 1))
        .scalars()
        .all()
    )
    has_more = len(rows) > limit
    rows = rows[:limit]
    instance_ids = [row.instance_id for row in rows]
    claim_rows = (
        db.execute(
            select(ProcessingJob.locked_by, func.count())
            .where(ProcessingJob.status == STATUS_CLAIMED, ProcessingJob.locked_by.in_(instance_ids))
            .group_by(ProcessingJob.locked_by)
        ).all()
        if instance_ids
        else []
    )
    active_claims: dict[str, int] = {
        instance_id: int(count) for instance_id, count in claim_rows if instance_id is not None
    }
    window_start = now - timedelta(hours=hours)
    outcomes: dict[str, dict[str, int]] = {}
    last_finished: dict[str, datetime] = {}
    if record_attempts and instance_ids:
        for worker_id, outcome, count, finished in db.execute(
            select(JobAttempt.worker_id, JobAttempt.outcome, func.count(), func.max(JobAttempt.finished_at))
            .where(JobAttempt.worker_id.in_(instance_ids), JobAttempt.finished_at >= window_start)
            .group_by(JobAttempt.worker_id, JobAttempt.outcome)
        ).all():
            outcomes.setdefault(worker_id, {})[outcome] = int(count)
            if finished is not None and (worker_id not in last_finished or finished > last_finished[worker_id]):
                last_finished[worker_id] = finished
    workers: list[WorkerInfo] = []
    for row in rows:
        status: WorkerStatus
        if row.stopped_at is not None:
            status = "stopped"
        elif (age_seconds(row.last_heartbeat_at, now) or 0.0) > stale_after_seconds:
            status = "stale"
        else:
            status = "running"
        workers.append(
            WorkerInfo(
                instance_id=row.instance_id,
                started_at=row.started_at,
                last_heartbeat_at=row.last_heartbeat_at,
                stopped_at=row.stopped_at,
                queues=list(row.queues) if row.queues is not None else None,
                role=row.role,
                label=row.label,
                metadata=dict(row.worker_metadata) if row.worker_metadata is not None else None,
                # The persisted value is the latest heartbeat snapshot. Reads
                # derive the current count from processing_jobs so a crashed
                # worker does not retain phantom claims after the reaper runs.
                active_claimed_jobs=int(active_claims.get(row.instance_id, 0)),
                status=status,
                **_outcome_counts(outcomes.get(row.instance_id, {}), record_attempts),
                last_finished_at=last_finished.get(row.instance_id),
            )
        )
    return WorkersSummary(
        stale_after_seconds=stale_after_seconds,
        window_hours=hours,
        claimed_by_stale_workers=claimed_by_stale,
        total_workers=int(total_workers),
        running=int(running),
        stale=int(stale),
        stopped=int(stopped),
        workers=workers,
        next_cursor=_encode_cursor(rows[-1]) if has_more else None,
    )


def _outcome_counts(counts: dict[str, int], record_attempts: bool) -> dict[str, int | None]:
    """A worker's per-outcome attempt counts; ``None`` when attempts are not recorded."""
    if not record_attempts:
        return {key: None for key in ("completed", "failed", "dead_lettered", "lease_expired", "superseded")}
    return {
        "completed": counts.get("completed", 0),
        "failed": counts.get("failed", 0),
        "dead_lettered": counts.get("dead_letter", 0),
        "lease_expired": counts.get("lease_expired", 0),
        "superseded": counts.get("superseded", 0),
    }


def _encode_cursor(worker: JobWorker) -> str:
    return encode_cursor(worker.started_at, worker.instance_id)


def _decode_cursor(cursor: str) -> tuple[datetime, str]:
    return decode_cursor(cursor, name="worker summary")


def prune_worker_records_before(cutoff: datetime, *, db: Session, batch_size: int = PRUNE_BATCH_SIZE) -> int:
    """Delete stopped or stale worker rows before ``cutoff`` in bounded batches."""
    total = 0
    old_record = or_(
        JobWorker.stopped_at < cutoff,
        and_(JobWorker.stopped_at.is_(None), JobWorker.last_heartbeat_at < cutoff),
    )
    for _ in range(PRUNE_MAX_BATCHES):
        instance_ids = list(
            db.execute(select(JobWorker.instance_id).where(old_record).limit(batch_size)).scalars().all()
        )
        if not instance_ids:
            break
        deleted = cast(
            CursorResult[Any],
            db.execute(delete(JobWorker).where(JobWorker.instance_id.in_(instance_ids), old_record)),
        )
        db.commit()
        total += deleted.rowcount
        if len(instance_ids) < batch_size:
            break
    return total
