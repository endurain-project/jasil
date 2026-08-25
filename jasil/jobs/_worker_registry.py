"""Internal persistence for durable-worker lifecycle telemetry."""

from collections.abc import Iterable
from datetime import datetime
from typing import Any, cast

from sqlalchemy import CursorResult, and_, delete, func, or_, select
from sqlalchemy.orm import Session

from jasil._core.limits import MAX_WORKER_LABEL_LENGTH, MAX_WORKER_ROLE_LENGTH, check_length
from jasil._core.pruning import PRUNE_BATCH_SIZE, PRUNE_MAX_BATCHES
from jasil._core.timestamps import age_seconds
from jasil.jobs.crud import STATUS_CLAIMED
from jasil.jobs.models import JobWorker, ProcessingJob
from jasil.jobs.registry import normalize_queue_selector
from jasil.jobs.schema import WorkerInfo, WorkersSummary, WorkerStatus


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
    _validate_metadata(role=role, label=label)
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
    worker.worker_metadata = dict(metadata) if metadata is not None else None
    worker.active_claimed_jobs = _active_claim_count(instance_id, db)
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
    worker.active_claimed_jobs = _active_claim_count(instance_id, db)
    db.commit()


def record_worker_stop(instance_id: str, *, now: datetime, db: Session) -> None:
    """Stamp a graceful stop and the final active-claim count."""
    worker = db.get(JobWorker, instance_id)
    if worker is None:
        return
    worker.last_heartbeat_at = now
    worker.stopped_at = now
    worker.active_claimed_jobs = _active_claim_count(instance_id, db)
    db.commit()


def get_workers_summary(*, now: datetime, stale_after_seconds: float, db: Session) -> WorkersSummary:
    """Read retained workers and derive their current operator status."""
    if stale_after_seconds <= 0:
        raise ValueError("stale_after_seconds must be greater than zero")
    rows = db.execute(select(JobWorker).order_by(JobWorker.started_at.desc())).scalars().all()
    claim_rows = db.execute(
        select(ProcessingJob.locked_by, func.count())
        .where(ProcessingJob.status == STATUS_CLAIMED, ProcessingJob.locked_by.is_not(None))
        .group_by(ProcessingJob.locked_by)
    ).all()
    active_claims: dict[str, int] = {
        instance_id: int(count) for instance_id, count in claim_rows if instance_id is not None
    }
    workers: list[WorkerInfo] = []
    counts = {"running": 0, "stale": 0, "stopped": 0}
    for row in rows:
        status: WorkerStatus
        if row.stopped_at is not None:
            status = "stopped"
        elif (age_seconds(row.last_heartbeat_at, now) or 0.0) > stale_after_seconds:
            status = "stale"
        else:
            status = "running"
        counts[status] += 1
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
            )
        )
    return WorkersSummary(
        stale_after_seconds=stale_after_seconds,
        total_workers=len(workers),
        running=counts["running"],
        stale=counts["stale"],
        stopped=counts["stopped"],
        workers=workers,
    )


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


def _active_claim_count(instance_id: str, db: Session) -> int:
    return int(
        db.execute(
            select(func.count())
            .select_from(ProcessingJob)
            .where(ProcessingJob.status == STATUS_CLAIMED, ProcessingJob.locked_by == instance_id)
        ).scalar_one()
    )


def _validate_metadata(*, role: str | None, label: str | None) -> None:
    if role is not None:
        check_length(role, field="role", limit=MAX_WORKER_ROLE_LENGTH)
    if label is not None:
        check_length(label, field="label", limit=MAX_WORKER_LABEL_LENGTH)
