"""PostgreSQL conformance for selective and competing durable-job workers.

The normal test matrix points the whole suite at PostgreSQL 17. These tests are
skipped on the SQLite developer default and the MySQL/MariaDB matrix jobs; no SQL
or session is mocked here.
"""

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import event as sqlalchemy_event

import jasil.admin as jasil_admin
import jasil.jobs._worker_registry as worker_registry
import jasil.jobs.crud as jobs_crud
from jasil.events import new_event
from jasil.jobs.models import ProcessingJob
from jasil.jobs.registry import JobHandlerRegistry
from jasil.jobs.runner import JobRunner

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _postgres_only(db_engine):
    if db_engine.dialect.name != "postgresql":
        pytest.skip("requires the real PostgreSQL test service")


class FixedClock:
    def __init__(self, moment: datetime = T0) -> None:
        self._moment = moment

    def now(self) -> datetime:
        return self._moment


def _runner(registry, session_factory, worker_id, *, queues, batch_size=10, clock=None):
    return JobRunner(
        registry=registry,
        clock=clock or FixedClock(),
        session_factory=session_factory,
        worker_id=worker_id,
        lease_seconds=60,
        batch_size=batch_size,
        backoff_base_seconds=1,
        backoff_max_seconds=2,
        queues=queues,
    )


def _enqueue(db, *, subscriber, queue, item):
    return jobs_crud.enqueue_job(
        new_event("work.created", {"item": item}, source="test"),
        subscriber,
        queue=queue,
        max_attempts=3,
        now=T0,
        db=db,
    )


def test_workers_selecting_different_queues_only_run_their_own_handlers(db, session_factory):
    registry = JobHandlerRegistry()
    seen: dict[str, list[int]] = {"campaign": [], "intake": []}
    registry.register("work.created", "campaign-handler", lambda event: seen["campaign"].append(event.payload["item"]))
    registry.register("work.created", "intake-handler", lambda event: seen["intake"].append(event.payload["item"]))
    _enqueue(db, subscriber="campaign-handler", queue="campaign", item=1)
    _enqueue(db, subscriber="intake-handler", queue="intake", item=2)
    campaign = _runner(registry, session_factory, "campaign-worker", queues=("campaign",))
    intake = _runner(registry, session_factory, "intake-worker", queues=("intake",))

    with ThreadPoolExecutor(max_workers=2) as executor:
        processed = list(executor.map(lambda runner: runner.run_once(), (campaign, intake)))

    assert processed == [1, 1]
    assert seen == {"campaign": [1], "intake": [2]}


def test_same_queue_workers_process_each_job_once_with_skip_locked(db, db_engine, session_factory):
    registry = JobHandlerRegistry()
    seen: list[int] = []
    seen_lock = threading.Lock()

    def handle(event):
        with seen_lock:
            seen.append(event.payload["item"])

    registry.register("work.created", "handler", handle)
    for item in range(20):
        _enqueue(db, subscriber="handler", queue="campaign", item=item)
    first = _runner(registry, session_factory, "worker-1", queues=("campaign",), batch_size=10)
    second = _runner(registry, session_factory, "worker-2", queues=("campaign",), batch_size=10)
    statements: list[str] = []

    def record_statement(_connection, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement)

    sqlalchemy_event.listen(db_engine, "before_cursor_execute", record_statement)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            processed = list(executor.map(lambda runner: runner.run_once(), (first, second)))
    finally:
        sqlalchemy_event.remove(db_engine, "before_cursor_execute", record_statement)

    assert processed == [10, 10]
    assert sorted(seen) == list(range(20))
    assert any("FOR UPDATE SKIP LOCKED" in statement.upper() for statement in statements)
    db.rollback()
    assert db.query(ProcessingJob).filter_by(status=jobs_crud.STATUS_COMPLETED).count() == 20


def test_unselected_queue_stays_pending_until_a_matching_worker_arrives(db, session_factory):
    registry = JobHandlerRegistry()
    seen: list[int] = []
    registry.register("work.created", "handler", lambda event: seen.append(event.payload["item"]))
    _enqueue(db, subscriber="handler", queue="maintenance", item=1)

    assert _runner(registry, session_factory, "wrong-worker", queues=("campaign",)).run_once() == 0
    db.rollback()
    assert db.query(ProcessingJob).one().status == jobs_crud.STATUS_PENDING

    assert _runner(registry, session_factory, "right-worker", queues=("maintenance",)).run_once() == 1
    assert seen == [1]


def test_a_crashed_workers_lease_is_reclaimed_and_drained(db, session_factory):
    registry = JobHandlerRegistry()
    seen: list[int] = []
    registry.register("work.created", "handler", lambda event: seen.append(event.payload["item"]))
    _enqueue(db, subscriber="handler", queue="maintenance", item=1)
    claimed = jobs_crud.claim_jobs(
        worker_id="crashed-worker",
        limit=1,
        lease_seconds=60,
        now=T0,
        db=db,
        queues=("maintenance",),
    )
    assert len(claimed) == 1

    with session_factory() as reaper_session:
        assert jobs_crud.reclaim_expired_leases(now=T0 + timedelta(seconds=61), db=reaper_session) == 1

    assert (
        _runner(
            registry,
            session_factory,
            "replacement-worker",
            queues=("maintenance",),
            clock=FixedClock(T0 + timedelta(seconds=61)),
        ).run_once()
        == 1
    )
    assert seen == [1]


def test_heartbeat_rows_distinguish_running_stale_and_graceful_stop(db):
    now = datetime.now(UTC)
    worker_registry.record_worker_start(
        "00000000-0000-4000-8000-000000000011",
        started_at=now,
        queues=("campaign",),
        role="campaign",
        label="running",
        metadata=None,
        db=db,
    )
    worker_registry.record_worker_start(
        "00000000-0000-4000-8000-000000000012",
        started_at=now - timedelta(hours=2),
        queues=None,
        role=None,
        label="stale",
        metadata=None,
        db=db,
    )
    worker_registry.record_worker_heartbeat(
        "00000000-0000-4000-8000-000000000012",
        started_at=now - timedelta(hours=2),
        now=now - timedelta(hours=1),
        queues=None,
        role=None,
        label="stale",
        metadata=None,
        db=db,
    )
    worker_registry.record_worker_start(
        "00000000-0000-4000-8000-000000000013",
        started_at=now - timedelta(minutes=5),
        queues=("maintenance",),
        role=None,
        label="stopped",
        metadata=None,
        db=db,
    )
    worker_registry.record_worker_stop(
        "00000000-0000-4000-8000-000000000013",
        now=now,
        db=db,
    )

    summary = jasil_admin.get_workers_summary(stale_after_seconds=60)

    assert (summary.running, summary.stale, summary.stopped) == (1, 1, 1)
    assert {worker.label: worker.status for worker in summary.workers} == {
        "running": "running",
        "stale": "stale",
        "stopped": "stopped",
    }


def test_queue_and_worker_admin_summaries_match_concurrent_claims(db, session_factory):
    now = datetime.now(UTC)
    worker_ids = (
        "00000000-0000-4000-8000-000000000021",
        "00000000-0000-4000-8000-000000000022",
    )
    for item in range(10):
        jobs_crud.enqueue_job(
            new_event("work.created", {"item": item}, source="test"),
            "handler",
            queue="campaign",
            max_attempts=3,
            now=now - timedelta(seconds=1),
            db=db,
        )
    for worker_id in worker_ids:
        worker_registry.record_worker_start(
            worker_id,
            started_at=now,
            queues=("campaign",),
            role="campaign",
            label=worker_id[-2:],
            metadata=None,
            db=db,
        )

    def claim(worker_id):
        with session_factory() as claim_session:
            return len(
                jobs_crud.claim_jobs(
                    worker_id=worker_id,
                    limit=5,
                    lease_seconds=60,
                    now=now,
                    db=claim_session,
                    queues=("campaign",),
                )
            )

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sorted(executor.map(claim, worker_ids)) == [5, 5]
    jobs = jasil_admin.get_jobs_summary()
    workers = jasil_admin.get_workers_summary(stale_after_seconds=60)

    campaign = next(queue for queue in jobs.by_queue if queue.queue == "campaign")
    assert (campaign.pending, campaign.claimed) == (0, 10)
    assert sorted(worker.active_claimed_jobs for worker in workers.workers) == [5, 5]
