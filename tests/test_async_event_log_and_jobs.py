"""Async event-log and durable-job data-layer parity."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select

import jasil._core.pruning as pruning
import jasil.event_log.crud_async as event_log_crud
import jasil.jobs.crud_async as jobs_crud
import jasil.jobs.outbox_async as jobs_outbox
from jasil._core.limits import MAX_HANDLER_NAME_LENGTH, MAX_STORED_ERROR_LENGTH, MAX_WORKER_ID_LENGTH, fit_length
from jasil._core.sessions import commit_or_flush, commit_or_flush_async
from jasil._core.timestamps import as_utc
from jasil.event_log.models import EventLog
from jasil.event_log.recorder_async import AsyncEventLogRecorder
from jasil.event_log.statements import build_summary
from jasil.events import new_event
from jasil.jobs.models import EventOutbox, ProcessingJob
from jasil.jobs.statements import build_jobs_summary, insert_ignoring_duplicate

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
OLD = T0 - timedelta(days=90)
CUTOFF = T0 - timedelta(days=30)
SUBSCRIBER = "thumbnails.generate"


def _event(event_type: str = "activity.created", **kwargs):
    return new_event(event_type, kwargs.pop("payload", {"activity_id": 7}), source="test", **kwargs)


async def _row(db, event_id: str) -> EventLog:
    await db.rollback()  # recorder writes through its own committed session
    row = await db.get(EventLog, event_id)
    assert row is not None
    return row


async def _event_log_count(db) -> int:
    return (await db.execute(select(func.count()).select_from(EventLog))).scalar_one()


async def _job_count(db) -> int:
    return (await db.execute(select(func.count()).select_from(ProcessingJob))).scalar_one()


async def _outbox_count(db) -> int:
    return (await db.execute(select(func.count()).select_from(EventOutbox))).scalar_one()


async def _enqueue(event, db, *, subscriber=SUBSCRIBER, max_attempts=3, now=T0, available_at=None, commit=True):
    return await jobs_crud.enqueue_job(
        event,
        subscriber,
        max_attempts=max_attempts,
        now=now,
        db=db,
        available_at=available_at,
        commit=commit,
    )


class TestEventLogStatements:
    def test_build_summary_aggregates_pure_rows(self):
        failed = EventLog(
            id="failed",
            event_type="invoice.rendered",
            event_source="test",
            event_payload={},
            event_metadata={"request_id": "r-1"},
            status="failed",
            handler_name="h",
            error_message="boom",
            retry_count=0,
            created_at=T0,
        )

        summary = build_summary(
            hours=6,
            now=T0,
            status_rows=[
                ("activity.created", "published", 2),
                ("activity.created", "completed", 1),
                ("invoice.rendered", "failed", 1),
            ],
            latency_rows=[("activity.created", 20, 30)],
            pending_rows=[("activity.created", "published", 2, T0 - timedelta(seconds=5))],
            failure_rows=[failed],
        )

        by_type = {stats.event_type: stats for stats in summary.by_type}
        assert summary.window_hours == 6
        assert summary.total_events == 4
        assert by_type["activity.created"].published == 2
        assert by_type["activity.created"].avg_processing_time_ms == 20.0
        assert by_type["activity.created"].max_processing_time_ms == 30
        assert summary.pending[0].oldest_seconds == 5.0
        assert summary.recent_failures[0].event_metadata == {"request_id": "r-1"}

    def test_build_summary_orders_types_and_pending_groups(self):
        summary = build_summary(
            hours=24,
            now=T0,
            status_rows=[("zeta.happened", "queued", 1), ("alpha.happened", "dead_letter", 1)],
            latency_rows=[],
            pending_rows=[
                ("recent.thing", "published", 1, T0 - timedelta(seconds=1)),
                ("old.thing", "processing", 1, T0 - timedelta(seconds=30)),
            ],
            failure_rows=[],
        )

        assert [stats.event_type for stats in summary.by_type] == ["alpha.happened", "zeta.happened"]
        assert [group.event_type for group in summary.pending] == ["old.thing", "recent.thing"]


class TestEventLogLifecycleWrites:
    async def test_publishing_records_the_envelope(self, async_db):
        event = _event(metadata={"request_id": "r-1"})

        await event_log_crud.record_published(event, async_db)

        row = await _row(async_db, event.event_id)
        assert row.status == "published"
        assert row.event_type == "activity.created"
        assert row.event_source == "test"
        assert row.event_payload == {"activity_id": 7}
        assert row.event_metadata == {"request_id": "r-1"}

    async def test_empty_metadata_is_stored_as_null(self, async_db):
        event = _event()

        await event_log_crud.record_published(event, async_db)

        assert (await _row(async_db, event.event_id)).event_metadata is None

    async def test_a_durable_event_is_recorded_queued(self, async_db):
        event = _event()

        await event_log_crud.record_queued(event, async_db)

        assert (await _row(async_db, event.event_id)).status == "queued"

    async def test_processing_stamps_the_worker(self, async_db):
        event = _event()
        await event_log_crud.record_published(event, async_db)

        await event_log_crud.mark_processing(event.event_id, "worker-7", async_db)

        row = await _row(async_db, event.event_id)
        assert row.status == "processing"
        assert row.worker_id == "worker-7"
        assert row.processed_at is not None

    async def test_completion_stamps_the_handler_and_duration(self, async_db):
        event = _event()
        await event_log_crud.record_published(event, async_db)

        await event_log_crud.mark_completed(event.event_id, "render_invoice", 42, async_db)

        row = await _row(async_db, event.event_id)
        assert row.status == "completed"
        assert row.handler_name == "render_invoice"
        assert row.processing_time_ms == 42
        assert row.completed_at is not None

    async def test_failure_stores_the_reason(self, async_db):
        event = _event()
        await event_log_crud.record_published(event, async_db)

        await event_log_crud.mark_failed(event.event_id, "render_invoice", "upstream refused", 7, async_db)

        row = await _row(async_db, event.event_id)
        assert row.status == "failed"
        assert row.error_message == "upstream refused"

    async def test_long_values_are_clamped_to_their_columns(self, async_db):
        event = _event()
        await event_log_crud.record_published(event, async_db)

        await event_log_crud.mark_processing(event.event_id, "w" * (MAX_WORKER_ID_LENGTH * 2), async_db)
        await event_log_crud.mark_failed(
            event.event_id,
            "subscriber," * 200,
            "x" * (MAX_STORED_ERROR_LENGTH * 2),
            1,
            async_db,
        )

        row = await _row(async_db, event.event_id)
        assert len(row.worker_id) == MAX_WORKER_ID_LENGTH
        assert len(row.handler_name) == MAX_HANDLER_NAME_LENGTH
        assert len(row.error_message) == MAX_STORED_ERROR_LENGTH
        assert fit_length("subscriber," * 200, MAX_HANDLER_NAME_LENGTH).endswith("...")

    async def test_a_transition_for_an_unknown_event_is_a_no_op(self, async_db):
        await event_log_crud.mark_completed("never-published", "handler", 1, async_db)


class TestEventLogSummary:
    async def test_an_empty_log_summarises_to_nothing(self, async_db):
        summary = await event_log_crud.get_event_log_summary(async_db)

        assert summary.total_events == 0
        assert summary.by_type == []
        assert summary.pending == []
        assert summary.recent_failures == []

    async def test_the_window_is_reported_back(self, async_db):
        assert (await event_log_crud.get_event_log_summary(async_db, hours=6)).window_hours == 6

    async def test_events_are_counted_per_type_and_status(self, async_db):
        for _ in range(2):
            await event_log_crud.record_published(_event("order.created"), async_db)
        completed = _event("order.created")
        await event_log_crud.record_published(completed, async_db)
        await event_log_crud.mark_completed(completed.event_id, "h", 10, async_db)
        await event_log_crud.record_queued(_event("invoice.rendered"), async_db)

        summary = await event_log_crud.get_event_log_summary(async_db)

        by_type = {stats.event_type: stats for stats in summary.by_type}
        assert summary.total_events == 4
        assert by_type["order.created"].total == 3
        assert by_type["order.created"].published == 2
        assert by_type["order.created"].completed == 1
        assert by_type["invoice.rendered"].queued == 1

    async def test_pending_groups_and_recent_failures_match_the_sync_contract(self, async_db):
        await event_log_crud.record_published(_event("pending.thing"), async_db)
        failed = _event("failed.thing", metadata={"request_id": "r-9"})
        await event_log_crud.record_published(failed, async_db)
        await event_log_crud.mark_failed(failed.event_id, "render_invoice", "boom", 5, async_db)
        queued = _event("queued.thing")
        await event_log_crud.record_queued(queued, async_db)

        summary = await event_log_crud.get_event_log_summary(async_db, failure_limit=1)

        assert [group.event_type for group in summary.pending] == ["pending.thing"]
        assert [failure.id for failure in summary.recent_failures] == [failed.event_id]
        assert summary.recent_failures[0].handler_name == "render_invoice"
        assert summary.recent_failures[0].event_metadata == {"request_id": "r-9"}

    async def test_events_outside_the_window_are_excluded(self, async_db):
        async_db.add(
            EventLog(
                id="ancient",
                event_type="order.created",
                event_source="api",
                event_payload={},
                status="completed",
                created_at=datetime.now(UTC) - timedelta(days=2),
            )
        )
        await async_db.commit()

        assert (await event_log_crud.get_event_log_summary(async_db, hours=1)).total_events == 0


class TestAsyncRecorder:
    @pytest.fixture
    def recorder(self, async_session_factory):
        return AsyncEventLogRecorder()

    async def test_it_records_a_publication(self, recorder, async_db):
        event = _event()

        await recorder.record_published(event)

        assert (await _row(async_db, event.event_id)).status == "published"

    async def test_it_records_a_durable_queueing(self, recorder, async_db):
        event = _event()

        await recorder.record_queued(event)

        assert (await _row(async_db, event.event_id)).status == "queued"

    async def test_tracking_records_processing_then_completed(self, recorder, async_db):
        event = _event()
        await recorder.record_published(event)

        async with recorder.track(event, worker_id="worker-1", handler_name="h"):
            pass

        row = await _row(async_db, event.event_id)
        assert row.status == "completed"
        assert row.worker_id == "worker-1"
        assert row.handler_name == "h"
        assert row.processing_time_ms >= 0

    async def test_the_in_process_bus_can_skip_the_processing_row(self, recorder, async_db):
        event = _event()
        await recorder.record_published(event)

        async with recorder.track(event, worker_id="w", handler_name="h", record_processing=False):
            pass

        row = await _row(async_db, event.event_id)
        assert row.status == "completed"
        assert row.worker_id is None

    async def test_a_handler_failure_is_recorded_and_re_raised(self, recorder, async_db):
        event = _event()
        await recorder.record_published(event)

        with pytest.raises(RuntimeError, match="boom"):
            async with recorder.track(event, worker_id="w", handler_name="h"):
                raise RuntimeError("boom")

        row = await _row(async_db, event.event_id)
        assert row.status == "failed"
        assert row.error_message == "boom"


class TestAsyncRecorderNeverBreaksProcessing:
    @pytest.fixture
    def broken(self, monkeypatch):
        def _explode():
            raise RuntimeError("database is gone")

        monkeypatch.setattr("jasil.orm.get_async_sessionmaker", _explode)
        return AsyncEventLogRecorder()

    async def test_a_failed_publication_write_is_swallowed(self, broken, caplog):
        with caplog.at_level("WARNING"):
            await broken.record_published(_event())

        assert "record_published failed" in caplog.text

    async def test_a_failed_queue_write_is_swallowed(self, broken, caplog):
        with caplog.at_level("WARNING"):
            await broken.record_queued(_event())

        assert "record_queued failed" in caplog.text

    async def test_tracking_still_runs_the_handlers(self, broken, caplog):
        ran = []

        with caplog.at_level("WARNING"):
            async with broken.track(_event(), worker_id="w", handler_name="h"):
                ran.append(1)

        assert ran == [1]
        assert "mark_processing failed" in caplog.text
        assert "mark_completed failed" in caplog.text

    async def test_a_handler_error_still_propagates(self, broken):
        with pytest.raises(RuntimeError, match="boom"):
            async with broken.track(_event(), worker_id="w", handler_name="h"):
                raise RuntimeError("boom")


class TestBoundedDeleteAsync:
    async def _event_log_row(self, db, event_id: str, *, created_at: datetime, status: str = "completed") -> None:
        db.add(
            EventLog(
                id=event_id,
                event_type="activity.created",
                event_source="test",
                status=status,
                event_payload={},
                event_metadata={},
                created_at=created_at,
            )
        )
        await db.commit()

    async def test_it_deletes_every_matching_row(self, async_db):
        for index in range(5):
            await self._event_log_row(async_db, f"e{index}", created_at=OLD)

        deleted = await pruning.bounded_delete_async(EventLog, EventLog.created_at < CUTOFF, db=async_db)

        assert deleted == 5
        assert await _event_log_count(async_db) == 0

    async def test_it_leaves_non_matching_rows_and_deletes_across_batches(self, async_db):
        await self._event_log_row(async_db, "recent", created_at=T0)
        for index in range(5):
            await self._event_log_row(async_db, f"old-{index}", created_at=OLD)

        deleted = await pruning.bounded_delete_async(EventLog, EventLog.created_at < CUTOFF, db=async_db, batch_size=2)

        assert deleted == 5
        rows = (await async_db.execute(select(EventLog).order_by(EventLog.id))).scalars().all()
        assert [row.id for row in rows] == ["recent"]

    async def test_deleting_nothing_returns_zero(self, async_db):
        assert await pruning.bounded_delete_async(EventLog, EventLog.created_at < CUTOFF, db=async_db) == 0

    async def test_it_stops_rather_than_spinning_forever(self, async_db, monkeypatch):
        monkeypatch.setattr(pruning, "PRUNE_MAX_BATCHES", 2)
        for index in range(10):
            await self._event_log_row(async_db, f"e{index}", created_at=OLD)

        deleted = await pruning.bounded_delete_async(EventLog, EventLog.created_at < CUTOFF, db=async_db, batch_size=1)

        assert deleted == 2
        assert await _event_log_count(async_db) == 8


class TestBoundedDeleteSyncCoverage:
    def _event_log_row(self, db, event_id: str, *, created_at: datetime) -> None:
        db.add(
            EventLog(
                id=event_id,
                event_type="activity.created",
                event_source="test",
                status="completed",
                event_payload={},
                event_metadata={},
                created_at=created_at,
            )
        )
        db.commit()

    def test_the_sync_helper_still_deletes_in_bounded_batches(self, db):
        for index in range(3):
            self._event_log_row(db, f"sync-{index}", created_at=OLD)

        deleted = pruning.bounded_delete(EventLog, EventLog.created_at < CUTOFF, db=db, batch_size=2)

        assert deleted == 3
        assert db.query(EventLog).count() == 0

    def test_the_sync_helper_returns_zero_for_no_matches(self, db):
        assert pruning.bounded_delete(EventLog, EventLog.created_at < CUTOFF, db=db) == 0


class TestEventLogPruningAsync:
    async def test_old_rows_are_pruned_regardless_of_status(self, async_db):
        for status in ("published", "processing", "failed", "dead_letter"):
            async_db.add(
                EventLog(
                    id=status,
                    event_type="order.created",
                    event_source="api",
                    event_payload={},
                    status=status,
                    created_at=OLD,
                )
            )
        await async_db.commit()

        deleted = await event_log_crud.delete_events_before(CUTOFF, db=async_db, batch_size=2)

        assert deleted == 4
        assert await _event_log_count(async_db) == 0

    async def test_recent_rows_survive(self, async_db):
        await event_log_crud.record_published(_event(), async_db)

        assert await event_log_crud.delete_events_before(CUTOFF, db=async_db) == 0
        assert await _event_log_count(async_db) == 1


class TestJobStatements:
    def test_build_jobs_summary_aggregates_pure_rows(self):
        dead = ProcessingJob(
            id="dead",
            event_id="evt-dead",
            event_type="activity.created",
            subscriber_id="thumbnails.generate",
            source="test",
            payload={},
            status=jobs_crud.STATUS_DEAD_LETTER,
            attempts=1,
            max_attempts=1,
            available_at=T0,
            created_at=T0,
            updated_at=T0,
            completed_at=T0,
            last_error="boom",
        )

        summary = build_jobs_summary(
            hours=6,
            now=T0,
            count_rows=[
                ("invoice.render", "activity.created", "pending", 2),
                ("invoice.render", "activity.created", "claimed", 1),
                ("thumbnails.generate", "activity.created", "completed", 1),
                ("thumbnails.generate", "activity.created", "dead_letter", 1),
            ],
            oldest_pending=T0 - timedelta(seconds=15),
            dead_letter_rows=[dead],
        )

        assert summary.window_hours == 6
        assert summary.total_jobs == 5
        assert summary.pending == 2
        assert summary.claimed == 1
        assert summary.completed == 1
        assert summary.dead_letter == 1
        assert summary.oldest_pending_seconds == 15.0
        assert [row.subscriber_id for row in summary.by_subscriber] == ["invoice.render", "thumbnails.generate"]
        assert summary.recent_dead_letter[0].last_error == "boom"

    def test_unsupported_insert_dialect_fails_closed(self):
        with pytest.raises(RuntimeError, match="insert-or-ignore clause"):
            insert_ignoring_duplicate({}, "oracle")


class TestEnqueueAsync:
    async def test_a_job_starts_pending_with_no_attempts(self, async_db):
        job = await _enqueue(_event(), async_db)

        assert job.status == jobs_crud.STATUS_PENDING
        assert job.attempts == 0

    async def test_the_envelope_is_carried_onto_the_job(self, async_db):
        event = _event(metadata={"request_id": "r-1"})

        job = await _enqueue(event, async_db)

        assert job.event_id == event.event_id
        assert job.event_type == "activity.created"
        assert job.payload == {"activity_id": 7}
        assert job.schema_version == event.schema_version
        assert job.job_metadata == {"request_id": "r-1"}

    async def test_enqueueing_the_same_pair_twice_is_a_no_op(self, async_db):
        event = _event()
        await _enqueue(event, async_db)

        duplicate = await _enqueue(event, async_db)

        assert duplicate is None
        assert await _job_count(async_db) == 1

    async def test_the_same_event_fans_out_to_distinct_subscribers(self, async_db):
        event = _event()
        await _enqueue(event, async_db, subscriber="a")
        await _enqueue(event, async_db, subscriber="b")

        assert await _job_count(async_db) == 2

    async def test_a_future_available_at_is_honoured(self, async_db):
        job = await _enqueue(_event(), async_db, available_at=T0 + timedelta(hours=1))

        assert as_utc(job.available_at) > T0

    async def test_an_uncommitted_enqueue_joins_the_callers_transaction(self, async_db):
        await _enqueue(_event(), async_db, commit=False)

        await async_db.rollback()

        assert await _job_count(async_db) == 0


class TestClaimAsync:
    async def test_claiming_takes_a_lease_and_counts_the_attempt(self, async_db):
        await _enqueue(_event(), async_db)

        claimed = await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db)

        assert len(claimed) == 1
        assert claimed[0].status == jobs_crud.STATUS_CLAIMED
        assert claimed[0].attempts == 1
        assert claimed[0].locked_by == "w1"
        assert as_utc(claimed[0].lease_expires_at) - as_utc(claimed[0].locked_at) == timedelta(seconds=60)

    async def test_a_lease_taken_at_a_sub_second_instant_still_returns_its_rows(self, async_db):
        await _enqueue(_event(), async_db)

        claimed = await jobs_crud.claim_jobs(
            worker_id="w1",
            limit=10,
            lease_seconds=60,
            now=T0 + timedelta(seconds=1, microseconds=654321),
            db=async_db,
        )

        assert len(claimed) == 1
        assert claimed[0].status == jobs_crud.STATUS_CLAIMED

    async def test_a_claimed_job_is_not_claimed_again(self, async_db):
        await _enqueue(_event(), async_db)
        await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db)

        second = await jobs_crud.claim_jobs(worker_id="w2", limit=10, lease_seconds=60, now=T0, db=async_db)

        assert second == []

    async def test_a_job_scheduled_in_the_future_is_not_claimed(self, async_db):
        await _enqueue(_event(), async_db, available_at=T0 + timedelta(minutes=5))

        assert await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db) == []

    async def test_the_batch_size_and_oldest_available_order_are_respected(self, async_db):
        newest = _event(payload={"i": 2})
        oldest = _event(payload={"i": 1})
        await _enqueue(newest, async_db, available_at=T0 + timedelta(seconds=10))
        await _enqueue(oldest, async_db, available_at=T0)
        await _enqueue(_event(payload={"i": 3}), async_db, available_at=T0 + timedelta(seconds=20))

        claimed = await jobs_crud.claim_jobs(
            worker_id="w1", limit=2, lease_seconds=60, now=T0 + timedelta(minutes=1), db=async_db
        )

        assert len(claimed) == 2
        assert claimed[0].event_id == oldest.event_id

    async def test_claiming_an_empty_queue_returns_nothing(self, async_db):
        assert await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db) == []


class TestCompletionAsync:
    async def test_completing_marks_the_job_terminal(self, async_db):
        job = await _enqueue(_event(), async_db)
        await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db)

        await jobs_crud.mark_job_completed(job.id, now=T0, db=async_db)

        stored = await jobs_crud.get_job(job.id, async_db)
        assert stored.status == jobs_crud.STATUS_COMPLETED
        assert stored.completed_at is not None

    async def test_a_completed_job_is_never_claimed_again(self, async_db):
        job = await _enqueue(_event(), async_db)
        await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db)
        await jobs_crud.mark_job_completed(job.id, now=T0, db=async_db)

        assert await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db) == []


class TestFailureAndBackoffAsync:
    async def _claim(self, db):
        return (await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db))[0]

    async def test_a_failure_below_the_ceiling_reschedules_as_pending(self, async_db):
        await _enqueue(_event(), async_db, max_attempts=3)
        job = await self._claim(async_db)

        status = await jobs_crud.mark_job_failed(job.id, "boom", base_seconds=10, max_seconds=100, now=T0, db=async_db)

        stored = await jobs_crud.get_job(job.id, async_db)
        assert status == jobs_crud.STATUS_PENDING
        assert as_utc(stored.available_at) > T0
        assert stored.locked_by is None
        assert stored.lease_expires_at is None
        assert stored.last_error == "boom"

    async def test_exhausting_the_attempt_ceiling_dead_letters(self, async_db):
        await _enqueue(_event(), async_db, max_attempts=1)
        job = await self._claim(async_db)

        status = await jobs_crud.mark_job_failed(job.id, "boom", base_seconds=10, max_seconds=100, now=T0, db=async_db)

        stored = await jobs_crud.get_job(job.id, async_db)
        assert status == jobs_crud.STATUS_DEAD_LETTER
        assert stored.status == jobs_crud.STATUS_DEAD_LETTER
        assert stored.locked_by is None

    async def test_a_dead_lettered_job_is_never_claimed_again(self, async_db):
        await _enqueue(_event(), async_db, max_attempts=1)
        job = await self._claim(async_db)
        await jobs_crud.mark_job_failed(job.id, "boom", base_seconds=10, max_seconds=100, now=T0, db=async_db)

        assert await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db) == []

    async def test_failing_an_unknown_job_reports_no_status(self, async_db):
        assert await jobs_crud.mark_job_failed("nope", "boom", base_seconds=1, max_seconds=2, now=T0, db=async_db) == ""


class TestLeaseReclamationAsync:
    async def test_an_expired_lease_is_requeued(self, async_db):
        await _enqueue(_event(), async_db, max_attempts=3)
        await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db)

        reclaimed = await jobs_crud.reclaim_expired_leases(now=T0 + timedelta(seconds=61), db=async_db)

        job = (await async_db.execute(select(ProcessingJob))).scalar_one()
        assert reclaimed == 1
        assert job.status == jobs_crud.STATUS_PENDING
        assert "lease expired" in job.last_error

    async def test_a_live_lease_is_left_alone(self, async_db):
        await _enqueue(_event(), async_db)
        await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db)

        reclaimed = await jobs_crud.reclaim_expired_leases(now=T0 + timedelta(seconds=30), db=async_db)

        job = (await async_db.execute(select(ProcessingJob))).scalar_one()
        assert reclaimed == 0
        assert job.status == jobs_crud.STATUS_CLAIMED

    async def test_an_expired_lease_with_no_attempts_left_is_dead_lettered(self, async_db):
        await _enqueue(_event(), async_db, max_attempts=1)
        await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db)

        reclaimed = await jobs_crud.reclaim_expired_leases(now=T0 + timedelta(seconds=61), db=async_db)

        job = (await async_db.execute(select(ProcessingJob))).scalar_one()
        assert reclaimed == 1
        assert job.status == jobs_crud.STATUS_DEAD_LETTER
        assert "max attempts exhausted" in job.last_error

    async def test_a_second_pass_reclaims_nothing(self, async_db):
        await _enqueue(_event(), async_db, max_attempts=3)
        await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db)
        expiry = T0 + timedelta(seconds=61)
        await jobs_crud.reclaim_expired_leases(now=expiry, db=async_db)

        assert await jobs_crud.reclaim_expired_leases(now=expiry, db=async_db) == 0

    async def test_nothing_to_reclaim_returns_zero(self, async_db):
        assert await jobs_crud.reclaim_expired_leases(now=T0, db=async_db) == 0


class TestDeadLetterReplayAsync:
    async def test_a_dead_lettered_job_can_be_replayed(self, async_db):
        job = await _enqueue(_event(), async_db, max_attempts=1)
        await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=async_db)
        await jobs_crud.mark_job_failed(job.id, "boom", base_seconds=1, max_seconds=2, now=T0, db=async_db)

        replayed = await jobs_crud.replay_dead_letter_job(job.id, now=T0, db=async_db)

        stored = await jobs_crud.get_job(job.id, async_db)
        assert replayed is True
        assert stored.status == jobs_crud.STATUS_PENDING
        assert stored.attempts == 0
        assert stored.last_error is None
        assert stored.completed_at is None

    async def test_replaying_a_job_that_is_not_dead_lettered_is_refused(self, async_db):
        job = await _enqueue(_event(), async_db)

        assert await jobs_crud.replay_dead_letter_job(job.id, now=T0, db=async_db) is False

    async def test_replaying_an_unknown_job_is_refused(self, async_db):
        assert await jobs_crud.replay_dead_letter_job("nope", now=T0, db=async_db) is False


class TestJobsSummaryAsync:
    @pytest.fixture
    def now(self):
        return datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=1)

    async def test_an_empty_queue_summarises_to_nothing(self, async_db):
        summary = await jobs_crud.get_jobs_summary(async_db)

        assert summary.total_jobs == 0
        assert summary.by_subscriber == []
        assert summary.recent_dead_letter == []
        assert summary.oldest_pending_seconds is None

    async def test_the_window_is_reported_back(self, async_db):
        assert (await jobs_crud.get_jobs_summary(async_db, hours=6)).window_hours == 6

    async def test_jobs_are_counted_by_status_and_subscriber(self, async_db, now):
        event = _event()
        await _enqueue(event, async_db, subscriber="thumbnails.generate", now=now)
        await _enqueue(event, async_db, subscriber="invoice.render", now=now)
        await jobs_crud.claim_jobs(worker_id="w1", limit=1, lease_seconds=60, now=now, db=async_db)

        summary = await jobs_crud.get_jobs_summary(async_db)

        assert summary.total_jobs == 2
        assert summary.claimed == 1
        assert summary.pending == 1
        assert [row.subscriber_id for row in summary.by_subscriber] == ["invoice.render", "thumbnails.generate"]
        assert all(row.event_type == "activity.created" for row in summary.by_subscriber)

    async def test_completed_jobs_and_windowing(self, async_db, now):
        old_job = await _enqueue(_event(), async_db, subscriber="old", now=now - timedelta(days=2))
        fresh_job = await _enqueue(_event(), async_db, subscriber="fresh", now=now)
        await jobs_crud.mark_job_completed(old_job.id, now=now, db=async_db)
        await jobs_crud.mark_job_completed(fresh_job.id, now=now, db=async_db)

        summary = await jobs_crud.get_jobs_summary(async_db, hours=1)

        assert summary.total_jobs == 1
        assert summary.completed == 1
        assert summary.by_subscriber[0].subscriber_id == "fresh"

    async def test_the_oldest_pending_age_and_finished_queue(self, async_db, now):
        pending = await _enqueue(_event(), async_db, subscriber="pending", now=now - timedelta(minutes=10))
        done = await _enqueue(_event(), async_db, subscriber="done", now=now)
        await jobs_crud.mark_job_completed(done.id, now=now, db=async_db)

        summary = await jobs_crud.get_jobs_summary(async_db)
        assert summary.oldest_pending_seconds > 500

        await jobs_crud.mark_job_completed(pending.id, now=now, db=async_db)
        assert (await jobs_crud.get_jobs_summary(async_db)).oldest_pending_seconds is None

    async def test_dead_letters_are_listed_and_capped(self, async_db, now):
        for index in range(5):
            job = await _enqueue(_event(payload={"i": index}), async_db, max_attempts=1, now=now)
            await jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=now, db=async_db)
            await jobs_crud.mark_job_failed(
                job.id, f"boom-{index}", base_seconds=1, max_seconds=1, now=now, db=async_db
            )

        summary = await jobs_crud.get_jobs_summary(async_db, dead_letter_limit=2)

        assert summary.dead_letter == 5
        assert len(summary.recent_dead_letter) == 2
        assert summary.recent_dead_letter[0].last_error.startswith("boom-")


class TestJobPruningAsync:
    async def _job(self, db, job_id: str, *, status: str, completed_at: datetime | None) -> None:
        db.add(
            ProcessingJob(
                id=job_id,
                event_id=f"evt-{job_id}",
                event_type="activity.created",
                subscriber_id="s",
                source="test",
                payload={},
                status=status,
                attempts=1,
                max_attempts=3,
                available_at=OLD,
                created_at=OLD,
                updated_at=OLD,
                completed_at=completed_at,
            )
        )
        await db.commit()

    async def test_old_completed_jobs_are_pruned(self, async_db):
        await self._job(async_db, "done", status=jobs_crud.STATUS_COMPLETED, completed_at=OLD)

        assert await jobs_crud.delete_completed_jobs_before(CUTOFF, db=async_db) == 1

    @pytest.mark.parametrize("status", [jobs_crud.STATUS_PENDING, jobs_crud.STATUS_CLAIMED])
    async def test_in_flight_jobs_are_never_pruned(self, async_db, status):
        await self._job(async_db, "live", status=status, completed_at=OLD)

        assert await jobs_crud.delete_completed_jobs_before(CUTOFF, db=async_db) == 0
        assert await _job_count(async_db) == 1

    async def test_dead_letters_are_never_pruned(self, async_db):
        await self._job(async_db, "dead", status=jobs_crud.STATUS_DEAD_LETTER, completed_at=OLD)

        assert await jobs_crud.delete_completed_jobs_before(CUTOFF, db=async_db) == 0
        assert await _job_count(async_db) == 1

    async def test_recently_completed_jobs_survive(self, async_db):
        await self._job(async_db, "fresh", status=jobs_crud.STATUS_COMPLETED, completed_at=T0)

        assert await jobs_crud.delete_completed_jobs_before(CUTOFF, db=async_db) == 0


class TestOutboxAsync:
    async def test_an_outbox_row_carries_the_event(self, async_db):
        event = _event(metadata={"request_id": "r-1"})

        outbox_id = await jobs_outbox.add_to_outbox(event, now=T0, db=async_db)

        row = await async_db.get(EventOutbox, outbox_id)
        assert row.event_id == event.event_id
        assert row.event_type == "activity.created"
        assert row.payload == {"activity_id": 7}
        assert row.event_metadata == {"request_id": "r-1"}

    async def test_list_unrelayed_returns_oldest_rows_with_a_limit(self, async_db):
        old_id = await jobs_outbox.add_to_outbox(_event(payload={"i": 1}), now=T0, db=async_db)
        await jobs_outbox.add_to_outbox(_event(payload={"i": 2}), now=T0 + timedelta(seconds=1), db=async_db)
        await jobs_outbox.add_to_outbox(_event(payload={"i": 3}), now=T0 + timedelta(seconds=2), db=async_db)

        rows = await jobs_outbox.list_unrelayed(limit=2, db=async_db)

        assert len(rows) == 2
        assert rows[0].id == old_id

    async def test_a_relayed_row_is_stamped_and_not_listed_again(self, async_db):
        outbox_id = await jobs_outbox.add_to_outbox(_event(), now=T0, db=async_db)

        await jobs_outbox.mark_relayed(outbox_id, now=T0, db=async_db)

        row = await async_db.get(EventOutbox, outbox_id)
        assert row.relayed_at is not None
        assert await jobs_outbox.list_unrelayed(limit=10, db=async_db) == []

    async def test_an_uncommitted_outbox_write_joins_the_callers_transaction(self, async_db):
        await jobs_outbox.add_to_outbox(_event(), now=T0, db=async_db, commit=False)

        await async_db.rollback()

        assert await _outbox_count(async_db) == 0

    async def test_an_uncommitted_relay_stamp_joins_the_callers_transaction(self, async_db):
        outbox_id = await jobs_outbox.add_to_outbox(_event(), now=T0, db=async_db)

        await jobs_outbox.mark_relayed(outbox_id, now=T0, db=async_db, commit=False)
        await async_db.rollback()

        row = await async_db.get(EventOutbox, outbox_id)
        assert row.relayed_at is None


class TestOutboxPruningAsync:
    async def test_old_relayed_rows_are_pruned(self, async_db):
        outbox_id = await jobs_outbox.add_to_outbox(_event(), now=OLD, db=async_db)
        await jobs_outbox.mark_relayed(outbox_id, now=OLD, db=async_db)

        assert await jobs_outbox.delete_relayed_before(CUTOFF, db=async_db) == 1

    async def test_unrelayed_rows_are_never_pruned(self, async_db):
        await jobs_outbox.add_to_outbox(_event(), now=OLD, db=async_db)

        assert await jobs_outbox.delete_relayed_before(CUTOFF, db=async_db) == 0
        assert await _outbox_count(async_db) == 1

    async def test_recently_relayed_rows_survive(self, async_db):
        outbox_id = await jobs_outbox.add_to_outbox(_event(), now=T0, db=async_db)
        await jobs_outbox.mark_relayed(outbox_id, now=T0, db=async_db)

        assert await jobs_outbox.delete_relayed_before(CUTOFF, db=async_db) == 0


class TestCommitOrFlushAsync:
    async def test_commit_makes_the_write_durable(self, async_db):
        async_db.add(
            EventLog(
                id="committed",
                event_type="activity.created",
                event_source="test",
                event_payload={},
                status="published",
            )
        )

        await commit_or_flush_async(async_db, True)
        await async_db.rollback()

        assert await _event_log_count(async_db) == 1

    async def test_flush_leaves_the_write_in_the_callers_transaction(self, async_db):
        async_db.add(
            EventLog(
                id="flushed",
                event_type="activity.created",
                event_source="test",
                event_payload={},
                status="published",
            )
        )

        await commit_or_flush_async(async_db, False)
        assert await _event_log_count(async_db) == 1
        await async_db.rollback()

        assert await _event_log_count(async_db) == 0


class TestCommitOrFlushSyncCoverage:
    def test_commit_makes_the_write_durable(self, db):
        db.add(
            EventLog(
                id="sync-committed",
                event_type="activity.created",
                event_source="test",
                event_payload={},
                status="published",
            )
        )

        commit_or_flush(db, True)
        db.rollback()

        assert db.query(EventLog).count() == 1

    def test_flush_leaves_the_write_in_the_callers_transaction(self, db):
        db.add(
            EventLog(
                id="sync-flushed",
                event_type="activity.created",
                event_source="test",
                event_payload={},
                status="published",
            )
        )

        commit_or_flush(db, False)
        assert db.query(EventLog).count() == 1
        db.rollback()

        assert db.query(EventLog).count() == 0
