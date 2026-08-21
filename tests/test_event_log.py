"""The event_log trail — the recorder and the CRUD beneath it.

This is the observability path: what was published, whether it was processed, by
which worker, how long it took, and why it failed. Two properties matter more
than the individual queries.

*It must never break event processing.* Every write goes through the recorder,
which swallows and logs any storage error. A database hiccup that took the bus
down with it would make enabling observability strictly worse than leaving it
off, so the swallowing is tested as a feature rather than tolerated.

*It must not silently lose a write.* The `handler_name` column is the one field
whose length is unbounded from the caller's side (it is the joined list of every
subscriber that ran), and overflowing it once made PostgreSQL reject the whole
UPDATE — silently, because the recorder swallows. The row then stayed at
`published` forever while the work had actually completed.
"""

from datetime import UTC, datetime, timedelta

import pytest

import jasil.event_log.crud as event_log_crud
import jasil.orm as jasil_orm
from jasil._core.limits import MAX_HANDLER_NAME_LENGTH, MAX_STORED_ERROR_LENGTH, MAX_WORKER_ID_LENGTH, fit_length
from jasil.event_log.models import EventLog
from jasil.event_log.recorder import EventLogRecorder
from jasil.events import new_event


def _event(event_type: str = "order.created", **kwargs):
    return new_event(event_type, kwargs.pop("payload", {"id": 1}), source="api:create_order", **kwargs)


def _row(db, event_id: str) -> EventLog:
    db.rollback()  # the recorder commits on its own session
    return db.get(EventLog, event_id)


class TestLifecycleWrites:
    def test_publishing_records_the_envelope(self, db):
        event = _event(metadata={"request_id": "r-1"})

        event_log_crud.record_published(event, db)

        row = _row(db, event.event_id)
        assert row.status == "published"
        assert row.event_type == "order.created"
        assert row.event_source == "api:create_order"
        assert row.event_payload == {"id": 1}
        assert row.event_metadata == {"request_id": "r-1"}

    def test_empty_metadata_is_stored_as_null(self, db):
        """Nothing to correlate reads better as NULL than as an empty object."""
        event = _event()

        event_log_crud.record_published(event, db)

        assert _row(db, event.event_id).event_metadata is None

    def test_a_durable_event_is_recorded_queued(self, db):
        """Terminal from event_log's view: execution lives in processing_jobs."""
        event = _event()

        event_log_crud.record_queued(event, db)

        assert _row(db, event.event_id).status == "queued"

    def test_processing_stamps_the_worker(self, db):
        event = _event()
        event_log_crud.record_published(event, db)

        event_log_crud.mark_processing(event.event_id, "worker-7", db)

        row = _row(db, event.event_id)
        assert row.status == "processing"
        assert row.worker_id == "worker-7"
        assert row.processed_at is not None

    def test_completion_stamps_the_handler_and_duration(self, db):
        event = _event()
        event_log_crud.record_published(event, db)

        event_log_crud.mark_completed(event.event_id, "render_invoice", 42, db)

        row = _row(db, event.event_id)
        assert row.status == "completed"
        assert row.handler_name == "render_invoice"
        assert row.processing_time_ms == 42
        assert row.completed_at is not None

    def test_failure_stores_the_reason(self, db):
        event = _event()
        event_log_crud.record_published(event, db)

        event_log_crud.mark_failed(event.event_id, "render_invoice", "upstream refused", 7, db)

        row = _row(db, event.event_id)
        assert row.status == "failed"
        assert row.error_message == "upstream refused"

    def test_a_long_error_is_clamped_to_the_column(self, db):
        """Clipped, not refused: losing the diagnostic beats losing the failure record."""
        event = _event()
        event_log_crud.record_published(event, db)

        event_log_crud.mark_failed(event.event_id, None, "x" * (MAX_STORED_ERROR_LENGTH * 2), 1, db)

        assert len(_row(db, event.event_id).error_message) == MAX_STORED_ERROR_LENGTH

    def test_a_transition_for_an_unknown_event_is_a_no_op(self, db):
        """The trail is best-effort; a missing row must not raise into the bus."""
        event_log_crud.mark_completed("never-published", "handler", 1, db)


class TestHandlerNameClamping:
    """The joined subscriber list grows with the number of subscribers.

    Overflowing the column made PostgreSQL reject the whole UPDATE, and because
    the recorder swallows, the row silently stayed at ``published`` while the
    handlers had in fact run.
    """

    def test_a_name_that_fits_is_untouched(self):
        assert fit_length("a,b,c", MAX_HANDLER_NAME_LENGTH) == "a,b,c"

    def test_no_name_stays_none(self):
        assert fit_length(None, MAX_HANDLER_NAME_LENGTH) is None

    def test_a_long_list_is_marked_as_truncated(self):
        """A reader must be able to tell the list was cut, not assume it is complete."""
        clamped = fit_length("subscriber," * 200, MAX_HANDLER_NAME_LENGTH)

        assert len(clamped) == MAX_HANDLER_NAME_LENGTH
        assert clamped.endswith("...")

    def test_the_column_is_declared_at_the_same_width_it_is_clamped_to(self, mapped_base):
        """The drift this constant was extracted to prevent."""
        assert mapped_base.metadata.tables["event_log"].c.handler_name.type.length == MAX_HANDLER_NAME_LENGTH

    def test_a_clamped_name_round_trips_to_the_database(self, db):
        """The point of the clamp: the write must actually land."""
        event = _event()
        event_log_crud.record_published(event, db)

        event_log_crud.mark_completed(event.event_id, "subscriber," * 200, 1, db)

        row = _row(db, event.event_id)
        assert row.status == "completed"
        assert len(row.handler_name) == MAX_HANDLER_NAME_LENGTH


class TestWorkerIdClamping:
    """A host may supply its own consumer name to the bus, and it lands here."""

    def test_a_long_worker_id_is_clamped_rather_than_lost(self, db):
        event = _event()
        event_log_crud.record_published(event, db)

        event_log_crud.mark_processing(event.event_id, "w" * (MAX_WORKER_ID_LENGTH * 2), db)

        row = _row(db, event.event_id)
        assert row.status == "processing"
        assert len(row.worker_id) == MAX_WORKER_ID_LENGTH

    def test_the_column_is_declared_at_the_same_width_it_is_clamped_to(self, mapped_base):
        assert mapped_base.metadata.tables["event_log"].c.worker_id.type.length == MAX_WORKER_ID_LENGTH


class TestSummary:
    def test_an_empty_log_summarises_to_nothing(self, db):
        summary = event_log_crud.get_event_log_summary(db)

        assert summary.total_events == 0
        assert summary.by_type == []
        assert summary.pending == []
        assert summary.recent_failures == []

    def test_the_window_is_reported_back(self, db):
        assert event_log_crud.get_event_log_summary(db, hours=6).window_hours == 6

    def test_events_are_counted_per_type_and_status(self, db):
        for _ in range(2):
            event_log_crud.record_published(_event("order.created"), db)
        completed = _event("order.created")
        event_log_crud.record_published(completed, db)
        event_log_crud.mark_completed(completed.event_id, "h", 10, db)
        event_log_crud.record_published(_event("invoice.rendered"), db)

        summary = event_log_crud.get_event_log_summary(db)

        by_type = {stats.event_type: stats for stats in summary.by_type}
        assert summary.total_events == 4
        assert by_type["order.created"].total == 3
        assert by_type["order.created"].published == 2
        assert by_type["order.created"].completed == 1
        assert by_type["invoice.rendered"].total == 1

    def test_types_are_ordered_by_name(self, db):
        """A dashboard that reorders itself between refreshes is unreadable."""
        for event_type in ("zeta.happened", "alpha.happened"):
            event_log_crud.record_published(_event(event_type), db)

        summary = event_log_crud.get_event_log_summary(db)

        assert [stats.event_type for stats in summary.by_type] == ["alpha.happened", "zeta.happened"]

    def test_latency_is_averaged_and_peaked(self, db):
        for duration in (10, 30):
            event = _event()
            event_log_crud.record_published(event, db)
            event_log_crud.mark_completed(event.event_id, "h", duration, db)

        stats = event_log_crud.get_event_log_summary(db).by_type[0]

        assert stats.avg_processing_time_ms == 20.0
        assert stats.max_processing_time_ms == 30

    def test_unmeasured_types_report_no_latency(self, db):
        """Zero would read as 'instant' rather than 'never finished'."""
        event_log_crud.record_published(_event(), db)

        stats = event_log_crud.get_event_log_summary(db).by_type[0]

        assert stats.avg_processing_time_ms is None
        assert stats.max_processing_time_ms is None

    def test_events_outside_the_window_are_excluded(self, db):
        db.add(
            EventLog(
                id="ancient",
                event_type="order.created",
                event_source="api",
                event_payload={},
                status="completed",
                created_at=datetime.now(UTC) - timedelta(days=2),
            )
        )
        db.commit()

        assert event_log_crud.get_event_log_summary(db, hours=1).total_events == 0

    def test_queued_events_are_counted_separately(self, db):
        event_log_crud.record_queued(_event(), db)

        assert event_log_crud.get_event_log_summary(db).by_type[0].queued == 1


class TestPendingGroups:
    def test_unfinished_events_are_grouped(self, db):
        event_log_crud.record_published(_event(), db)
        event_log_crud.record_published(_event(), db)

        pending = event_log_crud.get_event_log_summary(db).pending

        assert len(pending) == 1
        assert pending[0].status == "published"
        assert pending[0].count == 2

    def test_finished_events_are_not_pending(self, db):
        event = _event()
        event_log_crud.record_published(event, db)
        event_log_crud.mark_completed(event.event_id, "h", 1, db)

        assert event_log_crud.get_event_log_summary(db).pending == []

    def test_a_queued_event_is_not_pending(self, db):
        """Durable events are terminal here; the Jobs dashboard tracks them."""
        event_log_crud.record_queued(_event(), db)

        assert event_log_crud.get_event_log_summary(db).pending == []

    def test_the_oldest_group_comes_first(self, db):
        """The backlog a human should look at first is the one that has waited longest."""
        db.add_all(
            [
                EventLog(
                    id="recent",
                    event_type="recent.thing",
                    event_source="api",
                    event_payload={},
                    status="published",
                    created_at=datetime.now(UTC) - timedelta(minutes=1),
                ),
                EventLog(
                    id="old",
                    event_type="old.thing",
                    event_source="api",
                    event_payload={},
                    status="published",
                    created_at=datetime.now(UTC) - timedelta(minutes=30),
                ),
            ]
        )
        db.commit()

        pending = event_log_crud.get_event_log_summary(db).pending

        assert [group.event_type for group in pending] == ["old.thing", "recent.thing"]
        assert pending[0].oldest_seconds > pending[1].oldest_seconds


class TestRecentFailures:
    def test_only_failures_are_returned(self, db):
        completed = _event()
        event_log_crud.record_published(completed, db)
        event_log_crud.mark_completed(completed.event_id, "h", 1, db)
        failed = _event()
        event_log_crud.record_published(failed, db)
        event_log_crud.mark_failed(failed.event_id, "h", "boom", 1, db)

        failures = event_log_crud.get_event_log_summary(db).recent_failures

        assert [failure.id for failure in failures] == [failed.event_id]

    def test_the_failure_carries_enough_to_investigate(self, db):
        event = _event(metadata={"request_id": "r-9"})
        event_log_crud.record_published(event, db)
        event_log_crud.mark_failed(event.event_id, "render_invoice", "boom", 5, db)

        failure = event_log_crud.get_event_log_summary(db).recent_failures[0]

        assert failure.error_message == "boom"
        assert failure.handler_name == "render_invoice"
        assert failure.event_metadata == {"request_id": "r-9"}

    def test_the_list_is_capped(self, db):
        for _ in range(5):
            event = _event()
            event_log_crud.record_published(event, db)
            event_log_crud.mark_failed(event.event_id, "h", "boom", 1, db)

        summary = event_log_crud.get_event_log_summary(db, failure_limit=2)

        assert len(summary.recent_failures) == 2

    def test_dead_letters_are_included(self, db):
        db.add(
            EventLog(
                id="dead",
                event_type="order.created",
                event_source="api",
                event_payload={},
                status="dead_letter",
            )
        )
        db.commit()

        assert [f.id for f in event_log_crud.get_event_log_summary(db).recent_failures] == ["dead"]


class TestRecorder:
    """The seam between the bus and the trail. It must never raise."""

    @pytest.fixture
    def recorder(self, session_factory):
        return EventLogRecorder()

    def test_it_records_a_publication(self, recorder, db):
        event = _event()

        recorder.record_published(event)

        assert _row(db, event.event_id).status == "published"

    def test_it_records_a_durable_queueing(self, recorder, db):
        event = _event()

        recorder.record_queued(event)

        assert _row(db, event.event_id).status == "queued"

    def test_tracking_records_processing_then_completed(self, recorder, db):
        event = _event()
        recorder.record_published(event)

        with recorder.track(event, worker_id="worker-1", handler_name="h"):
            pass

        row = _row(db, event.event_id)
        assert row.status == "completed"
        assert row.worker_id == "worker-1"
        assert row.handler_name == "h"

    def test_the_in_process_bus_can_skip_the_processing_row(self, recorder, db):
        """published -> completed within one call; the middle state is never observed."""
        event = _event()
        recorder.record_published(event)

        with recorder.track(event, worker_id="w", handler_name="h", record_processing=False):
            pass

        row = _row(db, event.event_id)
        assert row.status == "completed"
        assert row.worker_id is None

    def test_a_handler_failure_is_recorded_and_re_raised(self, recorder, db):
        """The bus needs the exception to decide whether to ack."""
        event = _event()
        recorder.record_published(event)

        with pytest.raises(RuntimeError, match="boom"), recorder.track(event, worker_id="w", handler_name="h"):
            raise RuntimeError("boom")

        row = _row(db, event.event_id)
        assert row.status == "failed"
        assert row.error_message == "boom"

    def test_the_elapsed_time_is_measured(self, recorder, db):
        event = _event()
        recorder.record_published(event)

        with recorder.track(event, worker_id="w", handler_name="h"):
            pass

        assert _row(db, event.event_id).processing_time_ms >= 0


class TestRecorderNeverBreaksProcessing:
    """Observability is best-effort. A storage failure is logged, never raised."""

    @pytest.fixture
    def broken(self, monkeypatch):
        def _explode():
            raise RuntimeError("database is gone")

        monkeypatch.setattr(jasil_orm, "get_sessionmaker", _explode)
        return EventLogRecorder()

    def test_a_failed_publication_write_is_swallowed(self, broken, caplog):
        with caplog.at_level("WARNING"):
            broken.record_published(_event())

        assert "record_published failed" in caplog.text

    def test_a_failed_queue_write_is_swallowed(self, broken, caplog):
        with caplog.at_level("WARNING"):
            broken.record_queued(_event())

        assert "record_queued failed" in caplog.text

    def test_tracking_still_runs_the_handlers(self, broken, caplog):
        """The work must happen even when nothing can be recorded about it."""
        ran = []

        with caplog.at_level("WARNING"), broken.track(_event(), worker_id="w", handler_name="h"):
            ran.append(1)

        assert ran == [1]
        assert "mark_processing failed" in caplog.text
        assert "mark_completed failed" in caplog.text

    def test_a_handler_error_still_propagates(self, broken):
        """A recording failure must not swallow the handler's own exception."""
        with pytest.raises(RuntimeError, match="boom"), broken.track(_event(), worker_id="w", handler_name="h"):
            raise RuntimeError("boom")


class TestPruning:
    def test_rows_before_the_cutoff_are_deleted(self, db):
        db.add(
            EventLog(
                id="old",
                event_type="order.created",
                event_source="api",
                event_payload={},
                status="completed",
                created_at=datetime.now(UTC) - timedelta(days=90),
            )
        )
        db.commit()

        deleted = event_log_crud.delete_events_before(datetime.now(UTC) - timedelta(days=30), db=db)

        assert deleted == 1
        assert db.query(EventLog).count() == 0

    def test_rows_after_the_cutoff_are_kept(self, db):
        event_log_crud.record_published(_event(), db)

        event_log_crud.delete_events_before(datetime.now(UTC) - timedelta(days=30), db=db)

        assert db.query(EventLog).count() == 1

    def test_every_status_is_prunable(self, db):
        """event_log is a safe-to-lose trail; nothing in it is a source of truth."""
        old = datetime.now(UTC) - timedelta(days=90)
        db.add_all(
            [
                EventLog(
                    id=status,
                    event_type="order.created",
                    event_source="api",
                    event_payload={},
                    status=status,
                    created_at=old,
                )
                for status in ("published", "processing", "failed", "dead_letter")
            ]
        )
        db.commit()

        event_log_crud.delete_events_before(datetime.now(UTC) - timedelta(days=30), db=db)

        assert db.query(EventLog).count() == 0

    def test_deletion_is_batched(self, db):
        """A single unbounded DELETE on a large table locks it for too long."""
        old = datetime.now(UTC) - timedelta(days=90)
        db.add_all(
            [
                EventLog(
                    id=f"old-{index}",
                    event_type="order.created",
                    event_source="api",
                    event_payload={},
                    status="completed",
                    created_at=old,
                )
                for index in range(5)
            ]
        )
        db.commit()

        deleted = event_log_crud.delete_events_before(datetime.now(UTC) - timedelta(days=30), db=db, batch_size=2)

        assert deleted == 5
        assert db.query(EventLog).count() == 0
