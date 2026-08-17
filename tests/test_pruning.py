"""Retention pruning: bounded batches, and what must never be deleted.

The dangerous failure here is over-deletion — pruning a row that is still the
source of truth for in-flight work, or discarding a dead-letter an operator
still needs. Those invariants get a test each.
"""

from datetime import UTC, datetime, timedelta

import pytest

import jasil.event_log.crud as event_log_crud
import jasil.jobs.crud as jobs_crud
import jasil.jobs.outbox as jobs_outbox
import jasil.pruning as pruning
from jasil.event_log.models import EventLog
from jasil.events import new_event
from jasil.jobs.models import EventOutbox, ProcessingJob

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
OLD = T0 - timedelta(days=90)
CUTOFF = T0 - timedelta(days=30)


def _event_log_row(db, event_id: str, *, created_at: datetime, status: str = "completed") -> None:
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
    db.commit()


class TestBoundedDelete:
    def test_it_deletes_every_matching_row(self, db):
        for index in range(5):
            _event_log_row(db, f"e{index}", created_at=OLD)

        deleted = pruning.bounded_delete(EventLog, EventLog.created_at < CUTOFF, db=db)

        assert deleted == 5
        assert db.query(EventLog).count() == 0

    def test_it_leaves_non_matching_rows(self, db):
        _event_log_row(db, "old", created_at=OLD)
        _event_log_row(db, "recent", created_at=T0)

        pruning.bounded_delete(EventLog, EventLog.created_at < CUTOFF, db=db)

        assert [row.id for row in db.query(EventLog).all()] == ["recent"]

    def test_it_works_across_several_batches(self, db):
        """The batch bound keeps each delete transaction short; the pass must
        still drain the backlog rather than stopping after one batch."""
        for index in range(7):
            _event_log_row(db, f"e{index}", created_at=OLD)

        deleted = pruning.bounded_delete(EventLog, EventLog.created_at < CUTOFF, db=db, batch_size=2)

        assert deleted == 7
        assert db.query(EventLog).count() == 0

    def test_deleting_nothing_returns_zero(self, db):
        assert pruning.bounded_delete(EventLog, EventLog.created_at < CUTOFF, db=db) == 0

    def test_it_stops_rather_than_spinning_forever(self, db, monkeypatch):
        """A pathological backlog must not hang the scheduler; the next pass
        continues where this one stopped."""
        monkeypatch.setattr(pruning, "PRUNE_MAX_BATCHES", 2)
        for index in range(10):
            _event_log_row(db, f"e{index}", created_at=OLD)

        deleted = pruning.bounded_delete(EventLog, EventLog.created_at < CUTOFF, db=db, batch_size=1)

        assert deleted == 2
        assert db.query(EventLog).count() == 8


class TestEventLogPruning:
    def test_old_rows_are_pruned_regardless_of_status(self, db):
        """event_log is a best-effort observability trail; nothing in it is a
        source of truth worth keeping past the window."""
        _event_log_row(db, "failed", created_at=OLD, status="failed")
        _event_log_row(db, "completed", created_at=OLD, status="completed")

        deleted = event_log_crud.delete_events_before(CUTOFF, db=db)

        assert deleted == 2

    def test_recent_rows_survive(self, db):
        _event_log_row(db, "recent", created_at=T0)

        assert event_log_crud.delete_events_before(CUTOFF, db=db) == 0


class TestJobPruning:
    def _job(self, db, job_id: str, *, status: str, completed_at: datetime | None) -> None:
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
        db.commit()

    def test_old_completed_jobs_are_pruned(self, db):
        self._job(db, "done", status=jobs_crud.STATUS_COMPLETED, completed_at=OLD)

        assert jobs_crud.delete_completed_jobs_before(CUTOFF, db=db) == 1

    @pytest.mark.parametrize(
        "status",
        [jobs_crud.STATUS_PENDING, jobs_crud.STATUS_CLAIMED],
    )
    def test_in_flight_jobs_are_never_pruned(self, db, status):
        """Deleting a pending or claimed row would silently drop derived work."""
        self._job(db, "live", status=status, completed_at=OLD)

        assert jobs_crud.delete_completed_jobs_before(CUTOFF, db=db) == 0
        assert db.query(ProcessingJob).count() == 1

    def test_dead_letters_are_never_pruned(self, db):
        """They are rare, human-actionable, and the only record of a total failure."""
        self._job(db, "dead", status=jobs_crud.STATUS_DEAD_LETTER, completed_at=OLD)

        assert jobs_crud.delete_completed_jobs_before(CUTOFF, db=db) == 0
        assert db.query(ProcessingJob).count() == 1

    def test_recently_completed_jobs_survive(self, db):
        self._job(db, "fresh", status=jobs_crud.STATUS_COMPLETED, completed_at=T0)

        assert jobs_crud.delete_completed_jobs_before(CUTOFF, db=db) == 0


class TestOutboxPruning:
    def test_old_relayed_rows_are_pruned(self, db):
        event = new_event("activity.created", {}, source="test")
        outbox_id = jobs_outbox.add_to_outbox(event, now=OLD, db=db)
        jobs_outbox.mark_relayed(outbox_id, now=OLD, db=db)

        assert jobs_outbox.delete_relayed_before(CUTOFF, db=db) == 1

    def test_unrelayed_rows_are_never_pruned(self, db):
        """An unrelayed row is pending work, not history."""
        jobs_outbox.add_to_outbox(new_event("activity.created", {}, source="test"), now=OLD, db=db)

        assert jobs_outbox.delete_relayed_before(CUTOFF, db=db) == 0
        assert db.query(EventOutbox).count() == 1

    def test_recently_relayed_rows_survive(self, db):
        event = new_event("activity.created", {}, source="test")
        outbox_id = jobs_outbox.add_to_outbox(event, now=T0, db=db)
        jobs_outbox.mark_relayed(outbox_id, now=T0, db=db)

        assert jobs_outbox.delete_relayed_before(CUTOFF, db=db) == 0
