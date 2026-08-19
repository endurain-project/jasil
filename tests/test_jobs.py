"""The durable-job state machine: enqueue, claim, lease, backoff, dead-letter.

This is the highest-risk logic in the library — a bug here silently drops or
duplicates derived work — so the tests drive the CRUD layer directly against a
real (SQLite) database rather than mocking it. Time is injected everywhere, so
nothing sleeps and lease expiry is exercised deterministically.
"""

from datetime import UTC, datetime, timedelta

import pytest

import jasil.jobs.crud as jobs_crud
import jasil.jobs.outbox as jobs_outbox
import jasil.jobs.relay as jobs_relay
from jasil._core.timestamps import as_utc
from jasil.events import MAX_EVENT_TYPE_LENGTH, MAX_SOURCE_LENGTH, new_event
from jasil.jobs.backoff import backoff_seconds
from jasil.jobs.models import EventOutbox, ProcessingJob
from jasil.jobs.registry import MAX_SUBSCRIBER_ID_LENGTH, JobHandlerRegistry

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
SUBSCRIBER = "thumbnails.generate"


class FixedClock:
    """A ``ClockProvider`` whose time only moves when a test moves it."""

    def __init__(self, now: datetime = T0) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


@pytest.fixture
def clock():
    return FixedClock()


@pytest.fixture
def event():
    return new_event("activity.created", {"activity_id": 7}, source="test")


def _enqueue(event, db, *, subscriber=SUBSCRIBER, max_attempts=3, now=T0, available_at=None):
    return jobs_crud.enqueue_job(
        event, subscriber, max_attempts=max_attempts, now=now, db=db, available_at=available_at
    )


class TestEnqueue:
    def test_a_job_starts_pending_with_no_attempts(self, db, event):
        job = _enqueue(event, db)

        assert job.status == jobs_crud.STATUS_PENDING
        assert job.attempts == 0

    def test_the_envelope_is_carried_onto_the_job(self, db, event):
        job = _enqueue(event, db)

        assert job.event_id == event.event_id
        assert job.event_type == "activity.created"
        assert job.payload == {"activity_id": 7}
        assert job.schema_version == event.schema_version

    def test_enqueueing_the_same_pair_twice_is_a_no_op(self, db, event):
        """``(event_id, subscriber_id)`` uniqueness is what makes the consumer
        idempotent: a re-delivered event must not run the subscriber twice."""
        _enqueue(event, db)

        duplicate = _enqueue(event, db)

        assert duplicate is None
        assert db.query(ProcessingJob).count() == 1

    def test_the_same_event_fans_out_to_distinct_subscribers(self, db, event):
        _enqueue(event, db, subscriber="a")
        _enqueue(event, db, subscriber="b")

        assert db.query(ProcessingJob).count() == 2

    def test_a_future_available_at_is_honoured(self, db, event):
        job = _enqueue(event, db, available_at=T0 + timedelta(hours=1))

        assert as_utc(job.available_at) > T0

    def test_identifiers_at_the_column_limit_round_trip(self, db):
        """Proves the limits the mint-time checks enforce really are the widths.

        SQLite ignores a VARCHAR length, so this only bites on the Postgres and
        MySQL matrix jobs — which is exactly where it needs to.
        """
        event = new_event("e" * MAX_EVENT_TYPE_LENGTH, {"k": "v"}, source="s" * MAX_SOURCE_LENGTH)

        job = _enqueue(event, db, subscriber="b" * MAX_SUBSCRIBER_ID_LENGTH)

        assert job is not None
        assert len(job.event_type) == MAX_EVENT_TYPE_LENGTH
        assert len(job.subscriber_id) == MAX_SUBSCRIBER_ID_LENGTH


class TestSubscriberRegistration:
    def test_an_over_long_subscriber_id_is_refused(self):
        """Caught at startup rather than when the relay first fans the event out."""
        with pytest.raises(ValueError, match="subscriber_id is 201 characters"):
            JobHandlerRegistry().register("a.b", "s" * 201, lambda _e: None)

    def test_an_over_long_event_type_is_refused(self):
        with pytest.raises(ValueError, match="event_type is 101 characters"):
            JobHandlerRegistry().register("e" * 101, "sub", lambda _e: None)

    def test_identifiers_at_the_limit_are_accepted(self):
        registry = JobHandlerRegistry()
        subscriber_id = "s" * MAX_SUBSCRIBER_ID_LENGTH

        registry.register("a.b", subscriber_id, lambda _e: None)

        assert registry.subscribers_for("a.b") == (subscriber_id,)


class TestClaim:
    def test_claiming_takes_a_lease_and_counts_the_attempt(self, db, event):
        """The attempt is counted at claim time so a worker crashing mid-run
        still consumes one, which is what bounds a crash loop."""
        _enqueue(event, db)

        claimed = jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db)

        assert len(claimed) == 1
        assert claimed[0].status == jobs_crud.STATUS_CLAIMED
        assert claimed[0].attempts == 1
        assert claimed[0].locked_by == "w1"
        assert as_utc(claimed[0].lease_expires_at) == T0 + timedelta(seconds=60)

    def test_a_claimed_job_is_not_claimed_again(self, db, event):
        _enqueue(event, db)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db)

        second = jobs_crud.claim_jobs(worker_id="w2", limit=10, lease_seconds=60, now=T0, db=db)

        assert second == []

    def test_a_job_scheduled_in_the_future_is_not_claimed(self, db, event):
        _enqueue(event, db, available_at=T0 + timedelta(minutes=5))

        assert jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db) == []

    def test_the_batch_size_is_respected(self, db):
        for index in range(5):
            _enqueue(new_event("activity.created", {"i": index}, source="test"), db)

        claimed = jobs_crud.claim_jobs(worker_id="w1", limit=2, lease_seconds=60, now=T0, db=db)

        assert len(claimed) == 2

    def test_the_oldest_available_job_is_claimed_first(self, db):
        old = new_event("activity.created", {"i": 1}, source="test")
        new = new_event("activity.created", {"i": 2}, source="test")
        _enqueue(new, db, available_at=T0 + timedelta(seconds=10))
        _enqueue(old, db, available_at=T0)

        claimed = jobs_crud.claim_jobs(worker_id="w1", limit=1, lease_seconds=60, now=T0 + timedelta(minutes=1), db=db)

        assert claimed[0].event_id == old.event_id

    def test_claiming_an_empty_queue_returns_nothing(self, db):
        assert jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db) == []


class TestCompletion:
    def test_completing_marks_the_job_terminal(self, db, event):
        job = _enqueue(event, db)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db)

        jobs_crud.mark_job_completed(job.id, now=T0, db=db)

        stored = jobs_crud.get_job(job.id, db)
        assert stored.status == jobs_crud.STATUS_COMPLETED
        assert stored.completed_at is not None

    def test_a_completed_job_is_never_claimed_again(self, db, event):
        job = _enqueue(event, db)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db)
        jobs_crud.mark_job_completed(job.id, now=T0, db=db)

        assert jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db) == []


class TestFailureAndBackoff:
    def _claim(self, db):
        return jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db)[0]

    def test_a_failure_below_the_ceiling_reschedules_as_pending(self, db, event):
        _enqueue(event, db, max_attempts=3)
        job = self._claim(db)

        status = jobs_crud.mark_job_failed(job.id, "boom", base_seconds=10, max_seconds=100, now=T0, db=db)

        assert status == jobs_crud.STATUS_PENDING
        assert as_utc(jobs_crud.get_job(job.id, db).available_at) > T0

    def test_the_failure_reason_is_recorded(self, db, event):
        _enqueue(event, db)
        job = self._claim(db)

        jobs_crud.mark_job_failed(job.id, "downstream exploded", base_seconds=10, max_seconds=100, now=T0, db=db)

        assert "downstream exploded" in jobs_crud.get_job(job.id, db).last_error

    def test_the_lease_is_released_on_failure(self, db, event):
        _enqueue(event, db)
        job = self._claim(db)

        jobs_crud.mark_job_failed(job.id, "boom", base_seconds=10, max_seconds=100, now=T0, db=db)

        stored = jobs_crud.get_job(job.id, db)
        assert stored.locked_by is None
        assert stored.lease_expires_at is None

    def test_exhausting_the_attempt_ceiling_dead_letters(self, db, event):
        _enqueue(event, db, max_attempts=1)
        job = self._claim(db)

        status = jobs_crud.mark_job_failed(job.id, "boom", base_seconds=10, max_seconds=100, now=T0, db=db)

        assert status == jobs_crud.STATUS_DEAD_LETTER
        assert jobs_crud.get_job(job.id, db).status == jobs_crud.STATUS_DEAD_LETTER

    def test_a_dead_lettered_job_is_never_claimed_again(self, db, event):
        _enqueue(event, db, max_attempts=1)
        job = self._claim(db)
        jobs_crud.mark_job_failed(job.id, "boom", base_seconds=10, max_seconds=100, now=T0, db=db)

        assert jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db) == []

    def test_failing_an_unknown_job_reports_no_status(self, db):
        assert jobs_crud.mark_job_failed("nope", "boom", base_seconds=1, max_seconds=2, now=T0, db=db) == ""


class TestBackoffSchedule:
    def test_the_first_retry_waits_the_base_delay(self):
        assert backoff_seconds(1, base_seconds=10, max_seconds=1000, jitter=False) == 10

    @pytest.mark.parametrize(("attempts", "expected"), [(1, 10), (2, 20), (3, 40), (4, 80)])
    def test_the_delay_doubles_per_attempt(self, attempts, expected):
        assert backoff_seconds(attempts, base_seconds=10, max_seconds=10_000, jitter=False) == expected

    def test_the_delay_is_clamped_to_the_ceiling(self):
        assert backoff_seconds(20, base_seconds=10, max_seconds=100, jitter=False) == 100

    def test_a_huge_attempt_count_does_not_overflow(self):
        """The exponent is capped so ``base * 2**n`` stays finite."""
        assert backoff_seconds(10_000, base_seconds=10, max_seconds=100, jitter=False) == 100

    def test_jitter_keeps_at_least_half_the_delay(self):
        """Equal jitter spreads a burst without collapsing the delay to zero."""
        delays = [backoff_seconds(3, base_seconds=10, max_seconds=1000) for _ in range(200)]

        assert all(20 <= delay <= 40 for delay in delays)

    def test_jitter_actually_varies_the_delay(self):
        delays = {backoff_seconds(3, base_seconds=10, max_seconds=1000) for _ in range(50)}

        assert len(delays) > 1

    def test_the_delay_is_never_negative(self):
        assert backoff_seconds(0, base_seconds=0, max_seconds=0) == 0


class TestLeaseReclamation:
    def test_an_expired_lease_is_requeued(self, db, event):
        """A crashed worker must not strand its job forever."""
        _enqueue(event, db, max_attempts=3)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db)

        reclaimed = jobs_crud.reclaim_expired_leases(now=T0 + timedelta(seconds=61), db=db)

        assert reclaimed == 1
        assert db.query(ProcessingJob).one().status == jobs_crud.STATUS_PENDING

    def test_a_live_lease_is_left_alone(self, db, event):
        _enqueue(event, db)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db)

        reclaimed = jobs_crud.reclaim_expired_leases(now=T0 + timedelta(seconds=30), db=db)

        assert reclaimed == 0
        assert db.query(ProcessingJob).one().status == jobs_crud.STATUS_CLAIMED

    def test_an_expired_lease_with_no_attempts_left_is_dead_lettered(self, db, event):
        _enqueue(event, db, max_attempts=1)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db)

        jobs_crud.reclaim_expired_leases(now=T0 + timedelta(seconds=61), db=db)

        assert db.query(ProcessingJob).one().status == jobs_crud.STATUS_DEAD_LETTER

    def test_reclaiming_records_why(self, db, event):
        _enqueue(event, db, max_attempts=3)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db)

        jobs_crud.reclaim_expired_leases(now=T0 + timedelta(seconds=61), db=db)

        assert "lease expired" in db.query(ProcessingJob).one().last_error

    def test_nothing_to_reclaim_returns_zero(self, db):
        assert jobs_crud.reclaim_expired_leases(now=T0, db=db) == 0


class TestDeadLetterReplay:
    def test_a_dead_lettered_job_can_be_replayed(self, db, event):
        _enqueue(event, db, max_attempts=1)
        job = jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=60, now=T0, db=db)[0]
        jobs_crud.mark_job_failed(job.id, "boom", base_seconds=1, max_seconds=2, now=T0, db=db)

        replayed = jobs_crud.replay_dead_letter_job(job.id, now=T0, db=db)

        assert replayed is True
        assert jobs_crud.get_job(job.id, db).status == jobs_crud.STATUS_PENDING

    def test_replaying_a_job_that_is_not_dead_lettered_is_refused(self, db, event):
        job = _enqueue(event, db)

        assert jobs_crud.replay_dead_letter_job(job.id, now=T0, db=db) is False

    def test_replaying_an_unknown_job_is_refused(self, db):
        assert jobs_crud.replay_dead_letter_job("nope", now=T0, db=db) is False


class TestOutboxRelay:
    @pytest.fixture
    def registry(self):
        return JobHandlerRegistry()

    def test_an_outbox_row_fans_out_to_every_registered_subscriber(self, db, session_factory, event, clock, registry):
        registry.register("activity.created", "a", lambda _e: None)
        registry.register("activity.created", "b", lambda _e: None)
        jobs_outbox.add_to_outbox(event, now=T0, db=db)

        relayed = jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=session_factory, max_attempts=3, batch_size=10
        )

        assert relayed == 1
        assert {job.subscriber_id for job in db.query(ProcessingJob).all()} == {"a", "b"}

    def test_a_relayed_row_is_stamped_and_not_relayed_twice(self, db, session_factory, event, clock, registry):
        registry.register("activity.created", "a", lambda _e: None)
        jobs_outbox.add_to_outbox(event, now=T0, db=db)
        jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=session_factory, max_attempts=3, batch_size=10
        )

        second = jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=session_factory, max_attempts=3, batch_size=10
        )

        assert second == 0
        assert db.query(EventOutbox).one().relayed_at is not None

    def test_relaying_is_idempotent_across_overlapping_passes(self, db, session_factory, event, clock, registry):
        """Concurrent relayers may overlap; the unique constraint dedups the
        fan-out so a subscriber still runs exactly once."""
        registry.register("activity.created", "a", lambda _e: None)
        outbox_id = jobs_outbox.add_to_outbox(event, now=T0, db=db)
        jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=session_factory, max_attempts=3, batch_size=10
        )
        db.query(EventOutbox).filter_by(id=outbox_id).update({"relayed_at": None})
        db.commit()

        jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=session_factory, max_attempts=3, batch_size=10
        )

        assert db.query(ProcessingJob).count() == 1

    def test_an_event_with_no_subscribers_is_still_marked_relayed(self, db, session_factory, event, clock, registry):
        """Otherwise the row would be retried forever."""
        jobs_outbox.add_to_outbox(event, now=T0, db=db)

        jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=session_factory, max_attempts=3, batch_size=10
        )

        assert db.query(EventOutbox).one().relayed_at is not None
        assert db.query(ProcessingJob).count() == 0

    def test_the_batch_size_bounds_a_pass(self, db, session_factory, clock, registry):
        for index in range(5):
            jobs_outbox.add_to_outbox(new_event("activity.created", {"i": index}, source="test"), now=T0, db=db)

        relayed = jobs_relay.relay_outbox_once(
            registry=registry, clock=clock, session_factory=session_factory, max_attempts=3, batch_size=2
        )

        assert relayed == 2

    def test_an_uncommitted_outbox_write_joins_the_callers_transaction(self, db, event):
        """``commit=False`` is what makes the outbox write atomic with the
        producer's own domain change."""
        jobs_outbox.add_to_outbox(event, now=T0, db=db, commit=False)

        db.rollback()

        assert db.query(EventOutbox).count() == 0
