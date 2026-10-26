"""``DeferJob``: a handler asks to run again later, without failing or spending an attempt.

Time comes from an injected clock, so retry instants are exact and nothing sleeps.
The same tests run on whichever database the suite is pointed at.
"""

import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

import jasil.jobs._attempts as job_attempts
import jasil.jobs.backoff as jobs_backoff
import jasil.jobs.crud as jobs_crud
from jasil.events import new_event
from jasil.jobs import DeferJob
from jasil.jobs.models import JobAttempt, ProcessingJob
from jasil.jobs.registry import JobHandlerRegistry
from jasil.jobs.runner import JobRunner
from jasil.testing import FixedClock

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _naive(moment):
    return moment.replace(tzinfo=None) if moment is not None else None


@pytest.fixture
def clock():
    return FixedClock(moment=T0)


@pytest.fixture
def registry():
    return JobHandlerRegistry()


def _runner(registry, clock, session_factory, *, defer_max_seconds=86400, max_deferrals=100, record_attempts=True):
    return JobRunner(
        registry=registry,
        clock=clock,
        session_factory=session_factory,
        worker_id="worker-1",
        lease_seconds=60,
        max_runtime_seconds=600,
        heartbeat_interval_seconds=15,
        batch_size=10,
        backoff_base_seconds=10,
        backoff_max_seconds=100,
        defer_max_seconds=defer_max_seconds,
        max_deferrals=max_deferrals,
        record_attempts=record_attempts,
    )


@pytest.fixture
def runner(registry, clock, session_factory):
    return _runner(registry, clock, session_factory)


def _enqueue(db, *, max_attempts=3):
    event = new_event("activity.created", {"id": 1}, source="test")
    return jobs_crud.enqueue_job(event, "s", max_attempts=max_attempts, now=T0, db=db)


def _job(db) -> ProcessingJob:
    db.rollback()  # see the runner's commits under REPEATABLE READ
    return db.query(ProcessingJob).one()


def _attempts(db):
    db.rollback()
    return list(db.execute(select(JobAttempt).order_by(JobAttempt.claimed_at, JobAttempt.attempt)).scalars().all())


def _defer_in(seconds, reason="rate limited"):
    def handler(_event):
        raise DeferJob(T0 + timedelta(seconds=seconds), reason)

    return handler


class TestDeferral:
    def test_a_deferral_reschedules_without_failing_or_spending_an_attempt(self, runner, registry, clock, db):
        registry.register("activity.created", "s", _defer_in(300))
        _enqueue(db)

        runner.run_once()

        job = _job(db)
        assert job.status == "pending"
        assert job.deferrals == 1
        assert job.attempts == 1  # the claim still counts: it is the fence
        assert job.last_error == "rate limited"
        assert job.locked_by is None
        assert job.locked_at is None
        assert job.lease_expires_at is None
        assert job.completed_at is None
        # 300 s plus up to 10% jitter.
        wait = (job.available_at.replace(tzinfo=UTC) - T0).total_seconds()
        assert 300 <= wait <= 330

    def test_a_job_deferred_more_often_than_max_attempts_still_runs(self, registry, clock, session_factory, db):
        seen = []

        def handler(event):
            seen.append((event.retry_count, event.attempt, event.is_last_attempt, event.deferral_count))
            if event.deferral_count < 5:
                raise DeferJob(clock.now() + timedelta(seconds=10))

        registry.register("activity.created", "s", handler)
        _enqueue(db, max_attempts=2)
        runner = _runner(registry, clock, session_factory)

        for _ in range(6):
            runner.run_once()
            clock.advance(11)

        job = _job(db)
        assert job.status == "completed"
        assert job.attempts == 6
        assert job.deferrals == 5
        # Every run is attempt 1 of 2: a deferral never advances the budget.
        assert seen == [(1, 1, False, count) for count in range(6)]

    def test_a_deferred_job_is_claimable_only_from_its_retry_time(self, runner, registry, clock, db):
        calls = []
        registry.register("activity.created", "s", lambda _event: calls.append(1) or _defer_in(100)(_event))
        _enqueue(db)
        runner.run_once()
        assert len(calls) == 1

        clock.advance(99)
        assert runner.run_once() == 0
        clock.advance(12)  # past the 10% jitter as well
        assert runner.run_once() == 1
        assert len(calls) == 2

    def test_a_lost_claim_changes_nothing(self, runner, registry, session_factory, db, caplog):
        def handler(_event):
            with session_factory() as other:
                other.execute(update(ProcessingJob).values(locked_by="worker-2"))
                other.commit()
            raise DeferJob(T0 + timedelta(seconds=300), "rate limited")

        registry.register("activity.created", "s", handler)
        _enqueue(db)

        with caplog.at_level(logging.WARNING):
            runner.run_once()

        job = _job(db)
        assert job.status == "claimed"
        assert job.locked_by == "worker-2"
        assert job.deferrals == 0
        assert job.last_error is None
        assert "deferral skipped because claim ownership was lost" in caplog.text
        assert _attempts(db)[0].outcome == job_attempts.OUTCOME_LEASE_EXPIRED  # no later claim: it ran once

    def test_a_past_retry_time_is_clamped_to_now(self, runner, registry, db):
        registry.register("activity.created", "s", _defer_in(-3600))
        _enqueue(db)
        runner.run_once()
        assert _naive(_job(db).available_at) == _naive(T0)

    def test_a_far_future_retry_time_is_clamped_to_the_ceiling_jitter_included(
        self, registry, clock, session_factory, db
    ):
        registry.register("activity.created", "s", _defer_in(10 * 365 * 86400))
        _enqueue(db)
        _runner(registry, clock, session_factory, defer_max_seconds=600).run_once()
        wait = (_job(db).available_at.replace(tzinfo=UTC) - T0).total_seconds()
        assert 600 * 0.9 <= wait <= 600

    def test_a_naive_retry_time_is_read_as_utc(self, runner, registry, db):
        def handler(_event):
            raise DeferJob((T0 + timedelta(seconds=300)).replace(tzinfo=None))

        registry.register("activity.created", "s", handler)
        _enqueue(db)
        runner.run_once()
        wait = (_job(db).available_at.replace(tzinfo=UTC) - T0).total_seconds()
        assert 300 <= wait <= 330

    def test_the_reason_defaults_to_a_description_of_the_retry_time(self, runner, registry, db):
        def handler(_event):
            raise DeferJob(T0 + timedelta(seconds=300))

        registry.register("activity.created", "s", handler)
        _enqueue(db)
        runner.run_once()
        assert "deferred until" in _job(db).last_error


class TestDeferralBound:
    def test_a_job_past_max_deferrals_fails_like_any_exception(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _defer_in(0))
        _enqueue(db, max_attempts=3)
        runner = _runner(registry, clock, session_factory, max_deferrals=2)

        runner.run_once()
        runner.run_once()
        assert (_job(db).status, _job(db).deferrals) == ("pending", 2)

        clock.advance(1)
        runner.run_once()  # the third request is a failure

        job = _job(db)
        assert job.status == "pending"
        assert job.deferrals == 2
        assert job.attempts == 3
        assert "deferred more than 2 times" in job.last_error
        assert "rate limited" in job.last_error
        assert [a.outcome for a in _attempts(db)] == ["deferred", "deferred", "failed"]

    def test_a_job_past_max_deferrals_dead_letters_once_its_attempts_run_out(
        self, registry, clock, session_factory, db
    ):
        callbacks = []
        registry.register(
            "activity.created", "s", _defer_in(0), on_dead_letter=lambda event, error: callbacks.append(error)
        )
        _enqueue(db, max_attempts=1)
        runner = _runner(registry, clock, session_factory, max_deferrals=1)

        runner.run_once()
        assert callbacks == []
        clock.advance(1)
        runner.run_once()

        assert _job(db).status == "dead_letter"
        assert len(callbacks) == 1

    def test_zero_max_deferrals_disables_deferral(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _defer_in(0))
        _enqueue(db)
        _runner(registry, clock, session_factory, max_deferrals=0).run_once()
        job = _job(db)
        assert job.deferrals == 0
        assert "deferred more than 0 times" in job.last_error


class TestDeferralRecord:
    def test_the_attempt_history_says_deferred_and_no_dead_letter_callback_runs(
        self, registry, clock, session_factory, db
    ):
        callbacks = []
        registry.register(
            "activity.created", "s", _defer_in(60), on_dead_letter=lambda event, error: callbacks.append(error)
        )
        _enqueue(db, max_attempts=1)
        runner = _runner(registry, clock, session_factory)

        for _ in range(4):
            runner.run_once()
            clock.advance(100)

        assert callbacks == []
        assert _job(db).status == "pending"  # max_attempts=1, deferred four times, never dead-lettered
        rows = _attempts(db)
        assert [row.outcome for row in rows] == ["deferred"] * 4
        assert all(row.finished_at is not None and row.error == "rate limited" for row in rows)

    def test_a_deferral_logs_at_info_with_the_retry_time_and_never_at_error(self, runner, registry, db, caplog):
        registry.register("activity.created", "s", _defer_in(300))
        _enqueue(db)

        with caplog.at_level(logging.DEBUG):
            runner.run_once()

        records = [r for r in caplog.records if r.getMessage() == "Durable job deferred"]
        assert len(records) == 1
        record = records[0]
        assert record.levelno == logging.INFO
        assert record.reason == "rate limited"
        assert record.retry_at.startswith("2026-01-01T12")
        assert record.deferrals == 1
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_a_deferral_is_written_without_history_when_attempts_are_not_recorded(
        self, registry, clock, session_factory, db
    ):
        registry.register("activity.created", "s", _defer_in(60))
        _enqueue(db)
        _runner(registry, clock, session_factory, record_attempts=False).run_once()
        assert _job(db).deferrals == 1
        assert _attempts(db) == []

    def test_the_lease_keeper_stops_tracking_a_deferred_job(self, runner, registry, db):
        registry.register("activity.created", "s", _defer_in(60))
        _enqueue(db)
        runner.run_once()
        assert runner.leases._held == {}
        assert runner.leases._running is None


class TestDeferralAccounting:
    def test_the_reaper_requeues_a_job_whose_claims_were_mostly_deferred(self, clock, db):
        job = _enqueue(db, max_attempts=2)
        job_id = job.id
        db.execute(
            update(ProcessingJob)
            .where(ProcessingJob.id == job_id)
            .values(
                status="claimed",
                attempts=4,
                deferrals=3,
                locked_by="w",
                lease_expires_at=T0 - timedelta(seconds=1),
            )
        )
        db.commit()

        jobs_crud.reclaim_expired_leases(now=T0, db=db)

        assert _job(db).status == "pending"  # 4 claims, 3 deferred: attempt 1 of 2

    def test_the_reaper_dead_letters_a_job_that_spent_its_attempts(self, db):
        job_id = _enqueue(db, max_attempts=2).id
        db.execute(
            update(ProcessingJob)
            .where(ProcessingJob.id == job_id)
            .values(
                status="claimed", attempts=4, deferrals=2, locked_by="w", lease_expires_at=T0 - timedelta(seconds=1)
            )
        )
        db.commit()

        jobs_crud.reclaim_expired_leases(now=T0, db=db)

        assert _job(db).status == "dead_letter"

    def test_an_ordinary_failure_after_deferrals_counts_only_real_attempts(self, registry, clock, session_factory, db):
        def handler(event):
            if event.deferral_count < 3:
                raise DeferJob(clock.now())
            raise RuntimeError("boom")

        registry.register("activity.created", "s", handler)
        _enqueue(db, max_attempts=2)
        runner = _runner(registry, clock, session_factory)

        for _ in range(4):
            runner.run_once()
            clock.advance(1000)
        assert (_job(db).status, _job(db).attempts, _job(db).deferrals) == ("pending", 4, 3)  # attempt 1 failed
        runner.run_once()

        assert _job(db).status == "dead_letter"  # attempt 2 of 2

    def test_a_replay_restarts_the_deferral_count(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _defer_in(0))
        job_id = _enqueue(db).id
        _runner(registry, clock, session_factory).run_once()
        db.execute(update(ProcessingJob).where(ProcessingJob.id == job_id).values(status="dead_letter"))
        db.commit()

        assert jobs_crud.replay_dead_letter_job(job_id, now=T0, db=db)

        assert _job(db).deferrals == 0

    def test_a_dead_letter_callback_event_carries_the_budget_counts(self, registry, clock, session_factory, db):
        events = []

        def handler(event):
            if event.deferral_count < 2:
                raise DeferJob(clock.now())
            raise RuntimeError("boom")

        registry.register("activity.created", "s", handler, on_dead_letter=lambda event, error: events.append(event))
        _enqueue(db, max_attempts=1)
        runner = _runner(registry, clock, session_factory)
        for _ in range(3):
            runner.run_once()
            clock.advance(1000)

        assert [(e.retry_count, e.max_attempts, e.deferral_count) for e in events] == [(1, 1, 2)]


class TestDeferSeconds:
    def test_the_request_is_clamped_to_the_ceiling_and_to_zero(self):
        assert jobs_backoff.defer_seconds(-5, max_seconds=100, jitter=False) == 0
        assert jobs_backoff.defer_seconds(50, max_seconds=100, jitter=False) == 50
        assert jobs_backoff.defer_seconds(5000, max_seconds=100, jitter=False) == 100

    def test_jitter_adds_at_most_ten_percent_and_never_passes_the_ceiling(self):
        for _ in range(200):
            assert 100 <= jobs_backoff.defer_seconds(100, max_seconds=1000) <= 110
            assert jobs_backoff.defer_seconds(1000, max_seconds=1000) <= 1000
