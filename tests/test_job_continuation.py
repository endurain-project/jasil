"""``ContinueJob``: a handler that made progress requeues itself without spending a deferral or a failure.

Time comes from an injected clock, so availability instants are exact and nothing sleeps.
The same tests run on whichever database the suite is pointed at.
"""

import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

import jasil.jobs._attempts as job_attempts
import jasil.jobs.crud as jobs_crud
from jasil.events import new_event
from jasil.jobs import ContinueJob, DeferJob
from jasil.jobs.dead_letter import deliver_dead_letters
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


def _runner(
    registry,
    clock,
    session_factory,
    *,
    defer_max_seconds=86400,
    max_deferrals=100,
    max_continuations=10_000,
    record_attempts=True,
):
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
        max_continuations=max_continuations,
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


def _continue_in(seconds, reason="batch done"):
    def handler(_event):
        raise ContinueJob(seconds, reason)

    return handler


def _drive(runner, clock, passes, *, step=1000):
    for _ in range(passes):
        runner.run_once()
        clock.advance(step)


class TestContinueJob:
    @pytest.mark.parametrize("delay", [float("nan"), float("inf"), "soon"])
    def test_a_delay_that_is_not_a_finite_number_is_refused(self, delay):
        with pytest.raises((ValueError, TypeError)):
            ContinueJob(delay)

    def test_the_reason_defaults_to_a_description_of_the_delay(self):
        assert str(ContinueJob(5)) == "continuing after 5 s"
        assert str(ContinueJob()) == "continuing after 0.0 s"
        assert str(ContinueJob(5, "page 3")) == "page 3"


class TestContinuation:
    def test_a_continuation_requeues_without_failing_deferring_or_jitter(self, runner, registry, db):
        registry.register("activity.created", "s", _continue_in(30))
        _enqueue(db)

        runner.run_once()

        job = _job(db)
        assert job.status == "pending"
        assert job.continuations == 1
        assert job.deferrals == 0
        assert job.attempts == 1  # the claim still counts: it is the fence
        assert job.budget_offset == 1
        assert job.last_error == "batch done"
        assert job.locked_by is None
        assert job.locked_at is None
        assert job.lease_expires_at is None
        assert job.completed_at is None
        assert _naive(job.available_at) == _naive(T0 + timedelta(seconds=30))  # exact: no jitter

    def test_the_delay_is_clamped_to_zero_and_to_the_deferral_ceiling(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _continue_in(-60))
        _enqueue(db)
        _runner(registry, clock, session_factory).run_once()
        assert _naive(_job(db).available_at) == _naive(T0)

        registry.register("activity.created", "s", _continue_in(10_000))
        db.execute(update(ProcessingJob).values(available_at=T0))
        db.commit()
        _runner(registry, clock, session_factory, defer_max_seconds=600).run_once()
        assert _naive(_job(db).available_at) == _naive(T0 + timedelta(seconds=600))

    def test_a_job_continued_past_max_deferrals_and_max_attempts_completes(self, registry, clock, session_factory, db):
        seen = []

        def handler(event):
            seen.append((event.attempt, event.continuation_count))
            if event.continuation_count < 5:
                raise ContinueJob()

        registry.register("activity.created", "s", handler)
        _enqueue(db, max_attempts=1)
        runner = _runner(registry, clock, session_factory, max_deferrals=2)

        _drive(runner, clock, 6, step=0)

        job = _job(db)
        assert job.status == "completed"
        assert (job.attempts, job.continuations, job.deferrals) == (6, 5, 0)
        assert seen == [(1, count) for count in range(6)]

    def test_a_continuation_resets_the_failure_budget(self, registry, clock, session_factory, db):
        script = iter(["fail", "continue", "fail", "fail"])
        seen = []

        def handler(event):
            seen.append((event.attempt, event.is_last_attempt))
            if next(script) == "fail":
                raise RuntimeError("boom")
            raise ContinueJob()

        registry.register("activity.created", "s", handler)
        _enqueue(db, max_attempts=2)
        runner = _runner(registry, clock, session_factory)

        _drive(runner, clock, 3)
        assert _job(db).status == "pending"  # the failure before the continuation no longer counts
        _drive(runner, clock, 1)

        assert _job(db).status == "dead_letter"
        assert seen == [(1, False), (2, True), (1, False), (2, True)]

    def test_a_deferral_after_a_continuation_still_spends_no_attempt(self, registry, clock, session_factory, db):
        script = iter(["continue", "defer", "fail", "done"])
        seen = []

        def handler(event):
            seen.append((event.attempt, event.deferral_count, event.continuation_count))
            step = next(script)
            if step == "continue":
                raise ContinueJob()
            if step == "defer":
                raise DeferJob(clock.now())
            if step == "fail":
                raise RuntimeError("boom")

        registry.register("activity.created", "s", handler)
        _enqueue(db, max_attempts=2)

        _drive(_runner(registry, clock, session_factory), clock, 4)

        assert _job(db).status == "completed"
        assert seen == [(1, 0, 0), (1, 0, 1), (1, 1, 1), (2, 1, 1)]

    def test_a_lost_claim_changes_nothing(self, runner, registry, session_factory, db, caplog):
        def handler(_event):
            with session_factory() as other:
                other.execute(update(ProcessingJob).values(locked_by="worker-2"))
                other.commit()
            raise ContinueJob(5)

        registry.register("activity.created", "s", handler)
        _enqueue(db)

        with caplog.at_level(logging.WARNING):
            runner.run_once()

        job = _job(db)
        assert job.status == "claimed"
        assert job.locked_by == "worker-2"
        assert (job.continuations, job.budget_offset) == (0, 0)
        assert job.last_error is None
        assert "continuation skipped because claim ownership was lost" in caplog.text


class TestContinuationBound:
    def test_a_job_past_max_continuations_fails_like_any_exception(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _continue_in(0))
        _enqueue(db, max_attempts=3)
        runner = _runner(registry, clock, session_factory, max_continuations=2)

        _drive(runner, clock, 3, step=0)

        job = _job(db)
        assert job.status == "pending"
        assert (job.attempts, job.continuations) == (3, 2)
        assert "continued more than 2 times" in job.last_error
        assert "batch done" in job.last_error
        assert [a.outcome for a in _attempts(db)] == ["continued", "continued", "failed"]

    def test_a_job_past_max_continuations_dead_letters_once_its_attempts_run_out(
        self, registry, clock, session_factory, db
    ):
        registry.register("activity.created", "s", _continue_in(0))
        _enqueue(db, max_attempts=2)

        _drive(_runner(registry, clock, session_factory, max_continuations=1), clock, 3)

        assert _job(db).status == "dead_letter"

    def test_zero_max_continuations_disables_continuation(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _continue_in(0))
        _enqueue(db)
        _runner(registry, clock, session_factory, max_continuations=0).run_once()
        job = _job(db)
        assert job.continuations == 0
        assert "continued more than 0 times" in job.last_error


class TestContinuationRecord:
    def test_the_attempt_history_says_continued(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _continue_in(0))
        _enqueue(db)
        _drive(_runner(registry, clock, session_factory), clock, 3)

        rows = _attempts(db)
        assert [row.outcome for row in rows] == [job_attempts.OUTCOME_CONTINUED] * 3
        assert all(row.finished_at is not None and row.error == "batch done" for row in rows)

    def test_a_continuation_logs_at_info_and_never_at_error(self, runner, registry, db, caplog):
        registry.register("activity.created", "s", _continue_in(30))
        _enqueue(db)

        with caplog.at_level(logging.DEBUG):
            runner.run_once()

        (record,) = [r for r in caplog.records if r.getMessage() == "Durable job continued"]
        assert record.levelno == logging.INFO
        assert record.reason == "batch done"
        assert record.available_at.startswith("2026-01-01T12:00:30")
        assert record.continuations == 1
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]

    def test_a_continuation_is_written_without_history_when_attempts_are_not_recorded(
        self, registry, clock, session_factory, db
    ):
        registry.register("activity.created", "s", _continue_in(0))
        _enqueue(db)
        _runner(registry, clock, session_factory, record_attempts=False).run_once()
        assert _job(db).continuations == 1
        assert _attempts(db) == []

    def test_the_lease_keeper_stops_tracking_a_continued_job(self, runner, registry, db):
        registry.register("activity.created", "s", _continue_in(0))
        _enqueue(db)
        runner.run_once()
        assert runner.leases._held == {}
        assert runner.leases._running is None


class TestContinuationAccounting:
    @pytest.mark.parametrize(("budget_offset", "status"), [(3, "pending"), (2, "dead_letter")])
    def test_the_reaper_counts_failures_since_the_last_continuation(self, db, budget_offset, status):
        job_id = _enqueue(db, max_attempts=2).id
        db.execute(
            update(ProcessingJob)
            .where(ProcessingJob.id == job_id)
            .values(
                status="claimed",
                attempts=5,
                deferrals=1,
                continuations=3,
                budget_offset=budget_offset,
                locked_by="w",
                lease_expires_at=T0 - timedelta(seconds=1),
            )
        )
        db.commit()

        jobs_crud.reclaim_expired_leases(now=T0, db=db)

        assert _job(db).status == status  # 5 claims - 1 deferred - offset = the attempt that expired

    def test_a_replay_restarts_the_continuation_count_and_the_budget(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _continue_in(0))
        job_id = _enqueue(db).id
        _runner(registry, clock, session_factory).run_once()
        db.execute(update(ProcessingJob).where(ProcessingJob.id == job_id).values(status="dead_letter"))
        db.commit()

        assert jobs_crud.replay_dead_letter_job(job_id, now=T0, db=db)

        job = _job(db)
        assert (job.continuations, job.budget_offset) == (0, 0)

    def test_the_admin_views_show_the_continuations(self, registry, clock, session_factory, db):
        def handler(event):
            if event.continuation_count < 2:
                raise ContinueJob()
            raise RuntimeError("boom")

        registry.register("activity.created", "s", handler)
        job_id = _enqueue(db, max_attempts=1).id
        _drive(_runner(registry, clock, session_factory), clock, 3)

        db.rollback()
        (dead,) = jobs_crud.get_jobs_summary(db, now=T0 + timedelta(days=1)).recent_dead_letter
        assert (dead.attempts, dead.continuations, dead.max_attempts) == (3, 2, 1)
        detail = job_attempts.get_job_detail(job_id, db=db)
        assert detail is not None
        assert detail.continuations == 2

    def test_a_dead_letter_callback_event_carries_the_budget_counts(self, registry, clock, session_factory, db):
        events = []

        def handler(event):
            if event.continuation_count < 2:
                raise ContinueJob()
            raise RuntimeError("boom")

        registry.register("activity.created", "s", handler, on_dead_letter=lambda event, error: events.append(event))
        _enqueue(db, max_attempts=1)
        _drive(_runner(registry, clock, session_factory), clock, 3)
        deliver_dead_letters(registry=registry, session_factory=session_factory, clock=clock)

        assert [(e.retry_count, e.max_attempts, e.continuation_count) for e in events] == [(1, 1, 2)]
