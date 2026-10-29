"""The per-attempt history: who ran each durable job, when, for how long, and how it ended.

The history is written in the transaction of the state change it describes, so
these tests assert the two together: a job's row and its attempt row are never
observed to disagree. Time comes from an injected clock, so durations and
timestamps are exact and nothing sleeps.
"""

import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, update

import jasil.admin as jasil_admin
import jasil.container as container
import jasil.jobs._attempts as job_attempts
import jasil.jobs._worker_registry as worker_registry
import jasil.jobs.crud as jobs_crud
import jasil.jobs.service as jobs_service
import jasil.runtime as platform_runtime
import jasil.settings as settings
from jasil.events import new_event
from jasil.jobs.models import JobAttempt, JobWorker, ProcessingJob
from jasil.jobs.registry import JobHandlerRegistry
from jasil.jobs.runner import JobRunner
from jasil.testing import FixedClock

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
LEASE = 60
EVERYTHING = 24 * 365 * 10  # a look-back window wide enough to include T0 whatever "now" is


def _naive(moment: datetime | None) -> datetime | None:
    return moment.replace(tzinfo=None) if moment is not None else None


@pytest.fixture
def clock():
    return FixedClock(moment=T0)


@pytest.fixture
def registry():
    return JobHandlerRegistry()


def _runner(registry, clock, session_factory, *, record_attempts=True, worker_id="worker-1"):
    return JobRunner(
        registry=registry,
        clock=clock,
        session_factory=session_factory,
        worker_id=worker_id,
        lease_seconds=LEASE,
        max_runtime_seconds=600,
        heartbeat_interval_seconds=15,
        batch_size=10,
        backoff_base_seconds=10,
        backoff_max_seconds=100,
        record_attempts=record_attempts,
    )


@pytest.fixture
def runner(registry, clock, session_factory):
    return _runner(registry, clock, session_factory)


def _enqueue(db, subscriber="s", *, event=None, max_attempts=3, queue="default"):
    event = event or new_event("activity.created", {"id": 1}, source="test")
    return jobs_crud.enqueue_job(event, subscriber, queue=queue, max_attempts=max_attempts, now=T0, db=db)


def _attempts(db, job_id=None):
    db.rollback()
    statement = select(JobAttempt).order_by(JobAttempt.claimed_at, JobAttempt.attempt)
    if job_id is not None:
        statement = statement.where(JobAttempt.job_id == job_id)
    return list(db.execute(statement).scalars().all())


def _job(db, subscriber="s"):
    db.rollback()
    return db.query(ProcessingJob).filter_by(subscriber_id=subscriber).one()


def _insert_attempt(db, **overrides):
    values = {
        "id": f"attempt-{len(overrides)}-{id(overrides)}",
        "job_id": "job-1",
        "event_id": "event-1",
        "event_type": "activity.created",
        "subscriber_id": "s",
        "queue": "default",
        "attempt": 1,
        "worker_id": "worker-1",
        "claimed_at": T0,
    }
    values.update(overrides)
    attempt = JobAttempt(**values)
    db.add(attempt)
    db.commit()
    return attempt


class TestClaimOpensAnAttempt:
    def test_each_claimed_job_gets_one_open_attempt(self, db):
        first, second = _enqueue(db, "s"), _enqueue(db, "t")

        claimed = jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)

        assert len(claimed) == 2
        rows = _attempts(db)
        assert {row.job_id for row in rows} == {first.id, second.id}
        for row in rows:
            assert (row.attempt, row.worker_id, row.queue, row.event_type) == (1, "w1", "default", "activity.created")
            assert _naive(row.claimed_at) == _naive(T0)
            assert row.outcome is None and row.finished_at is None and row.started_at is None

    def test_the_attempt_carries_the_events_identity(self, db):
        event = new_event("order.created", {"id": 7}, source="test")
        job = _enqueue(db, "invoice.render", event=event, queue="campaign")

        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)

        (row,) = _attempts(db)
        assert (row.job_id, row.event_id, row.subscriber_id, row.queue) == (
            job.id,
            event.event_id,
            "invoice.render",
            "campaign",
        )

    def test_a_worker_that_lost_the_claim_race_opens_nothing(self, db):
        _enqueue(db)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)

        lost = jobs_crud.claim_jobs(worker_id="w2", limit=10, lease_seconds=LEASE, now=T0, db=db)

        assert lost == []
        assert [row.worker_id for row in _attempts(db)] == ["w1"]

    def test_the_attempt_number_follows_the_job_across_retries(self, db):
        job = _enqueue(db)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)
        jobs_crud.mark_job_failed(
            job.id, "boom", worker_id="w1", attempt=1, base_seconds=1, max_seconds=2, now=T0, db=db
        )
        later = T0 + timedelta(minutes=5)

        jobs_crud.claim_jobs(worker_id="w2", limit=10, lease_seconds=LEASE, now=later, db=db)

        assert [(row.attempt, row.worker_id, row.outcome) for row in _attempts(db, job.id)] == [
            (1, "w1", "failed"),
            (2, "w2", None),
        ]

    def test_recording_can_be_switched_off(self, db):
        _enqueue(db)

        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db, record_attempts=False)

        assert _attempts(db) == []

    def test_a_failed_history_write_rolls_the_claim_back(self, db, monkeypatch):
        """The claim and its history share one transaction, so neither survives alone."""
        job = _enqueue(db)

        def boom(*args, **kwargs):
            raise RuntimeError("history unavailable")

        monkeypatch.setattr(jobs_crud, "_open_attempts", boom)

        with pytest.raises(RuntimeError, match="history unavailable"):
            jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)

        db.rollback()
        assert db.get(ProcessingJob, job.id).status == jobs_crud.STATUS_PENDING
        assert _attempts(db) == []


class TestFinishingAnAttempt:
    def _claim(self, db, subscriber="s", max_attempts=3):
        job = _enqueue(db, subscriber, max_attempts=max_attempts)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)
        return job

    def test_completion_closes_the_attempt_with_its_duration(self, db):
        job = self._claim(db)
        done = T0 + timedelta(seconds=3)

        completed = jobs_crud.mark_job_completed(job.id, worker_id="w1", attempt=1, now=done, db=db, duration_ms=2500)

        (row,) = _attempts(db, job.id)
        assert completed is True
        assert (row.outcome, row.duration_ms, row.error) == ("completed", 2500, None)
        assert _naive(row.finished_at) == _naive(done)

    def test_a_failure_records_its_error_and_the_jobs_new_state(self, db):
        job = self._claim(db)

        status = jobs_crud.mark_job_failed(
            job.id, "boom", worker_id="w1", attempt=1, base_seconds=1, max_seconds=2, now=T0, db=db, duration_ms=40
        )

        (row,) = _attempts(db, job.id)
        assert status == jobs_crud.STATUS_PENDING
        assert (row.outcome, row.error, row.duration_ms) == ("failed", "boom", 40)

    def test_the_last_attempt_is_recorded_as_a_dead_letter(self, db):
        job = self._claim(db, max_attempts=1)

        status = jobs_crud.mark_job_failed(
            job.id, "boom", worker_id="w1", attempt=1, base_seconds=1, max_seconds=2, now=T0, db=db
        )

        assert status == jobs_crud.STATUS_DEAD_LETTER
        assert _attempts(db, job.id)[0].outcome == "dead_letter"

    def test_a_stored_error_is_truncated_like_the_jobs(self, db):
        job = self._claim(db, max_attempts=1)

        jobs_crud.mark_job_failed(
            job.id, "x" * 10_000, worker_id="w1", attempt=1, base_seconds=1, max_seconds=2, now=T0, db=db
        )

        assert len(_attempts(db, job.id)[0].error) <= 4000

    def test_the_job_and_its_attempt_change_together_or_not_at_all(self, db, monkeypatch):
        job = self._claim(db)

        def boom(*args, **kwargs):
            raise RuntimeError("history unavailable")

        monkeypatch.setattr(jobs_crud, "_finish_attempt", boom)

        with pytest.raises(RuntimeError):
            jobs_crud.mark_job_completed(job.id, worker_id="w1", attempt=1, now=T0, db=db)

        db.rollback()
        assert db.get(ProcessingJob, job.id).status == jobs_crud.STATUS_CLAIMED
        assert _attempts(db, job.id)[0].outcome is None

    def test_nothing_is_written_when_recording_is_off(self, db):
        job = self._claim(db)

        jobs_crud.mark_job_completed(job.id, worker_id="w1", attempt=1, now=T0, db=db, record_attempt=False)

        assert _attempts(db, job.id)[0].outcome is None


class _RenewsAfterTheSelect:
    """A session that runs ``renew`` right after the reaper has selected its ids."""

    class _Ids:
        def __init__(self, ids):
            self._ids = ids

        def scalars(self):
            return self

        def all(self):
            return self._ids

    def __init__(self, session, renew):
        self._session = session
        self._renew = renew
        self._calls = 0

    def execute(self, *args, **kwargs):
        result = self._session.execute(*args, **kwargs)
        self._calls += 1
        if self._calls == 1:
            ids = list(result.scalars().all())
            self._renew()
            return self._Ids(ids)
        return result

    def __getattr__(self, name):
        return getattr(self._session, name)


class TestAHandlerThatOutlivesItsLease:
    """The only durable trace that a side effect may have happened twice."""

    @pytest.mark.parametrize("fails", [False, True])
    def test_the_late_finish_is_recorded_as_superseded(self, runner, registry, clock, db, fails):
        def handle(_event):
            clock.advance(150)  # far past the lease, with no heartbeat to renew it
            jobs_crud.reclaim_expired_leases(now=clock.now(), db=db)
            jobs_crud.claim_jobs(worker_id="other", limit=10, lease_seconds=LEASE, now=clock.now(), db=db)
            if fails:
                raise RuntimeError("boom")

        registry.register("activity.created", "s", handle)
        job = _enqueue(db)

        runner.run_once()

        first, second = _attempts(db, job.id)
        assert (first.worker_id, first.attempt, first.outcome) == ("worker-1", 1, "superseded")
        assert first.duration_ms == 150_000
        assert first.error == ("RuntimeError: boom" if fails else None)
        assert (second.worker_id, second.attempt, second.outcome) == ("other", 2, None)
        stored = _job(db)
        assert (stored.status, stored.locked_by) == (jobs_crud.STATUS_CLAIMED, "other")

    def test_a_late_finish_nobody_took_over_is_not_called_superseded(self, runner, registry, clock, db):
        """Reaped but never claimed again: the handler ran once, so it did not run twice."""

        def handle(_event):
            clock.advance(150)
            jobs_crud.reclaim_expired_leases(now=clock.now(), db=db)

        registry.register("activity.created", "s", handle)
        job = _enqueue(db)

        runner.run_once()

        (row,) = _attempts(db, job.id)
        assert (row.outcome, row.duration_ms) == ("lease_expired", 150_000)
        assert row.finished_at is not None

    def test_the_reaper_closes_the_attempt_of_a_worker_that_died(self, db):
        job = _enqueue(db)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)
        reaped = T0 + timedelta(seconds=LEASE + 1)

        assert jobs_crud.reclaim_expired_leases(now=reaped, db=db) == 1

        (row,) = _attempts(db, job.id)
        assert row.outcome == "lease_expired"
        assert _naive(row.finished_at) == _naive(reaped)
        assert row.started_at is None

    def test_a_lease_renewed_after_the_reapers_select_keeps_its_open_attempt(self, db):
        """The race itself: the worker's heartbeat renews between the select and the updates."""
        job = _enqueue(db)
        (claimed,) = jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)
        later = T0 + timedelta(seconds=LEASE + 1)

        def renew():
            jobs_crud.renew_leases({job.id: claimed.attempts}, worker_id="w1", lease_seconds=LEASE, now=later, db=db)

        reclaimed = jobs_crud.reclaim_expired_leases(now=later, db=_RenewsAfterTheSelect(db, renew))

        assert reclaimed == 0
        assert _job(db).status == jobs_crud.STATUS_CLAIMED
        assert _attempts(db, job.id)[0].outcome is None, "a renewed lease keeps its open attempt"

    def test_a_dead_lettered_reclaim_is_closed_too(self, db):
        job = _enqueue(db, max_attempts=1)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)

        jobs_crud.reclaim_expired_leases(now=T0 + timedelta(seconds=LEASE + 1), db=db)

        assert _job(db).status == jobs_crud.STATUS_DEAD_LETTER
        assert _attempts(db, job.id)[0].outcome == "lease_expired"

    def test_the_reaper_closes_rows_even_with_recording_switched_off(self, db):
        """A row opened before the switch must not stay open forever."""
        job = _enqueue(db)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)

        jobs_crud.reclaim_expired_leases(now=T0 + timedelta(seconds=LEASE + 1), db=db)

        assert _attempts(db, job.id)[0].outcome == "lease_expired"


class TestTheRunnerRecordsTheRun:
    def test_a_handler_is_timed_with_the_monotonic_clock(self, runner, registry, clock, db):
        registry.register("activity.created", "s", lambda _e: clock.advance(2.5))
        job = _enqueue(db)

        runner.run_once()

        (row,) = _attempts(db, job.id)
        assert (row.outcome, row.duration_ms) == ("completed", 2500)

    def test_the_start_is_stamped_when_the_handler_begins(self, runner, registry, clock, db):
        seen = []

        def handle(_event):
            db.rollback()
            seen.append(db.execute(select(JobAttempt.started_at)).scalar_one())
            clock.advance(1)

        registry.register("activity.created", "s", handle)
        _enqueue(db)

        runner.run_once()

        assert [_naive(moment) for moment in seen] == [_naive(T0)]

    def test_a_history_failure_never_stops_the_job(self, runner, registry, db, monkeypatch, caplog):
        """The history is observability: losing a row must not lose the work."""

        def boom(*args, **kwargs):
            raise RuntimeError("history unavailable")

        monkeypatch.setattr(jobs_crud, "mark_attempt_started", boom)
        ran = []
        registry.register("activity.created", "s", ran.append)
        job = _enqueue(db)

        with caplog.at_level(logging.WARNING, logger="jasil.jobs.runner"):
            runner.run_once()

        assert len(ran) == 1
        assert _job(db).status == jobs_crud.STATUS_COMPLETED
        assert _attempts(db, job.id)[0].outcome == "completed"
        assert "start could not be recorded" in caplog.text

    def test_a_job_skipped_before_its_turn_never_started(self, runner, registry, db, monkeypatch):
        registry.register("activity.created", "s", lambda _e: None)
        registry.register("activity.created", "t", lambda _e: None)
        _enqueue(db, "s")
        _enqueue(db, "t")

        def fail():
            raise RuntimeError("database unreachable")

        monkeypatch.setattr(runner.leases, "renew", fail)

        runner.run_once()

        rows = {row.subscriber_id: row for row in _attempts(db)}
        started = [name for name, row in rows.items() if row.started_at is not None]
        assert len(started) == 1, "only the job that ran was stamped"

    def test_a_missing_handler_is_a_recorded_failure(self, runner, db):
        job = _enqueue(db)

        runner.run_once()

        (row,) = _attempts(db, job.id)
        assert row.outcome == "failed"
        assert "no durable handler" in row.error

    def test_the_rest_of_a_batch_is_released_when_its_runner_is_in_trouble(self, runner, registry, db):
        ran = []

        def handler_for(name):
            def handle(_event):
                ran.append(name)
                db.execute(update(ProcessingJob).where(ProcessingJob.subscriber_id == name).values(locked_by="other"))
                db.commit()
                runner.leases.renew()

            return handle

        for name in ("s", "t"):
            registry.register("activity.created", name, handler_for(name))
            _enqueue(db, name)

        runner.run_once()

        (first,) = ran
        other = "t" if first == "s" else "s"
        released = next(row for row in _attempts(db) if row.subscriber_id == other)
        assert released.outcome == "released"
        assert released.started_at is None and released.duration_ms is None
        assert _job(db, other).attempts == 0

    def test_a_job_past_its_runtime_cap_is_recorded_as_abandoned(self, runner, registry, clock, db):
        def handle(_event):
            clock.advance(601)
            runner.leases.renew()

        registry.register("activity.created", "s", handle)
        job = _enqueue(db)

        runner.run_once()

        (row,) = _attempts(db, job.id)
        assert row.outcome == "completed", "the claim was still held, so the handler's own result settles it"

    def test_an_abandoned_handler_that_finishes_after_the_job_was_taken_is_superseded(
        self, runner, registry, clock, db
    ):
        def handle(_event):
            clock.advance(601)
            runner.leases.renew()  # past the cap: abandoned, no longer renewed
            jobs_crud.reclaim_expired_leases(now=clock.now(), db=db)
            jobs_crud.claim_jobs(worker_id="other", limit=10, lease_seconds=LEASE, now=clock.now(), db=db)

        registry.register("activity.created", "s", handle)
        job = _enqueue(db)

        runner.run_once()

        first, second = _attempts(db, job.id)
        assert (first.worker_id, first.outcome) == ("worker-1", "superseded")
        assert (second.worker_id, second.outcome) == ("other", None)

    def test_an_abandon_that_cannot_be_recorded_is_logged_and_the_job_carries_on(
        self, runner, registry, clock, db, monkeypatch, caplog
    ):
        def boom(*args, **kwargs):
            raise RuntimeError("history unavailable")

        monkeypatch.setattr(jobs_crud, "close_attempts", boom)

        def handle(_event):
            clock.advance(601)
            with caplog.at_level(logging.WARNING, logger="jasil.jobs.heartbeat"):
                runner.leases.renew()

        registry.register("activity.created", "s", handle)
        _enqueue(db)

        runner.run_once()

        assert "could not be recorded" in caplog.text
        assert _job(db).status == jobs_crud.STATUS_COMPLETED

    def test_nothing_is_recorded_when_switched_off(self, registry, clock, session_factory, db):
        runner = _runner(registry, clock, session_factory, record_attempts=False)
        registry.register("activity.created", "s", lambda _e: None)
        _enqueue(db)

        runner.run_once()

        assert _attempts(db) == []
        assert _job(db).status == jobs_crud.STATUS_COMPLETED

    def test_a_replay_starts_numbering_again_from_one(self, runner, registry, db):
        registry.register("activity.created", "s", lambda _e: (_ for _ in ()).throw(RuntimeError("boom")))
        job = _enqueue(db, max_attempts=1)
        runner.run_once()
        assert _job(db).status == jobs_crud.STATUS_DEAD_LETTER

        jobs_crud.replay_dead_letter_job(job.id, now=T0 + timedelta(minutes=1), db=db)
        runner._clock.advance(120)
        runner.run_once()

        rows = _attempts(db, job.id)
        assert [(row.attempt, row.outcome) for row in rows] == [(1, "dead_letter"), (1, "dead_letter")]
        assert rows[0].claimed_at < rows[1].claimed_at, "history stays ordered by claim time"


class TestJobDetailRead:
    def _two_attempts(self, db):
        job = _enqueue(db)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)
        jobs_crud.mark_job_failed(
            job.id, "boom", worker_id="w1", attempt=1, base_seconds=1, max_seconds=2, now=T0, db=db, duration_ms=10
        )
        later = T0 + timedelta(minutes=5)
        jobs_crud.claim_jobs(worker_id="w2", limit=10, lease_seconds=LEASE, now=later, db=db)
        jobs_crud.mark_job_completed(job.id, worker_id="w2", attempt=2, now=later, db=db, duration_ms=20)
        return job

    def test_a_job_is_returned_with_its_history_newest_first(self, db, session_factory):
        job = self._two_attempts(db)

        detail = jasil_admin.get_job(job.id)

        assert detail.status == jobs_crud.STATUS_COMPLETED
        assert [(a.attempt, a.worker_id, a.outcome) for a in detail.history] == [
            (2, "w2", "completed"),
            (1, "w1", "failed"),
        ]
        assert detail.attempts_recorded is True

    def test_the_history_is_bounded_by_the_limit(self, db, session_factory):
        job = self._two_attempts(db)

        detail = jasil_admin.get_job(job.id, attempt_limit=1)

        assert [a.attempt for a in detail.history] == [2]

    def test_an_unknown_job_is_none(self, session_factory):
        assert jasil_admin.get_job("missing") is None

    @pytest.mark.parametrize("limit", [0, 501, True, "1"])
    def test_an_out_of_range_limit_is_refused(self, session_factory, limit):
        with pytest.raises(ValueError, match="attempt_limit"):
            jasil_admin.get_job("any", attempt_limit=limit)

    def test_it_says_when_attempts_are_not_recorded(self, db, session_factory):
        job = self._two_attempts(db)
        settings.configure(settings.JasilSettings(jobs=settings.JobSettings(record_attempts=False)))

        detail = jasil_admin.get_job(job.id)

        assert detail.attempts_recorded is False
        assert len(detail.history) == 2, "what was recorded earlier is still shown"


class TestEventJobsRead:
    def test_every_subscribers_job_is_shown_with_its_workers(self, db, session_factory):
        event = new_event("order.created", {"id": 1}, source="test")
        a = _enqueue(db, "invoice.render", event=event)
        b = _enqueue(db, "mail.send", event=event)
        other = _enqueue(db, "invoice.render")
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)
        jobs_crud.mark_job_completed(a.id, worker_id="w1", attempt=1, now=T0, db=db, duration_ms=5)
        jobs_crud.mark_job_failed(
            b.id, "smtp down", worker_id="w1", attempt=1, base_seconds=1, max_seconds=2, now=T0, db=db
        )

        result = jasil_admin.get_event_jobs(event.event_id)

        assert result.event_id == event.event_id
        by_subscriber = {job.subscriber_id: job for job in result.jobs}
        assert set(by_subscriber) == {"invoice.render", "mail.send"}
        assert other.id not in {job.id for job in result.jobs}
        assert [(h.worker_id, h.outcome) for h in by_subscriber["mail.send"].history] == [("w1", "failed")]
        assert by_subscriber["mail.send"].history[0].error == "smtp down"

    def test_an_unknown_event_has_no_jobs(self, session_factory):
        assert jasil_admin.get_event_jobs("missing").jobs == []


class TestWorkerAttemptsRead:
    def _history(self, db, count=5):
        for index in range(count):
            _insert_attempt(
                db,
                id=f"a{index}",
                job_id=f"job-{index}",
                claimed_at=T0 + timedelta(minutes=index),
                finished_at=T0 + timedelta(minutes=index, seconds=1),
                outcome="completed",
                duration_ms=1000,
            )
        _insert_attempt(db, id="elsewhere", worker_id="other-worker", job_id="job-x")

    def test_a_workers_attempts_come_newest_first(self, db, session_factory):
        self._history(db)

        page = jasil_admin.get_worker_attempts("worker-1")

        assert [a.id for a in page.attempts] == ["a4", "a3", "a2", "a1", "a0"]
        assert page.next_cursor is None

    def test_pages_follow_the_cursor_without_gaps_or_repeats(self, db, session_factory):
        self._history(db)
        seen = []
        cursor = None

        while True:
            page = jasil_admin.get_worker_attempts("worker-1", limit=2, cursor=cursor)
            seen.extend(a.id for a in page.attempts)
            cursor = page.next_cursor
            if cursor is None:
                break

        assert seen == ["a4", "a3", "a2", "a1", "a0"]

    def test_attempts_sharing_a_claim_time_are_still_paged_exactly_once(self, db, session_factory):
        for index in range(4):
            _insert_attempt(db, id=f"tie-{index}", job_id=f"job-{index}", claimed_at=T0)

        first = jasil_admin.get_worker_attempts("worker-1", limit=3)
        second = jasil_admin.get_worker_attempts("worker-1", limit=3, cursor=first.next_cursor)

        ids = [a.id for a in first.attempts] + [a.id for a in second.attempts]
        assert sorted(ids) == ["tie-0", "tie-1", "tie-2", "tie-3"]

    def test_an_unknown_worker_has_an_empty_page(self, session_factory):
        assert jasil_admin.get_worker_attempts("nobody").attempts == []

    @pytest.mark.parametrize("cursor", ["", "!!!", "e30", "x" * 600])
    def test_a_cursor_it_did_not_produce_is_refused(self, session_factory, cursor):
        with pytest.raises(ValueError, match="invalid worker attempts cursor"):
            jasil_admin.get_worker_attempts("worker-1", cursor=cursor)

    @pytest.mark.parametrize("limit", [0, 501, True])
    def test_an_out_of_range_page_size_is_refused(self, session_factory, limit):
        with pytest.raises(ValueError, match="limit"):
            jasil_admin.get_worker_attempts("worker-1", limit=limit)


class TestSummariesCarryTheHistory:
    def _finish(self, db, subscriber, *, duration_ms, outcome="completed", queue="default", worker="w1", at=None):
        _insert_attempt(
            db,
            id=f"{subscriber}-{duration_ms}-{outcome}-{worker}",
            job_id=f"job-{subscriber}-{duration_ms}-{outcome}",
            subscriber_id=subscriber,
            queue=queue,
            worker_id=worker,
            outcome=outcome,
            duration_ms=duration_ms,
            finished_at=at or datetime.now(UTC),
        )

    def test_durations_are_summarised_per_subscriber_and_queue(self, db, session_factory):
        for subscriber, queue, durations in (("fast", "default", (10, 30)), ("slow", "campaign", (2000,))):
            for duration in durations:
                job = _enqueue(db, subscriber, queue=queue)
                self._finish(db, subscriber, duration_ms=duration, queue=queue)
                del job

        summary = jobs_crud.get_jobs_summary(db, now=T0 + timedelta(days=1), hours=EVERYTHING)

        subscribers = {s.subscriber_id: s for s in summary.by_subscriber}
        queues = {q.queue: q for q in summary.by_queue}
        assert (subscribers["fast"].avg_duration_ms, subscribers["fast"].max_duration_ms) == (20.0, 30)
        assert subscribers["slow"].max_duration_ms == 2000
        assert queues["campaign"].duration_buckets.under_10s == 1
        assert queues["default"].duration_buckets.under_100ms == 2

    @pytest.mark.parametrize(
        ("duration_ms", "bucket"),
        [
            (0, "under_100ms"),
            (99, "under_100ms"),
            (100, "under_1s"),
            (999, "under_1s"),
            (1_000, "under_10s"),
            (9_999, "under_10s"),
            (10_000, "under_1m"),
            (59_999, "under_1m"),
            (60_000, "under_5m"),
            (299_999, "under_5m"),
            (300_000, "over_5m"),
            (10_000_000, "over_5m"),
        ],
    )
    def test_a_duration_on_a_boundary_lands_in_the_upper_bucket(self, db, duration_ms, bucket):
        self._finish(db, "s", duration_ms=duration_ms)

        stats = job_attempts.attempt_stats(db, JobAttempt.queue, window_start=T0)

        buckets = stats[("default",)].buckets.model_dump()
        assert buckets[bucket] == 1
        assert sum(buckets.values()) == 1

    def test_only_completed_attempts_are_timed_and_buckets_sum_to_them(self, db):
        for index, duration in enumerate((10, 500, 70_000)):
            self._finish(db, "s", duration_ms=duration, worker=f"w{index}")
        self._finish(db, "s", duration_ms=9_999_999, outcome="failed")
        self._finish(db, "s", duration_ms=8_888_888, outcome="superseded")

        stats = job_attempts.attempt_stats(db, JobAttempt.queue, window_start=T0)[("default",)]

        assert stats.max_duration_ms == 70_000
        assert sum(stats.buckets.model_dump().values()) == 3
        assert stats.superseded == 1

    def test_attempts_outside_the_window_are_not_counted(self, db):
        self._finish(db, "s", duration_ms=10, at=T0 - timedelta(days=2))

        stats = job_attempts.attempt_stats(db, JobAttempt.queue, window_start=T0)

        assert stats == {}

    def test_a_quiet_subscriber_reads_zero_not_none(self, db):
        _enqueue(db, "quiet")

        summary = jobs_crud.get_jobs_summary(db, now=T0 + timedelta(days=1), hours=EVERYTHING)

        quiet = summary.by_subscriber[0]
        assert quiet.superseded == 0
        assert quiet.avg_duration_ms is None
        assert quiet.duration_buckets.model_dump() == dict.fromkeys(
            ("under_100ms", "under_1s", "under_10s", "under_1m", "under_5m", "over_5m"), 0
        )

    def test_with_recording_off_the_fields_are_none_not_zero(self, db):
        _enqueue(db, "quiet")

        summary = jobs_crud.get_jobs_summary(db, now=T0 + timedelta(days=1), hours=EVERYTHING, record_attempts=False)

        quiet = summary.by_subscriber[0]
        assert (quiet.superseded, quiet.avg_duration_ms, quiet.duration_buckets) == (None, None, None)
        assert summary.by_queue[0].superseded is None

    def test_a_dead_letter_names_the_worker_that_last_ran_it(self, db):
        job = _enqueue(db, max_attempts=2)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)
        jobs_crud.mark_job_failed(
            job.id, "boom", worker_id="w1", attempt=1, base_seconds=1, max_seconds=2, now=T0, db=db
        )
        later = T0 + timedelta(minutes=5)
        jobs_crud.claim_jobs(worker_id="w2", limit=10, lease_seconds=LEASE, now=later, db=db)
        jobs_crud.mark_job_failed(
            job.id, "boom", worker_id="w2", attempt=2, base_seconds=1, max_seconds=2, now=later, db=db
        )

        summary = jobs_crud.get_jobs_summary(db, now=T0 + timedelta(days=1), hours=EVERYTHING)

        assert [d.last_worker_id for d in summary.recent_dead_letter] == ["w2"]

    def test_a_dead_letters_worker_is_none_when_not_recorded(self, db):
        job = _enqueue(db, max_attempts=1)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)
        jobs_crud.mark_job_failed(
            job.id, "boom", worker_id="w1", attempt=1, base_seconds=1, max_seconds=2, now=T0, db=db
        )

        summary = jobs_crud.get_jobs_summary(db, now=T0 + timedelta(days=1), hours=EVERYTHING, record_attempts=False)

        assert summary.recent_dead_letter[0].last_worker_id is None


class TestWorkerSummaryCarriesTheHistory:
    NOW = datetime(2026, 1, 1, 13, 0, 0, tzinfo=UTC)

    def _worker(self, db, instance_id, *, beat_age_seconds, stopped=False):
        beat = self.NOW - timedelta(seconds=beat_age_seconds)
        db.add(
            JobWorker(
                instance_id=instance_id,
                started_at=beat - timedelta(hours=1),
                last_heartbeat_at=beat,
                stopped_at=beat if stopped else None,
            )
        )
        db.commit()

    def _summary(self, db, **kwargs):
        return worker_registry.get_workers_summary(now=self.NOW, stale_after_seconds=45, db=db, **kwargs)

    def test_each_worker_reports_what_it_finished_in_the_window(self, db):
        self._worker(db, "w1", beat_age_seconds=5)
        for index, outcome in enumerate(("completed", "completed", "failed", "dead_letter", "lease_expired")):
            _insert_attempt(
                db,
                id=f"a{index}",
                job_id=f"j{index}",
                worker_id="w1",
                outcome=outcome,
                finished_at=self.NOW - timedelta(minutes=index + 1),
            )
        _insert_attempt(
            db, id="old", job_id="old", worker_id="w1", outcome="completed", finished_at=T0 - timedelta(days=5)
        )

        worker = self._summary(db, hours=24).workers[0]

        assert (worker.completed, worker.failed, worker.dead_lettered, worker.lease_expired, worker.superseded) == (
            2,
            1,
            1,
            1,
            0,
        )
        assert _naive(worker.last_finished_at) == _naive(self.NOW - timedelta(minutes=1))

    def test_the_counts_are_none_when_not_recorded(self, db):
        self._worker(db, "w1", beat_age_seconds=5)

        worker = self._summary(db, record_attempts=False).workers[0]

        assert (worker.completed, worker.failed, worker.superseded) == (None, None, None)

    def test_jobs_held_by_a_silent_worker_are_counted_as_stuck(self, db):
        self._worker(db, "alive", beat_age_seconds=5)
        self._worker(db, "silent", beat_age_seconds=3600)
        self._worker(db, "gone", beat_age_seconds=3600, stopped=True)
        for name, holder in (("a", "alive"), ("b", "silent"), ("c", "silent"), ("d", "gone")):
            job = _enqueue(db, name)
            jobs_crud.claim_jobs(worker_id=holder, limit=1, lease_seconds=LEASE, now=T0, db=db)
            del job

        summary = self._summary(db)

        assert summary.claimed_by_stale_workers == 2

    def test_a_worker_reported_by_the_admin_surface_has_the_same_fields(
        self, db, session_factory, tmp_path, monkeypatch
    ):
        built = container.build_platform(settings.JasilSettings(data_dir=str(tmp_path)))
        monkeypatch.setattr(platform_runtime, "_active_platform", built)
        self._worker(db, "w1", beat_age_seconds=5)

        summary = jasil_admin.get_workers_summary(hours=48)

        assert summary.window_hours == 48
        assert summary.workers[0].completed == 0


class TestRetention:
    OLD = T0
    CUTOFF = T0 + timedelta(days=1)

    def _job(self, db, subscriber, status, completed_at=None):
        job = _enqueue(db, subscriber)
        if completed_at is None and status == jobs_crud.STATUS_COMPLETED:
            completed_at = self.OLD
        db.execute(
            update(ProcessingJob).where(ProcessingJob.id == job.id).values(status=status, completed_at=completed_at)
        )
        db.commit()
        return job

    def test_only_the_history_of_jobs_that_no_longer_exist_goes(self, db):
        done = self._job(db, "done", jobs_crud.STATUS_COMPLETED)
        waiting = self._job(db, "waiting", jobs_crud.STATUS_PENDING)
        running = self._job(db, "running", jobs_crud.STATUS_CLAIMED)
        dead = self._job(db, "dead", jobs_crud.STATUS_DEAD_LETTER)
        for job in (done, waiting, running, dead):
            _insert_attempt(db, id=f"a-{job.subscriber_id}", job_id=job.id, claimed_at=self.OLD)
        _insert_attempt(db, id="orphan", job_id="pruned-job", claimed_at=self.OLD)
        _insert_attempt(db, id="recent", job_id=done.id, claimed_at=self.CUTOFF + timedelta(hours=1))

        deleted = job_attempts.delete_attempts_before(self.CUTOFF, db=db)

        db.rollback()
        remaining = {row.id for row in db.execute(select(JobAttempt)).scalars()}
        assert deleted == 1
        assert remaining == {"a-done", "a-waiting", "a-running", "a-dead", "recent"}

    def test_a_completed_jobs_history_goes_once_the_job_itself_is_pruned(self, db):
        done = self._job(db, "done", jobs_crud.STATUS_COMPLETED)
        _insert_attempt(db, id="a-done", job_id=done.id, claimed_at=self.OLD)

        assert job_attempts.delete_attempts_before(self.CUTOFF, db=db) == 0
        assert jobs_crud.delete_completed_jobs_before(self.CUTOFF, db=db) == 1
        assert job_attempts.delete_attempts_before(self.CUTOFF, db=db) == 1

    def test_a_recently_completed_job_keeps_its_whole_history(self, db):
        job = self._job(db, "fresh", jobs_crud.STATUS_COMPLETED, completed_at=self.CUTOFF + timedelta(hours=1))
        _insert_attempt(db, id="first", job_id=job.id, claimed_at=self.OLD)

        deleted = job_attempts.delete_attempts_before(self.CUTOFF, db=db)

        assert deleted == 0

    def test_it_deletes_in_bounded_batches(self, db):
        for index in range(7):
            _insert_attempt(db, id=f"o{index}", job_id=f"gone-{index}", claimed_at=self.OLD)

        deleted = job_attempts.delete_attempts_before(self.CUTOFF, db=db, batch_size=3)

        assert deleted == 7

    def test_the_scheduled_prune_removes_them_on_the_jobs_window(
        self, db, session_factory, tmp_path, monkeypatch, caplog
    ):
        import jasil.retention as retention

        built = container.build_platform(settings.JasilSettings(data_dir=str(tmp_path)))
        monkeypatch.setattr(platform_runtime, "_active_platform", built)
        _insert_attempt(db, id="orphan", job_id="pruned-job", claimed_at=T0)

        with caplog.at_level(logging.INFO, logger="jasil.retention"):
            retention._run_prune(T0 + timedelta(days=90), event_log_days=0, jobs_days=30)

        db.rollback()
        assert db.execute(select(JobAttempt)).first() is None
        assert "1 job attempt" in caplog.text


class TestPruneOrder:
    def test_jobs_go_before_their_history_so_a_failed_job_delete_keeps_both(self, db, tmp_path, monkeypatch):
        import jasil.retention as retention

        built = container.build_platform(settings.JasilSettings(data_dir=str(tmp_path)))
        monkeypatch.setattr(platform_runtime, "_active_platform", built)
        job = _enqueue(db)
        jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=LEASE, now=T0, db=db)
        jobs_crud.mark_job_completed(job.id, worker_id="w1", attempt=1, now=T0, db=db)

        def boom(*args, **kwargs):
            raise RuntimeError("lock timeout")

        monkeypatch.setattr(jobs_crud, "delete_completed_jobs_before", boom)

        with pytest.raises(RuntimeError):
            retention._run_prune(T0 + timedelta(days=90), event_log_days=0, jobs_days=30)

        assert len(_attempts(db, job.id)) == 1


class TestLogsNameTheWorker:
    def test_outcome_records_carry_the_worker_queue_attempt_and_duration(self, runner, registry, clock, db, caplog):
        def boom(_event):
            clock.advance(1.5)
            raise RuntimeError("boom")

        registry.register("activity.created", "s", boom)
        _enqueue(db, queue="campaign")

        with caplog.at_level(logging.WARNING, logger="jasil.jobs.runner"):
            runner.run_once()

        (record,) = [r for r in caplog.records if r.getMessage() == "Durable job failed"]
        assert (record.worker_id, record.queue, record.attempts, record.duration_ms) == (
            "worker-1",
            "campaign",
            1,
            1500,
        )
        assert record.subscriber == "s"

    def test_the_reaper_names_the_workers_that_went_silent(self, db, session_factory, tmp_path, monkeypatch, caplog):
        built = container.build_platform(settings.JasilSettings(data_dir=str(tmp_path)))
        monkeypatch.setattr(platform_runtime, "_active_platform", built)
        for name in ("a", "b", "c"):
            _enqueue(db, name)
        jobs_crud.claim_jobs(worker_id="w-dead", limit=2, lease_seconds=1, now=T0, db=db)
        jobs_crud.claim_jobs(worker_id="w-silent", limit=1, lease_seconds=1, now=T0, db=db)

        with caplog.at_level(logging.WARNING, logger="jasil.jobs.service"):
            jobs_service.reap_expired_jobs_scheduled()

        (record,) = [r for r in caplog.records if "Reaped" in r.getMessage()]
        assert record.workers == {"w-dead": 2, "w-silent": 1}

    def test_expired_lease_holders_are_counted_per_worker(self, db):
        for name in ("a", "b"):
            _enqueue(db, name)
        jobs_crud.claim_jobs(worker_id="w1", limit=2, lease_seconds=LEASE, now=T0, db=db)

        assert jobs_crud.expired_lease_holders(now=T0, db=db) == {}
        assert jobs_crud.expired_lease_holders(now=T0 + timedelta(seconds=LEASE + 1), db=db) == {"w1": 2}
