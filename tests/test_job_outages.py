"""A handler that fails because storage or the database is unreachable is deferred, not failed.

Time comes from an injected clock, so retry instants are exact and nothing sleeps.
The same tests run on whichever database the suite is pointed at.
"""

import logging
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

import jasil.jobs.backoff as jobs_backoff
import jasil.jobs.crud as jobs_crud
from jasil.events import new_event
from jasil.jobs import ContinueJob, DeferJob
from jasil.jobs.dead_letter import deliver_dead_letters
from jasil.jobs.models import JobAttempt, ProcessingJob
from jasil.jobs.registry import JobHandlerRegistry
from jasil.jobs.runner import JobRunner
from jasil.providers import StorageBackendUnavailableError
from jasil.testing import FixedClock

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
WINDOW = 3600


class DomainError(Exception):
    """What a host handler raises around a storage failure."""


@pytest.fixture
def clock():
    return FixedClock(moment=T0)


@pytest.fixture
def registry():
    return JobHandlerRegistry()


def _runner(registry, clock, session_factory, *, outage_defer_max_seconds=WINDOW, max_deferrals=100, **overrides):
    return JobRunner(
        registry=registry,
        clock=clock,
        session_factory=session_factory,
        worker_id="worker-1",
        lease_seconds=60,
        max_runtime_seconds=600,
        heartbeat_interval_seconds=15,
        batch_size=100,
        backoff_base_seconds=10,
        backoff_max_seconds=100,
        max_deferrals=max_deferrals,
        outage_defer_max_seconds=outage_defer_max_seconds,
        **overrides,
    )


@pytest.fixture
def runner(registry, clock, session_factory):
    return _runner(registry, clock, session_factory)


def _enqueue(db, *, max_attempts=3):
    event = new_event("activity.created", {"id": 1}, source="test")
    return jobs_crud.insert_job(event, "s", max_attempts=max_attempts, now=T0, db=db)


def _job(db) -> ProcessingJob:
    db.rollback()  # see the runner's commits under REPEATABLE READ
    return db.query(ProcessingJob).one()


def _outcomes(db):
    db.rollback()
    rows = db.execute(select(JobAttempt).order_by(JobAttempt.claimed_at, JobAttempt.attempt)).scalars().all()
    return [row.outcome for row in rows]


def _wait(job) -> float:
    return (job.available_at.replace(tzinfo=UTC) - T0).total_seconds()


def _storage_down(error=None):
    def handler(_event):
        raise error or StorageBackendUnavailableError("S3 storage backend is unavailable")

    return handler


def _failing_until(clock, recovered_at, error=None):
    """A handler that fails with ``error`` until the clock reaches ``recovered_at``, then succeeds."""
    runs = []

    def handler(_event):
        runs.append(clock.now())
        if clock.now() < recovered_at:
            raise error or StorageBackendUnavailableError("S3 storage backend is unavailable")

    return handler, runs


def _drive(runner, clock, db, *, until, step=1000):
    """Run the worker, advancing the clock to each retry, until ``until`` or a terminal state."""
    while clock.now() < until:
        runner.run_once()
        job = _job(db)
        if job.status in {"completed", "dead_letter"}:
            return job
        clock.advance(max(step, (job.available_at.replace(tzinfo=UTC) - clock.now()).total_seconds()))
    return _job(db)


class TestStorageOutage:
    def test_an_outage_longer_than_the_attempt_budget_ends_completed(
        self, runner, registry, clock, session_factory, db
    ):
        callbacks = []
        handler, runs = _failing_until(clock, T0 + timedelta(seconds=1800))
        registry.register("activity.created", "s", handler, on_dead_letter=lambda event, error: callbacks.append(error))
        _enqueue(db, max_attempts=3)

        job = _drive(runner, clock, db, until=T0 + timedelta(hours=2), step=1)
        deliver_dead_letters(registry=registry, session_factory=session_factory, clock=clock)

        assert job.status == "completed"
        assert len(runs) > 3  # more claims than max_attempts
        assert job.attempts - job.deferrals - job.budget_offset == 1  # only the run that completed counts
        assert job.outage_since is None
        assert callbacks == []
        assert set(_outcomes(db)[:-1]) == {"deferred"}
        assert _outcomes(db)[-1] == "completed"

    def test_an_outage_deferral_does_not_spend_max_deferrals(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _storage_down())
        _enqueue(db)

        _runner(registry, clock, session_factory, max_deferrals=0).run_once()

        job = _job(db)
        assert (job.status, job.deferrals, job.attempts) == ("pending", 1, 1)

    def test_a_wrapped_storage_error_is_recognised(self, runner, registry, db):
        def handler(_event):
            try:
                raise StorageBackendUnavailableError("S3 storage backend is unavailable")
            except StorageBackendUnavailableError as error:
                raise DomainError("could not delete the user's files") from error

        registry.register("activity.created", "s", handler)
        _enqueue(db)

        runner.run_once()

        job = _job(db)
        assert (job.status, job.deferrals) == ("pending", 1)
        assert job.last_error.startswith("storage unavailable: DomainError: could not delete")

    def test_a_permanent_storage_refusal_fails_and_dead_letters(self, runner, registry, clock, session_factory, db):
        callbacks = []
        registry.register(
            "activity.created",
            "s",
            _storage_down(StorageBackendUnavailableError("S3 refused the request", transient=False)),
            on_dead_letter=lambda event, error: callbacks.append(error),
        )
        _enqueue(db, max_attempts=3)

        job = _drive(runner, clock, db, until=T0 + timedelta(days=1))
        deliver_dead_letters(registry=registry, session_factory=session_factory, clock=clock)

        assert job.status == "dead_letter"
        assert (job.attempts, job.deferrals) == (3, 0)
        assert job.outage_since is None
        assert len(callbacks) == 1

    def test_an_ordinary_error_is_still_a_failure(self, runner, registry, db):
        registry.register("activity.created", "s", _storage_down(RuntimeError("boom")))
        _enqueue(db)

        runner.run_once()

        job = _job(db)
        assert (job.status, job.deferrals, job.attempts) == ("pending", 0, 1)
        assert _outcomes(db) == ["failed"]


class TestDatabaseOutage:
    def test_a_handler_that_cannot_reach_the_database_is_deferred(self, runner, registry, db):
        registry.register("activity.created", "s", _storage_down(PoolTimeoutError("QueuePool limit reached")))
        _enqueue(db)

        runner.run_once()

        job = _job(db)
        assert (job.status, job.deferrals) == ("pending", 1)
        assert job.last_error.startswith("database unavailable: TimeoutError")

    def test_a_wrapped_database_outage_is_recognised(self, runner, registry, db):
        def handler(_event):
            raise DomainError("export failed") from PoolTimeoutError("QueuePool limit reached")

        registry.register("activity.created", "s", handler)
        _enqueue(db)

        runner.run_once()

        assert _job(db).deferrals == 1


class TestOutageWindow:
    def test_the_window_starts_at_the_first_outage_deferral_and_is_kept(self, runner, registry, clock, db):
        registry.register("activity.created", "s", _storage_down())
        _enqueue(db)

        runner.run_once()
        assert _naive(_job(db).outage_since) == _naive(T0)

        clock.advance(100)
        runner.run_once()
        assert _naive(_job(db).outage_since) == _naive(T0)

    def test_an_outage_longer_than_the_window_dead_letters_once(self, registry, clock, session_factory, db):
        callbacks = []
        registry.register(
            "activity.created", "s", _storage_down(), on_dead_letter=lambda event, error: callbacks.append(error)
        )
        _enqueue(db, max_attempts=2)
        runner = _runner(registry, clock, session_factory, outage_defer_max_seconds=600)

        job = _drive(runner, clock, db, until=T0 + timedelta(days=1), step=1)
        deliver_dead_letters(registry=registry, session_factory=session_factory, clock=clock)
        deliver_dead_letters(registry=registry, session_factory=session_factory, clock=clock)

        assert job.status == "dead_letter"
        assert job.attempts - job.deferrals == 2  # past the window, each claim spent an attempt
        assert job.deferrals > 0
        assert _outcomes(db)[-2:] == ["failed", "dead_letter"]
        assert len(callbacks) == 1

    def test_past_the_window_every_further_outage_error_is_a_failure(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _storage_down())
        _enqueue(db, max_attempts=5)
        runner = _runner(registry, clock, session_factory, outage_defer_max_seconds=60)

        runner.run_once()  # deferred, the window opens
        clock.advance(61)
        runner.run_once()  # past the window: failed, and the window is not reopened
        clock.advance(1000)
        runner.run_once()

        job = _job(db)
        assert _outcomes(db) == ["deferred", "failed", "failed"]
        assert _naive(job.outage_since) == _naive(T0)

    def test_a_zero_window_turns_outage_deferral_off(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _storage_down())
        _enqueue(db)

        _runner(registry, clock, session_factory, outage_defer_max_seconds=0).run_once()

        job = _job(db)
        assert (job.deferrals, job.outage_since) == (0, None)
        assert _outcomes(db) == ["failed"]

    @pytest.mark.parametrize(
        "outcome",
        [
            pytest.param(lambda clock: DeferJob(clock.now()), id="defer"),
            pytest.param(lambda clock: ContinueJob(), id="continue"),
            pytest.param(lambda clock: RuntimeError("boom"), id="ordinary-failure"),
        ],
    )
    def test_a_claim_that_ends_any_other_way_closes_the_window(self, runner, registry, clock, db, outcome):
        raised = [StorageBackendUnavailableError("down"), outcome(clock)]

        def handler(_event):
            raise raised.pop(0)

        registry.register("activity.created", "s", handler)
        _enqueue(db)

        runner.run_once()
        assert _job(db).outage_since is not None
        clock.advance(1000)
        runner.run_once()

        assert _job(db).outage_since is None

    def test_a_replay_closes_the_window(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _storage_down())
        job_id = _enqueue(db, max_attempts=1)
        runner = _runner(registry, clock, session_factory, outage_defer_max_seconds=60)
        runner.run_once()
        clock.advance(61)
        runner.run_once()
        assert _job(db).status == "dead_letter"

        assert jobs_crud.replay_dead_letter_job(job_id, now=clock.now(), db=db)

        assert _job(db).outage_since is None


class TestOutageDelay:
    def test_the_first_retry_is_thirty_seconds_away(self, runner, registry, db):
        registry.register("activity.created", "s", _storage_down())
        _enqueue(db)

        runner.run_once()

        assert 30 <= _wait(_job(db)) <= 33

    def test_the_delay_grows_with_the_outage(self, runner, registry, clock, db):
        registry.register("activity.created", "s", _storage_down())
        _enqueue(db)
        runner.run_once()

        clock.advance(400)
        runner.run_once()

        job = _job(db)
        wait = (job.available_at.replace(tzinfo=UTC) - clock.now()).total_seconds()
        assert 400 <= wait <= 440

    def test_the_delay_is_clamped_to_defer_max_seconds(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", _storage_down())
        _enqueue(db)

        _runner(registry, clock, session_factory, defer_max_seconds=10).run_once()

        assert _wait(_job(db)) <= 10

    @pytest.mark.parametrize(
        ("elapsed", "expected"), [(0, 30), (-5, 30), (30, 30), (120, 120), (900, 900), (86400, 900)]
    )
    def test_outage_seconds_is_the_elapsed_outage_between_the_floor_and_the_cap(self, elapsed, expected):
        assert jobs_backoff.outage_seconds(elapsed) == expected


class TestOutageLogging:
    def test_one_outage_across_many_jobs_logs_one_warning_without_a_traceback(
        self, registry, clock, session_factory, db, caplog
    ):
        registry.register("activity.created", "s", _storage_down())
        for _ in range(50):
            _enqueue(db)
        runner = _runner(registry, clock, session_factory)

        with caplog.at_level(logging.DEBUG, logger="jasil.jobs.runner"):
            runner.run_once()
            clock.advance(100)
            runner.run_once()

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert warnings[0].exc_info is None
        assert warnings[0].error == "StorageBackendUnavailableError"
        assert "storage" in warnings[0].getMessage()
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
        deferred = [r for r in caplog.records if r.getMessage() == "Durable job deferred"]
        assert len(deferred) == 100
        assert {r.levelno for r in deferred} == {logging.INFO}

    def test_a_job_completing_after_an_outage_ends_it_and_the_next_one_warns_again(
        self, runner, registry, clock, db, caplog
    ):
        storage = {"down": True}

        def handler(_event):
            if storage["down"]:
                raise StorageBackendUnavailableError("S3 storage backend is unavailable")

        registry.register("activity.created", "s", handler)
        _enqueue(db)

        with caplog.at_level(logging.INFO, logger="jasil.jobs.runner"):
            runner.run_once()  # the outage begins: one warning
            storage["down"] = False
            clock.advance(60)
            runner.run_once()  # the deferred job completes: the outage is over
            storage["down"] = True
            _enqueue(db)
            runner.run_once()  # a new outage: a new warning

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        recovered = [r for r in caplog.records if r.levelno == logging.INFO and "again" in r.getMessage()]
        assert len(warnings) == 2
        assert len(recovered) == 1


def _naive(moment):
    return moment.replace(tzinfo=None) if moment is not None else None
