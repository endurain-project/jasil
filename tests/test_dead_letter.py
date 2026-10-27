"""The dead-letter hook: attempt counts on the event, and ``on_dead_letter`` delivery.

The callback is delivered at least once. A job is owed one while it is
``dead_letter`` with ``dead_letter_notified_at`` NULL; the process that
dead-lettered it delivers at once, and a sweep delivers what a crash left behind.
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

import jasil.container as container
import jasil.jobs.crud as jobs_crud
import jasil.jobs.registry as jobs_registry
import jasil.jobs.service as jobs_service
import jasil.runtime as platform_runtime
import jasil.settings as settings
from jasil.events import Event, new_event
from jasil.jobs.dead_letter import (
    CLAIM_LEASE_SECONDS,
    MAX_DELIVERIES,
    RETRY_BASE_SECONDS,
    RETRY_MAX_SECONDS,
    SWEEP_WINDOW,
    deliver_dead_letters,
)
from jasil.jobs.models import ProcessingJob
from jasil.jobs.registry import JobHandlerRegistry
from jasil.jobs.runner import JobRunner

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
LEASE = 60


class FixedClock:
    def __init__(self, now: datetime = T0) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now

    def monotonic(self) -> float:
        return self._now.timestamp()

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


def _boom(_event):
    raise RuntimeError("handler failed")


def stored_job(db) -> ProcessingJob:
    db.rollback()
    return db.query(ProcessingJob).one()


@pytest.fixture
def registry():
    return JobHandlerRegistry()


@pytest.fixture
def clock():
    return FixedClock()


@pytest.fixture
def runner(registry, clock, session_factory):
    return JobRunner(
        registry=registry,
        clock=clock,
        session_factory=session_factory,
        worker_id="worker-1",
        lease_seconds=LEASE,
        max_runtime_seconds=600,
        heartbeat_interval_seconds=15,
        batch_size=10,
        backoff_base_seconds=10,
        backoff_max_seconds=100,
    )


@pytest.fixture
def calls(registry):
    """Every ``(event, error)`` the callback received."""
    received = []
    registry.register("a.b", "s", _boom, on_dead_letter=lambda event, error: received.append((event, error)))
    return received


def enqueue(db, max_attempts=2):
    event = new_event("a.b", {"id": 1}, source="test")
    return jobs_crud.enqueue_job(event, "s", max_attempts=max_attempts, now=T0, db=db)


def fail_until_dead(runner, clock, db, attempts):
    for _ in range(attempts):
        runner.run_once()
        clock.advance(1000)  # past any backoff
    return stored_job(db)


class TestAttemptCounts:
    def test_a_durable_handler_sees_a_one_based_attempt_and_the_ceiling(self, runner, registry, clock, db):
        seen = []
        registry.register("a.b", "s", lambda event: (seen.append(event), _boom(event)))
        enqueue(db, max_attempts=3)

        fail_until_dead(runner, clock, db, 3)

        assert [(e.attempt, e.max_attempts, e.is_last_attempt) for e in seen] == [
            (1, 3, False),
            (2, 3, False),
            (3, 3, True),
        ]

    def test_the_last_attempt_is_the_one_that_dead_letters(self, runner, registry, clock, db):
        seen = []
        registry.register("a.b", "s", lambda event: (seen.append(event.is_last_attempt), _boom(event)))
        enqueue(db, max_attempts=2)

        job = fail_until_dead(runner, clock, db, 2)

        assert seen == [False, True]
        assert job.status == jobs_crud.STATUS_DEAD_LETTER

    def test_the_ceiling_is_the_jobs_own_not_the_current_setting(self, runner, registry, db):
        seen = []
        registry.register("a.b", "s", seen.append)
        enqueue(db, max_attempts=1)

        runner.run_once()

        assert seen[0].max_attempts == 1
        assert seen[0].is_last_attempt

    def test_an_event_outside_a_durable_handler_has_no_attempt(self):
        event = new_event("a.b", {}, source="test")

        assert (event.attempt, event.max_attempts, event.is_last_attempt) == (None, None, False)

    def test_a_replay_restarts_the_count(self, runner, registry, clock, db):
        seen = []
        registry.register("a.b", "s", lambda event: (seen.append(event.attempt), _boom(event)))
        job = enqueue(db, max_attempts=1)
        runner.run_once()
        jobs_crud.replay_dead_letter_job(job.id, now=clock.now(), db=db)

        runner.run_once()

        assert seen == [1, 1]


class TestRegistration:
    def test_a_subscriber_without_a_callback_has_none(self, registry):
        registry.register("a.b", "s", _boom)

        assert registry.dead_letter_callback("s") is None
        assert registry.dead_letter_subscriber_ids() == frozenset()

    def test_a_callback_is_registered_with_its_subscriber(self, registry):
        def callback(_event, _error):
            return None

        registry.register("a.b", "s", _boom, on_dead_letter=callback)

        assert registry.dead_letter_callback("s") is callback
        assert registry.dead_letter_subscriber_ids() == {"s"}

    def test_registering_again_without_a_callback_removes_it(self, registry):
        registry.register("a.b", "s", _boom, on_dead_letter=lambda _e, _r: None)
        registry.register("a.b", "s", _boom)

        assert registry.dead_letter_callback("s") is None

    def test_clear_removes_callbacks(self, registry):
        registry.register("a.b", "s", _boom, on_dead_letter=lambda _e, _r: None)

        registry.clear()

        assert registry.dead_letter_subscriber_ids() == frozenset()


class TestHandlerException:
    def test_the_callback_runs_once_when_the_last_attempt_raises(self, runner, clock, db, calls):
        enqueue(db, max_attempts=2)

        job = fail_until_dead(runner, clock, db, 2)

        assert len(calls) == 1
        event, error = calls[0]
        assert isinstance(event, Event)
        assert (event.event_type, event.payload, event.attempt, event.is_last_attempt) == ("a.b", {"id": 1}, 2, True)
        assert error == "handler failed"
        assert job.dead_letter_notified_at is not None

    def test_it_does_not_run_while_attempts_remain(self, runner, db, calls):
        enqueue(db, max_attempts=3)

        runner.run_once()

        assert calls == []
        assert stored_job(db).dead_letter_notified_at is None

    def test_a_later_sweep_does_not_repeat_a_delivered_callback(self, runner, clock, db, calls):
        enqueue(db, max_attempts=1)
        runner.run_once()
        clock.advance(CLAIM_LEASE_SECONDS * 2)

        runner.reap_once()

        assert len(calls) == 1

    def test_the_callback_runs_after_the_transition_is_committed(self, runner, registry, db):
        seen = []

        def callback(event, _error):
            db.rollback()
            seen.append(db.query(ProcessingJob).one().status)

        registry.register("a.b", "s", _boom, on_dead_letter=callback)
        enqueue(db, max_attempts=1)

        runner.run_once()

        assert seen == [jobs_crud.STATUS_DEAD_LETTER]


class TestLeaseExpiry:
    def test_a_lease_that_expires_on_the_last_attempt_calls_back_from_the_reaper(self, runner, clock, db, calls):
        enqueue(db, max_attempts=1)
        jobs_crud.claim_jobs(worker_id="dead-worker", limit=10, lease_seconds=LEASE, now=T0, db=db)
        clock.advance(LEASE + 1)

        assert runner.reap_once() == 1

        job = stored_job(db)
        assert job.status == jobs_crud.STATUS_DEAD_LETTER
        assert job.dead_letter_notified_at is not None
        [(event, error)] = calls
        assert (event.attempt, event.max_attempts, event.is_last_attempt) == (1, 1, True)
        assert error == "lease expired; max attempts exhausted"

    def test_a_lease_that_expires_with_attempts_left_does_not_call_back(self, runner, clock, db, calls):
        enqueue(db, max_attempts=2)
        jobs_crud.claim_jobs(worker_id="dead-worker", limit=10, lease_seconds=LEASE, now=T0, db=db)
        clock.advance(LEASE + 1)

        runner.reap_once()

        assert calls == []
        assert stored_job(db).status == jobs_crud.STATUS_PENDING

    def test_the_scheduled_delivery_calls_back_for_the_process_wide_registry(
        self, tmp_path, session_factory, db, monkeypatch
    ):
        monkeypatch.setattr(
            platform_runtime,
            "_active_platform",
            container.build_platform(settings.JasilSettings(data_dir=str(tmp_path))),
        )
        received = []
        jobs_registry.registry.register("a.b", "s", _boom, on_dead_letter=lambda e, r: received.append((e, r)))
        try:
            # MySQL's DATETIME keeps whole seconds and rounds on write, so a
            # microsecond-precision ``now`` would leave ``available_at`` up to a
            # second in the future and the claim below would select nothing.
            now = platform_runtime.get_active_platform().clock.now().replace(microsecond=0)
            jobs_crud.enqueue_job(new_event("a.b", {}, source="t"), "s", max_attempts=1, now=now, db=db)
            jobs_crud.claim_jobs(worker_id="w1", limit=10, lease_seconds=-1, now=now, db=db)

            jobs_service.reap_expired_jobs_scheduled()
            assert received == []  # the reaper itself never runs a callback
            stored_job(db)
            db.rollback()  # end the snapshot a REPEATABLE READ server would otherwise keep

            jobs_service.deliver_dead_letters_scheduled()
        finally:
            jobs_registry.registry.clear()

        assert [r for _e, r in received] == ["lease expired; max attempts exhausted"]


class TestReaperRecovery:
    """A crash after the dead-letter commit but before the callback leaves it owed."""

    def _crash_after_commit(self, db, clock, runner):
        enqueue(db, max_attempts=1)
        jobs_crud.claim_jobs(worker_id="w", limit=10, lease_seconds=LEASE, now=T0, db=db)
        jobs_crud.mark_job_failed(
            stored_job(db).id,
            "boom",
            worker_id="w",
            attempt=1,
            base_seconds=1,
            max_seconds=1,
            now=clock.now(),
            db=db,
        )

    def test_the_sweep_delivers_a_callback_the_crash_left_owed(self, runner, clock, db, calls):
        self._crash_after_commit(db, clock, runner)
        assert calls == []
        clock.advance(1)

        runner.reap_once()

        assert [error for _e, error in calls] == ["boom"]
        assert stored_job(db).dead_letter_notified_at is not None

    def test_a_delivery_in_flight_elsewhere_is_not_started_again(self, runner, registry, clock, db, calls):
        """The claim lease keeps a slow callback from running twice at once."""
        self._crash_after_commit(db, clock, runner)
        with runner._session_factory() as other:
            claimed = jobs_crud.claim_dead_letter_callbacks(
                registry.dead_letter_subscriber_ids(),
                now=clock.now(),
                lease_seconds=CLAIM_LEASE_SECONDS,
                db=other,
            )
        assert len(claimed) == 1

        runner.reap_once()
        assert calls == []

        clock.advance(CLAIM_LEASE_SECONDS + 1)
        runner.reap_once()
        assert len(calls) == 1

    def test_only_one_of_two_claimers_wins(self, runner, registry, clock, db, calls):
        self._crash_after_commit(db, clock, runner)
        subscribers = registry.dead_letter_subscriber_ids()

        def claim():
            with runner._session_factory() as session:
                return jobs_crud.claim_dead_letter_callbacks(
                    subscribers, now=clock.now(), lease_seconds=CLAIM_LEASE_SECONDS, db=session
                )

        assert [len(claim()), len(claim())] == [1, 0]

    def test_a_crash_after_the_callback_repeats_it(self, runner, registry, clock, db, calls, session_factory):
        """At least once: the marker is written after the callback returns."""
        self._crash_after_commit(db, clock, runner)
        clock.advance(1)
        job_id = stored_job(db).id
        # The first delivery's callback ran but the marker write never happened.
        calls.append("ran before the crash")

        deliver_dead_letters(registry=registry, session_factory=session_factory, clock=clock)

        assert len(calls) == 2
        assert stored_job(db).id == job_id

    def test_a_process_without_the_callback_leaves_it_owed(self, runner, registry, clock, db):
        registry.register("a.b", "s", _boom)
        self._crash_after_commit(db, clock, runner)
        clock.advance(1)

        runner.reap_once()

        assert stored_job(db).dead_letter_notified_at is None
        received = []
        other = JobHandlerRegistry()
        other.register("a.b", "s", _boom, on_dead_letter=lambda e, r: received.append(r))
        deliver_dead_letters(registry=other, session_factory=runner._session_factory, clock=clock)
        assert received == ["boom"]
        assert stored_job(db).dead_letter_notified_at is not None


class TestCallbackRaising:
    @pytest.fixture
    def failing(self, registry):
        attempts = []

        def callback(_event, _error):
            attempts.append(1)
            raise RuntimeError("app is down")

        registry.register("a.b", "s", _boom, on_dead_letter=callback)
        return attempts

    def test_the_exception_is_logged_and_swallowed_and_the_job_stays_dead(self, runner, db, failing, caplog):
        enqueue(db, max_attempts=1)

        with caplog.at_level("ERROR"):
            assert runner.run_once() == 1

        job = stored_job(db)
        assert job.status == jobs_crud.STATUS_DEAD_LETTER
        assert job.dead_letter_notified_at is None
        assert "Dead-letter callback failed" in caplog.text

    def test_it_is_retried_after_a_growing_backoff_not_on_every_pass(self, runner, clock, db, failing):
        enqueue(db, max_attempts=1)
        runner.run_once()
        assert len(failing) == 1

        clock.advance(RETRY_BASE_SECONDS - 1)
        runner.reap_once()
        assert len(failing) == 1

        clock.advance(2)
        runner.reap_once()
        assert len(failing) == 2

        clock.advance(RETRY_BASE_SECONDS + 1)  # the second wait is twice as long
        runner.reap_once()
        assert len(failing) == 2

        clock.advance(RETRY_BASE_SECONDS)
        runner.reap_once()
        assert len(failing) == 3

    def test_the_failure_is_stored_and_the_dead_letter_time_is_kept(self, runner, clock, db, failing):
        enqueue(db, max_attempts=1)
        runner.run_once()
        dead_at = stored_job(db).updated_at
        clock.advance(RETRY_BASE_SECONDS + 1)

        runner.reap_once()

        job = stored_job(db)
        assert job.updated_at == dead_at
        assert job.dead_letter_attempts == 2
        assert job.dead_letter_callback_error == "RuntimeError: app is down"

    def test_the_failure_is_logged_before_the_callback_runs(self, runner, registry, db, caplog):
        order = []
        registry.register("a.b", "s", _boom, on_dead_letter=lambda _e, _r: order.append("callback"))
        enqueue(db, max_attempts=1)

        class Spy(__import__("logging").Handler):
            def emit(self, record):
                order.append(record.getMessage())

        spy = Spy()
        __import__("logging").getLogger("jasil.jobs.runner").addHandler(spy)
        try:
            runner.run_once()
        finally:
            __import__("logging").getLogger("jasil.jobs.runner").removeHandler(spy)

        assert order.index("Durable job failed") < order.index("callback")

    def test_one_failing_callback_does_not_stop_the_next(self, runner, registry, clock, db, session_factory):
        done = []
        registry.register("a.b", "bad", _boom, on_dead_letter=lambda _e, _r: 1 / 0)
        registry.register("a.b", "good", _boom, on_dead_letter=lambda e, r: done.append(e.event_id))
        event = new_event("a.b", {}, source="t")
        jobs_crud.enqueue_job(event, "bad", max_attempts=1, now=T0, db=db)
        jobs_crud.enqueue_job(event, "good", max_attempts=1, now=T0, db=db)

        runner.run_once()

        assert done == [event.event_id]

    def test_a_failure_recording_the_delivery_is_logged_and_repeats_it(
        self, registry, clock, db, session_factory, caplog
    ):
        received = []
        registry.register("a.b", "s", _boom, on_dead_letter=lambda e, r: received.append(r))
        enqueue(db, max_attempts=1)
        db.execute(text("UPDATE jasil_processing_jobs SET status = 'dead_letter', last_error = 'x'"))
        db.commit()

        def broken():
            raise RuntimeError("database down")

        calls = iter([session_factory, broken, session_factory])

        def factory():
            return next(calls)()

        with caplog.at_level("ERROR"):
            assert deliver_dead_letters(registry=registry, session_factory=factory, clock=clock) == 1

        assert "could not be recorded" in caplog.text
        assert stored_job(db).dead_letter_notified_at is None


class TestReplay:
    def test_a_replay_clears_the_marker_and_the_callback_runs_again_on_the_next_dead_letter(
        self, runner, clock, db, calls
    ):
        job = enqueue(db, max_attempts=1)
        runner.run_once()
        assert len(calls) == 1

        assert jobs_crud.replay_dead_letter_job(job.id, now=clock.now(), db=db)
        replayed = stored_job(db)
        assert replayed.dead_letter_notified_at is None
        assert replayed.status == jobs_crud.STATUS_PENDING
        clock.advance(CLAIM_LEASE_SECONDS * 2)
        runner.reap_once()
        assert len(calls) == 1  # replaying does not call it

        runner.run_once()
        assert len(calls) == 2
        assert stored_job(db).dead_letter_notified_at is not None


class TestNoCallback:
    def test_a_subscriber_without_one_dead_letters_exactly_as_before(self, runner, registry, clock, db):
        registry.register("a.b", "s", _boom)
        enqueue(db, max_attempts=1)

        assert runner.run_once() == 1
        clock.advance(CLAIM_LEASE_SECONDS * 2)
        runner.reap_once()

        job = stored_job(db)
        assert job.status == jobs_crud.STATUS_DEAD_LETTER
        assert job.last_error == "handler failed"
        assert job.dead_letter_notified_at is None

    def test_no_work_is_done_when_no_subscriber_has_a_callback(self, registry, clock, session_factory):
        def never():
            raise AssertionError("no session should be opened")

        assert deliver_dead_letters(registry=registry, session_factory=never, clock=clock) == 0
        assert deliver_dead_letters(registry=registry, session_factory=never, clock=clock, job_ids=[]) == 0


class TestDeliveryGuards:
    def _dead(self, db):
        enqueue(db, max_attempts=1)
        db.execute(
            text("UPDATE jasil_processing_jobs SET status = 'dead_letter', last_error = 'x', updated_at = :t"),
            {"t": T0},
        )
        db.commit()

    def test_a_stale_claim_cannot_overwrite_a_newer_delivery(self, registry, clock, db, session_factory):
        self._dead(db)
        registry.register("a.b", "s", _boom, on_dead_letter=lambda e, r: None)
        first = jobs_crud.claim_dead_letter_callbacks(["s"], now=T0, lease_seconds=10, db=db)[0]
        second = jobs_crud.claim_dead_letter_callbacks(["s"], now=T0 + timedelta(seconds=11), lease_seconds=10, db=db)[
            0
        ]

        assert not jobs_crud.mark_dead_letter_notified(first.id, delivery_attempt=first.delivery_attempt, now=T0, db=db)
        assert stored_job(db).dead_letter_notified_at is None
        assert jobs_crud.mark_dead_letter_notified(second.id, delivery_attempt=second.delivery_attempt, now=T0, db=db)

    def test_a_callback_that_always_fails_is_given_up_on(self, registry, clock, db, session_factory):
        self._dead(db)

        def failing(_event, _error):
            raise RuntimeError("nope")

        registry.register("a.b", "s", _boom, on_dead_letter=failing)
        for _ in range(MAX_DELIVERIES + 3):
            clock.advance(RETRY_MAX_SECONDS + 1)
            deliver_dead_letters(registry=registry, session_factory=session_factory, clock=clock, job_ids=None)

        job = stored_job(db)
        assert job.dead_letter_attempts == MAX_DELIVERIES
        assert job.dead_letter_notified_at is None

    def test_the_sweep_ignores_old_history_but_a_targeted_pass_does_not(self, registry, clock, db, session_factory):
        received = []
        registry.register("a.b", "s", _boom, on_dead_letter=lambda e, r: received.append(r))
        self._dead(db)
        clock.advance(SWEEP_WINDOW.total_seconds() + 1)
        job_id = stored_job(db).id

        assert deliver_dead_letters(registry=registry, session_factory=session_factory, clock=clock) == 0
        assert (
            deliver_dead_letters(registry=registry, session_factory=session_factory, clock=clock, job_ids=[job_id]) == 1
        )

    def test_a_job_is_not_leased_until_its_own_callback_starts(self, registry, clock, db, session_factory):
        for _ in range(2):
            enqueue(db, max_attempts=1)
        db.execute(text("UPDATE jasil_processing_jobs SET status = 'dead_letter', updated_at = :t"), {"t": T0})
        db.commit()
        leased_during_callback = []

        def callback(_event, _error):
            db.rollback()
            leased_during_callback.append(
                db.query(ProcessingJob).filter(ProcessingJob.dead_letter_notify_after.is_not(None)).count()
            )

        registry.register("a.b", "s", _boom, on_dead_letter=callback)

        assert deliver_dead_letters(registry=registry, session_factory=session_factory, clock=clock) == 2
        assert leased_during_callback == [1, 1]
