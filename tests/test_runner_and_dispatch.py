"""The job runner, the in-process bus, and the subscriber safety wrapper.

These are the three places a handler exception is caught, and each has a
different contract:

* the **runner** turns a failure into a retry or a dead-letter;
* the **in-process bus** re-raises to the publisher (dispatch is a direct call);
* the **subscriber wrapper** swallows, so one bad subscriber cannot fail a request.
"""

from datetime import UTC, datetime, timedelta

import pytest

import jasil.jobs.crud as jobs_crud
from jasil.backends.events_inprocess import InProcessEventBus
from jasil.events import new_event
from jasil.jobs.models import ProcessingJob
from jasil.jobs.registry import JobHandlerRegistry
from jasil.jobs.runner import JobRunner
from jasil.subscribers import best_effort

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def stored_job(db) -> ProcessingJob:
    """Re-read the job after the runner committed in a session of its own.

    The runner deliberately works in short-lived sessions, so this one has to
    end its read transaction to see those commits: MySQL defaults to REPEATABLE
    READ, where an open transaction keeps the snapshot it started with. Asserting
    on a stale snapshot is how a test passes for the wrong reason.
    """
    db.rollback()
    return db.query(ProcessingJob).one()


class FixedClock:
    def __init__(self, now: datetime = T0) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)


class TestJobRunner:
    @pytest.fixture
    def registry(self):
        return JobHandlerRegistry()

    @pytest.fixture
    def clock(self):
        return FixedClock()

    @pytest.fixture
    def runner(self, registry, clock, session_factory):
        return JobRunner(
            registry=registry,
            clock=clock,
            session_factory=session_factory,
            worker_id="worker-1",
            lease_seconds=60,
            batch_size=10,
            backoff_base_seconds=10,
            backoff_max_seconds=100,
        )

    def _enqueue(self, db, subscriber="s", max_attempts=3):
        event = new_event("activity.created", {"id": 1}, source="test")
        return jobs_crud.enqueue_job(event, subscriber, max_attempts=max_attempts, now=T0, db=db)

    def test_an_empty_queue_processes_nothing(self, runner):
        assert runner.run_once() == 0

    def test_a_due_job_runs_its_handler(self, runner, registry, db):
        seen = []
        registry.register("activity.created", "s", seen.append)
        self._enqueue(db)

        processed = runner.run_once()

        assert processed == 1
        assert len(seen) == 1

    def test_the_handler_receives_the_rebuilt_envelope(self, runner, registry, db):
        seen = []
        registry.register("activity.created", "s", seen.append)
        self._enqueue(db)

        runner.run_once()

        assert seen[0].event_type == "activity.created"
        assert seen[0].payload == {"id": 1}

    def test_a_successful_run_completes_the_job(self, runner, registry, db):
        registry.register("activity.created", "s", lambda _e: None)
        self._enqueue(db)

        runner.run_once()

        assert stored_job(db).status == jobs_crud.STATUS_COMPLETED

    def test_a_failing_handler_reschedules_the_job(self, runner, registry, db):
        def _boom(_event):
            raise RuntimeError("handler failed")

        registry.register("activity.created", "s", _boom)
        self._enqueue(db)

        runner.run_once()

        assert stored_job(db).status == jobs_crud.STATUS_PENDING

    def test_a_failing_handler_dead_letters_once_attempts_are_exhausted(self, runner, registry, db):
        def _boom(_event):
            raise RuntimeError("handler failed")

        registry.register("activity.created", "s", _boom)
        self._enqueue(db, max_attempts=1)

        runner.run_once()

        assert stored_job(db).status == jobs_crud.STATUS_DEAD_LETTER

    def test_a_job_with_no_registered_handler_fails_rather_than_vanishing(self, runner, db):
        """A subscriber removed from the code while jobs are still queued must
        surface, not silently succeed."""
        self._enqueue(db, subscriber="gone", max_attempts=1)

        runner.run_once()

        job = stored_job(db)
        assert job.status == jobs_crud.STATUS_DEAD_LETTER
        assert "no durable handler" in job.last_error

    def test_one_failing_job_does_not_abort_the_batch(self, runner, registry, db):
        """Each job is finalised independently; the rest of the batch must run."""
        seen = []
        registry.register("activity.created", "good", seen.append)
        registry.register("activity.created", "bad", lambda _e: (_ for _ in ()).throw(RuntimeError("boom")))
        event = new_event("activity.created", {"id": 1}, source="test")
        jobs_crud.enqueue_job(event, "good", max_attempts=3, now=T0, db=db)
        jobs_crud.enqueue_job(event, "bad", max_attempts=3, now=T0, db=db)

        processed = runner.run_once()

        assert processed == 2
        assert len(seen) == 1

    def test_the_batch_size_bounds_a_pass(self, registry, clock, session_factory, db):
        registry.register("activity.created", "s", lambda _e: None)
        for index in range(5):
            jobs_crud.enqueue_job(
                new_event("activity.created", {"i": index}, source="test"), "s", max_attempts=3, now=T0, db=db
            )
        runner = JobRunner(
            registry=registry,
            clock=clock,
            session_factory=session_factory,
            worker_id="w",
            lease_seconds=60,
            batch_size=2,
            backoff_base_seconds=1,
            backoff_max_seconds=2,
        )

        assert runner.run_once() == 2

    def test_reaping_requeues_an_expired_lease(self, runner, registry, clock, db):
        registry.register("activity.created", "s", lambda _e: None)
        self._enqueue(db)
        jobs_crud.claim_jobs(worker_id="other", limit=10, lease_seconds=60, now=T0, db=db)
        clock.advance(61)

        assert runner.reap_once() == 1


class TestInProcessBus:
    def test_a_published_event_reaches_its_subscriber(self):
        bus = InProcessEventBus()
        seen = []
        bus.subscribe("activity.created", seen.append)

        bus.publish(new_event("activity.created", {}, source="test"))

        assert len(seen) == 1

    def test_an_event_with_no_subscribers_is_a_no_op(self):
        InProcessEventBus().publish(new_event("nobody.listening", {}, source="test"))

    def test_only_matching_subscribers_are_called(self):
        bus = InProcessEventBus()
        matching, other = [], []
        bus.subscribe("activity.created", matching.append)
        bus.subscribe("activity.deleted", other.append)

        bus.publish(new_event("activity.created", {}, source="test"))

        assert len(matching) == 1
        assert other == []

    def test_every_subscriber_for_a_type_is_called(self):
        bus = InProcessEventBus()
        first, second = [], []
        bus.subscribe("activity.created", first.append)
        bus.subscribe("activity.created", second.append)

        bus.publish(new_event("activity.created", {}, source="test"))

        assert len(first) == len(second) == 1

    def test_a_handler_exception_propagates_to_the_publisher(self):
        """Dispatch is a direct call in the local profile, so the caller sees it;
        the scheduler backfill is the safety net."""
        bus = InProcessEventBus()
        bus.subscribe("activity.created", lambda _e: (_ for _ in ()).throw(RuntimeError("boom")))

        with pytest.raises(RuntimeError, match="boom"):
            bus.publish(new_event("activity.created", {}, source="test"))

    def test_start_and_stop_are_no_ops(self):
        bus = InProcessEventBus()

        bus.start()
        bus.stop()


class TestInProcessBusRecording:
    """The ``local`` profile's observability path: dispatch is inline, so the
    trail goes published -> completed in one call and the intermediate
    ``processing`` state is never observed."""

    class RecordingRecorder:
        def __init__(self) -> None:
            self.published = []
            self.tracked = []
            self.failed = []

        def record_published(self, event):
            self.published.append(event)

        def record_queued(self, event):  # pragma: no cover - the bus never queues
            raise AssertionError("the bus must not record 'queued'")

        def track(self, event, *, worker_id, handler_name, record_processing=True):
            recorder = self

            class _Tracked:
                def __enter__(self):
                    recorder.tracked.append((worker_id, handler_name, record_processing))

                def __exit__(self, exc_type, exc, traceback):
                    if exc is not None:
                        recorder.failed.append(event.event_id)
                    return False

            return _Tracked()

    @pytest.fixture
    def recorder(self):
        return self.RecordingRecorder()

    @pytest.fixture
    def bus(self, recorder):
        return InProcessEventBus(recorder=recorder)

    def test_the_publication_is_recorded(self, bus, recorder):
        event = new_event("activity.created", {}, source="test")

        bus.publish(event)

        assert recorder.published == [event]

    def test_the_processing_row_is_skipped(self, bus, recorder):
        """Inline dispatch never observes it, so writing it is a wasted round-trip."""
        bus.publish(new_event("activity.created", {}, source="test"))

        assert recorder.tracked[0][2] is False

    def test_the_handler_names_are_recorded(self, bus, recorder):
        def render_invoice(_event):
            pass

        def send_receipt(_event):
            pass

        bus.subscribe("activity.created", render_invoice)
        bus.subscribe("activity.created", send_receipt)

        bus.publish(new_event("activity.created", {}, source="test"))

        assert recorder.tracked[0][1] == "render_invoice,send_receipt"

    def test_no_subscribers_means_no_handler_name(self, bus, recorder):
        bus.publish(new_event("activity.created", {}, source="test"))

        assert recorder.tracked[0][1] is None

    def test_the_subscribers_still_run(self, bus):
        seen = []
        bus.subscribe("activity.created", seen.append)

        bus.publish(new_event("activity.created", {}, source="test"))

        assert len(seen) == 1

    def test_a_failure_is_recorded_and_still_propagates(self, bus, recorder):
        event = new_event("activity.created", {}, source="test")
        bus.subscribe("activity.created", lambda _e: (_ for _ in ()).throw(RuntimeError("boom")))

        with pytest.raises(RuntimeError, match="boom"):
            bus.publish(event)

        assert recorder.failed == [event.event_id]


class TestBestEffortSubscriber:
    def test_it_passes_the_event_through(self):
        seen = []

        best_effort(seen.append)(new_event("activity.created", {}, source="test"))

        assert len(seen) == 1

    def test_a_handler_exception_is_swallowed(self):
        """A failing side effect must never fail the request that triggered it."""

        def _boom(_event):
            raise RuntimeError("subscriber exploded")

        best_effort(_boom)(new_event("activity.created", {}, source="test"))

    def test_the_failure_is_logged_with_correlation_context(self, caplog):
        """The whole metadata dict is logged: which keys matter is host-defined,
        so the library must not pick favourites."""

        def _boom(_event):
            raise RuntimeError("subscriber exploded")

        event = new_event(
            "order.created",
            {},
            source="test",
            metadata={"request_id": "req-1", "tenant_id": 42},
        )

        with caplog.at_level("ERROR"):
            best_effort(_boom)(event)

        record = caplog.records[-1]
        assert record.event_type == "order.created"
        assert record.event_metadata == {"request_id": "req-1", "tenant_id": 42}

    def test_the_subscriber_name_is_logged(self, caplog):
        """It is what identifies which subscriber failed."""

        def my_handler(_event):
            raise RuntimeError("boom")

        with caplog.at_level("ERROR"):
            best_effort(my_handler)(new_event("order.created", {}, source="test"))

        assert caplog.records[-1].subscriber == "my_handler"

    def test_it_preserves_the_wrapped_handler_name(self):
        """The name is what identifies the subscriber in logs."""

        def my_handler(_event) -> None:
            pass

        assert best_effort(my_handler).__name__ == "my_handler"
