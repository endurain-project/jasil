"""The durable-job worker loop and its background thread.

The loop is what turns claimed rows into executed subscribers, so its two
scheduling rules matter operationally: it must drain a backlog without pausing
between productive batches, and it must survive an iteration that raises rather
than dying silently and leaving the queue to stall.

The runner is scripted rather than real — this file is about the loop, not about
the claim logic, which :mod:`tests.test_jobs` covers against a database. Nothing
sleeps: the poll interval is either zero or absurdly large, so a test that waits
when it should not would hang instead of passing slowly.
"""

import threading
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import jasil.jobs._worker_registry as worker_registry
import jasil.jobs.crud as jobs_crud
from jasil._core.threads import signal_and_join
from jasil.events import new_event
from jasil.jobs._worker_metadata import MAX_WORKER_METADATA_BYTES
from jasil.jobs.models import JobWorker
from jasil.jobs.registry import JobHandlerRegistry
from jasil.jobs.runner import JobRunner
from jasil.jobs.worker import BackgroundWorker, WorkerTelemetry, run_worker

# Long enough that an unwanted poll would hang the test rather than pass slowly.
NEVER = 3600.0
WAIT_TIMEOUT = 5.0
T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


class RecordingEvent(threading.Event):
    """A stop event that remembers the timeouts it was asked to wait for."""

    def __init__(self) -> None:
        super().__init__()
        self.waits: list[float | None] = []

    def wait(self, timeout: float | None = None) -> bool:
        self.waits.append(timeout)
        return super().wait(timeout)


class ScriptedRunner:
    """A ``JobRunner`` stand-in that plays a script, then ends the loop.

    Stopping from inside the last iteration is what keeps the loop bounded
    without a timer: the worker only checks its event between batches.
    """

    def __init__(self, script, stop: threading.Event) -> None:
        self._script = list(script)
        self._stop = stop
        self.calls = 0

    def run_once(self) -> int:
        self.calls += 1
        if not self._script:
            self._stop.set()
            return 0
        outcome = self._script.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class TestRunWorker:
    def test_an_already_stopped_worker_never_claims(self):
        stop = threading.Event()
        stop.set()
        runner = ScriptedRunner([1], stop)

        run_worker(runner, poll_interval_seconds=NEVER, stop=stop)

        assert runner.calls == 0

    def test_it_drains_a_backlog_without_pausing(self):
        """A productive batch loops straight into the next one.

        With a poll interval of an hour, a worker that waited between batches
        would hang here rather than fail.
        """
        stop = RecordingEvent()
        runner = ScriptedRunner([3, 2], stop)

        run_worker(runner, poll_interval_seconds=NEVER, stop=stop)

        assert runner.calls == 3
        assert stop.waits == [NEVER]

    def test_it_waits_when_the_queue_is_empty(self):
        stop = RecordingEvent()
        runner = ScriptedRunner([], stop)

        run_worker(runner, poll_interval_seconds=NEVER, stop=stop)

        assert stop.waits == [NEVER]

    def test_a_failing_iteration_does_not_end_the_loop(self):
        """One bad batch must not stall the queue until the process restarts."""
        stop = threading.Event()
        runner = ScriptedRunner([RuntimeError("connection reset"), 1], stop)

        run_worker(runner, poll_interval_seconds=0, stop=stop)

        assert runner.calls == 3

    def test_a_failing_iteration_is_logged(self, caplog):
        stop = threading.Event()
        runner = ScriptedRunner([RuntimeError("connection reset"), 1], stop)

        with caplog.at_level("ERROR"):
            run_worker(runner, poll_interval_seconds=0, stop=stop)

        assert "Durable job worker iteration failed" in caplog.text

    def test_a_failed_iteration_counts_as_empty(self, caplog):
        """It must back off rather than spin on a database that is still down."""
        stop = RecordingEvent()
        runner = ScriptedRunner([RuntimeError("connection reset")], stop)

        run_worker(runner, poll_interval_seconds=0, stop=stop)

        assert stop.waits == [0, 0]


class SignallingRunner:
    """Reports that it ran, on whichever thread the worker put it on."""

    def __init__(self) -> None:
        self.ran = threading.Event()
        self.thread_name: str | None = None

    def run_once(self) -> int:
        self.thread_name = threading.current_thread().name
        self.ran.set()
        return 0


class BlockingRunner:
    """Hold one handler-like iteration until the test explicitly releases it."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def run_once(self) -> int:
        self.started.set()
        assert self.release.wait(timeout=WAIT_TIMEOUT)
        return 1


class TestBackgroundWorker:
    @pytest.fixture
    def worker(self):
        worker = BackgroundWorker(SignallingRunner(), poll_interval_seconds=0.01)
        yield worker
        worker.stop()

    def test_it_runs_the_loop_off_the_calling_thread(self, worker):
        """A single-process deployment processes jobs without a second container."""
        worker.start()

        assert worker._runner.ran.wait(timeout=WAIT_TIMEOUT)
        assert worker._runner.thread_name != threading.current_thread().name

    def test_the_thread_is_a_daemon(self, worker):
        """It must never hold up interpreter shutdown."""
        worker.start()

        assert worker._thread.daemon is True

    def test_starting_twice_leaves_one_thread(self, worker):
        worker.start()
        thread = worker._thread

        worker.start()

        assert worker._thread is thread

    def test_stopping_releases_the_thread(self, worker):
        worker.start()
        thread = worker._thread

        worker.stop()

        assert worker._thread is None
        assert not thread.is_alive()

    def test_stopping_a_worker_that_never_started_is_a_no_op(self, worker):
        worker.stop()

        assert worker._thread is None

    def test_it_can_be_restarted(self, worker):
        worker.start()
        worker.stop()

        worker.start()

        assert worker._thread is not None
        assert worker._runner.ran.wait(timeout=WAIT_TIMEOUT)

    def test_a_timed_out_thread_is_retained_and_cannot_be_overlapped(self):
        runner = BlockingRunner()
        worker = BackgroundWorker(runner, poll_interval_seconds=0)
        worker.start()
        assert runner.started.wait(timeout=WAIT_TIMEOUT)
        original = worker._thread

        assert worker.stop(timeout=0.01) is False
        assert worker._thread is original
        worker.start()
        assert worker._thread is original

        runner.release.set()
        original.join(timeout=WAIT_TIMEOUT)
        assert not original.is_alive()
        worker.start()
        assert worker._thread is not original
        worker.stop()


class StepClock:
    """Thread-safe clock that advances one second on every observation."""

    def __init__(self) -> None:
        self._moment = T0
        self._lock = threading.Lock()

    def now(self) -> datetime:
        with self._lock:
            moment = self._moment
            self._moment += timedelta(seconds=1)
            return moment


class TestWorkerTelemetry:
    @pytest.fixture
    def worker_database(self, tmp_path, mapped_base):
        engine = create_engine(f"sqlite:///{tmp_path / 'worker.db'}")
        mapped_base.metadata.create_all(engine)
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        yield factory
        mapped_base.metadata.drop_all(engine)
        engine.dispose()

    def test_heartbeat_advances_during_a_long_handler_and_stop_is_recorded(
        self,
        worker_database,
        monkeypatch,
    ):
        handler_started = threading.Event()
        release_handler = threading.Event()
        heartbeat_written = threading.Event()
        stop = threading.Event()
        clock = StepClock()
        registry = JobHandlerRegistry()

        def handle(_event):
            handler_started.set()
            assert release_handler.wait(timeout=WAIT_TIMEOUT)

        registry.register("work.created", "handler", handle, queue="campaign")
        with worker_database() as db:
            jobs_crud.enqueue_job(
                new_event("work.created", {}, source="test"),
                "handler",
                queue="campaign",
                max_attempts=3,
                now=T0,
                db=db,
            )
        runner = JobRunner(
            registry=registry,
            clock=clock,
            session_factory=worker_database,
            worker_id="00000000-0000-4000-8000-000000000001",
            lease_seconds=60,
            batch_size=1,
            backoff_base_seconds=1,
            backoff_max_seconds=2,
            queues=("campaign",),
        )
        telemetry = WorkerTelemetry(
            instance_id=runner._worker_id,
            clock=clock,
            session_factory=worker_database,
            heartbeat_interval_seconds=0.01,
            queues=runner._queues,
            role="processor",
            label="campaign-1",
            metadata={"zone": "test"},
        )
        original = worker_registry.record_worker_heartbeat

        def record_heartbeat(*args, **kwargs):
            original(*args, **kwargs)
            if handler_started.is_set():
                heartbeat_written.set()

        monkeypatch.setattr(worker_registry, "record_worker_heartbeat", record_heartbeat)
        thread = threading.Thread(
            target=run_worker,
            args=(runner,),
            kwargs={"poll_interval_seconds": NEVER, "stop": stop, "telemetry": telemetry},
        )
        thread.start()
        assert handler_started.wait(timeout=WAIT_TIMEOUT)
        assert heartbeat_written.wait(timeout=WAIT_TIMEOUT)

        with worker_database() as db:
            running = db.get(JobWorker, runner._worker_id)
            assert running.last_heartbeat_at > running.started_at
            assert running.active_claimed_jobs == 1
            assert running.queues == ["campaign"]
            assert running.role == "processor"

        stop.set()
        release_handler.set()
        thread.join(timeout=WAIT_TIMEOUT)
        assert not thread.is_alive()
        with worker_database() as db:
            stopped = db.get(JobWorker, runner._worker_id)
            assert stopped.stopped_at is not None
            assert stopped.active_claimed_jobs == 0

    def test_heartbeat_recreates_a_missing_start_record(self, worker_database):
        heartbeat_at = T0 + timedelta(seconds=5)
        with worker_database() as db:
            worker_registry.record_worker_heartbeat(
                "00000000-0000-4000-8000-000000000004",
                started_at=T0,
                now=heartbeat_at,
                queues=("campaign",),
                role="processor",
                label="campaign-1",
                metadata={"zone": "test"},
                db=db,
            )

        with worker_database() as db:
            worker = db.get(JobWorker, "00000000-0000-4000-8000-000000000004")
            assert worker is not None
            assert worker.last_heartbeat_at == heartbeat_at.replace(tzinfo=None)
            assert worker.queues == ["campaign"]
            assert worker.worker_metadata == {"zone": "test"}

    def test_stopping_an_unknown_worker_is_a_no_op(self, worker_database):
        with worker_database() as db:
            worker_registry.record_worker_stop("missing", now=T0, db=db)
            assert db.get(JobWorker, "missing") is None

    def test_a_heartbeat_failure_does_not_kill_job_execution(self, worker_database, monkeypatch, caplog):
        heartbeat_attempted = threading.Event()
        release_handler = threading.Event()
        handled = threading.Event()
        stop = threading.Event()
        clock = StepClock()
        registry = JobHandlerRegistry()

        def handle(_event):
            assert release_handler.wait(timeout=WAIT_TIMEOUT)
            handled.set()

        registry.register("work.created", "handler", handle)
        with worker_database() as db:
            jobs_crud.enqueue_job(
                new_event("work.created", {}, source="test"),
                "handler",
                max_attempts=3,
                now=T0,
                db=db,
            )
        runner = JobRunner(
            registry=registry,
            clock=clock,
            session_factory=worker_database,
            worker_id="00000000-0000-4000-8000-000000000002",
            lease_seconds=60,
            batch_size=1,
            backoff_base_seconds=1,
            backoff_max_seconds=2,
        )
        telemetry = WorkerTelemetry(
            instance_id=runner._worker_id,
            clock=clock,
            session_factory=worker_database,
            heartbeat_interval_seconds=0.01,
            queues=None,
        )

        def fail_heartbeat(*args, **kwargs):
            heartbeat_attempted.set()
            raise RuntimeError("telemetry database unavailable")

        monkeypatch.setattr(worker_registry, "record_worker_heartbeat", fail_heartbeat)
        with caplog.at_level("WARNING"):
            thread = threading.Thread(
                target=run_worker,
                args=(runner,),
                kwargs={"poll_interval_seconds": NEVER, "stop": stop, "telemetry": telemetry},
            )
            thread.start()
            assert heartbeat_attempted.wait(timeout=WAIT_TIMEOUT)
            stop.set()
            release_handler.set()
            thread.join(timeout=WAIT_TIMEOUT)

        assert handled.is_set()
        assert not thread.is_alive()
        assert "Durable worker heartbeat failed" in caplog.text

    @pytest.mark.parametrize(("field", "value"), [("role", "r" * 101), ("label", "l" * 201)])
    def test_invalid_metadata_fails_before_a_thread_starts(self, worker_database, field, value):
        kwargs = {field: value}

        with pytest.raises(ValueError, match=field):
            WorkerTelemetry(
                instance_id="00000000-0000-4000-8000-000000000003",
                clock=StepClock(),
                session_factory=worker_database,
                heartbeat_interval_seconds=1,
                queues=None,
                **kwargs,
            )

    @pytest.mark.parametrize(
        "metadata",
        [
            {"value": "x" * MAX_WORKER_METADATA_BYTES},
            {"value": object()},
            {"value": float("nan")},
        ],
    )
    def test_invalid_worker_metadata_fails_before_a_thread_starts(self, worker_database, metadata):
        with pytest.raises(ValueError, match="worker metadata"):
            WorkerTelemetry(
                instance_id="00000000-0000-4000-8000-000000000003",
                clock=StepClock(),
                session_factory=worker_database,
                heartbeat_interval_seconds=1,
                queues=None,
                metadata=metadata,
            )


class TestAWedgedThreadIsReported:
    """Shutdown abandons a thread that will not stop, which is right \u2014 it is a
    daemon and shutdown must not block. Doing it silently is what is wrong: the
    next symptom is work that appears to keep running after shutdown, with
    nothing in the log connecting the two.
    """

    def test_giving_up_on_a_thread_warns(self, caplog):
        stop = threading.Event()
        wedged = threading.Thread(target=lambda: stop.wait(WAIT_TIMEOUT), name="wedged-thread", daemon=True)
        wedged.start()

        with caplog.at_level("WARNING"):
            signal_and_join(wedged, threading.Event(), timeout=0.01)

        assert "wedged-thread" in caplog.text
        assert "did not stop" in caplog.text
        stop.set()
        wedged.join(timeout=WAIT_TIMEOUT)

    def test_a_thread_that_stops_in_time_is_quiet(self, caplog):
        stop = threading.Event()
        finished = threading.Thread(target=lambda: stop.wait(WAIT_TIMEOUT), name="tidy-thread", daemon=True)
        finished.start()

        with caplog.at_level("WARNING"):
            signal_and_join(finished, stop, timeout=WAIT_TIMEOUT)

        assert caplog.text == ""
