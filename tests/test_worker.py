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

import logging
import threading
from datetime import UTC, datetime, timedelta

import pytest

import jasil.jobs._worker_registry as worker_registry
import jasil.jobs.crud as jobs_crud
from jasil._core.threads import STOP_JOIN_TIMEOUT_SECONDS, signal_and_join
from jasil._core.timestamps import as_utc
from jasil.events import new_event
from jasil.jobs._worker_metadata import MAX_WORKER_METADATA_BYTES
from jasil.jobs.models import JobWorker
from jasil.jobs.registry import JobHandlerRegistry
from jasil.jobs.runner import JobRunner
from jasil.jobs.worker import BackgroundWorker, WorkerHeartbeat, WorkerTelemetry, run_worker

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

    def run_once(self, stop=None) -> int:
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

    def test_an_exit_from_an_iteration_ends_the_loop_loudly(self, caplog):
        """A worker that silently stops leaves the queue unattended with nothing in the log to say why."""
        stop = threading.Event()
        runner = ScriptedRunner([SystemExit(3), 1], stop)

        with caplog.at_level("ERROR"), pytest.raises(SystemExit):
            run_worker(runner, poll_interval_seconds=0, stop=stop)

        assert runner.calls == 1
        assert "Durable job worker stopped by SystemExit" in caplog.text

    def test_a_failed_iteration_counts_as_empty(self, caplog):
        """It must back off rather than spin on a database that is still down."""
        stop = RecordingEvent()
        runner = ScriptedRunner([RuntimeError("connection reset")], stop)

        run_worker(runner, poll_interval_seconds=0, stop=stop)

        assert stop.waits == [0, 0]

    def test_an_outage_is_one_warning_and_the_recovery_one_info(self, caplog, postgres_error):
        """A database that is down is a degraded dependency, not a defect: no ERROR and no traceback per poll."""
        stop = threading.Event()
        script = [postgres_error("57P01"), postgres_error("08006"), 0, postgres_error("57P03"), 1]
        runner = ScriptedRunner(script, stop)

        with caplog.at_level("DEBUG", logger="jasil"):
            run_worker(runner, poll_interval_seconds=0, stop=stop)

        assert [(record.levelname, record.getMessage()) for record in _outage_records(caplog)] == [
            ("WARNING", "Durable job worker cannot reach the database; retrying"),
            ("INFO", "Durable job worker reached the database again"),
            ("WARNING", "Durable job worker cannot reach the database; retrying"),
            ("INFO", "Durable job worker reached the database again"),
        ]
        assert max(record.levelno for record in caplog.records) == logging.WARNING
        assert all(record.exc_info is None for record in caplog.records)
        assert [record.error for record in caplog.records if record.levelname == "WARNING"] == [
            "OperationalError",
            "OperationalError",
        ]

    def test_a_database_defect_is_still_an_error_with_a_traceback(self, caplog, postgres_error):
        """A full disk is reachable and broken: someone has to fix it."""
        stop = threading.Event()
        disk_full = postgres_error("53100")
        runner = ScriptedRunner([disk_full], stop)

        with caplog.at_level("ERROR", logger="jasil"):
            run_worker(runner, poll_interval_seconds=0, stop=stop)

        [record] = caplog.records
        assert record.getMessage() == "Durable job worker iteration failed"
        assert record.exc_info[1] is disk_full

    def test_a_defect_during_an_outage_does_not_announce_a_recovery(self, caplog, postgres_error):
        stop = threading.Event()
        runner = ScriptedRunner([postgres_error("57P01"), RuntimeError("bug"), 0], stop)

        with caplog.at_level("INFO", logger="jasil"):
            run_worker(runner, poll_interval_seconds=0, stop=stop)

        assert [record.levelname for record in _outage_records(caplog)] == ["WARNING", "ERROR", "INFO"]

    def test_a_worker_on_a_stopped_database_warns_once_until_it_is_back(
        self, session_factory, unreachable_session_factory, caplog
    ):
        """The real runner, claiming against a database that refuses connections and then accepts them."""
        database = SwitchableDatabase(up=session_factory, down=unreachable_session_factory)
        stop = threading.Event()
        runner = OutageRunner(_job_runner(database), database, stop, down=[True, True, True, False, False])

        with caplog.at_level("DEBUG", logger="jasil"):
            run_worker(runner, poll_interval_seconds=0, stop=stop)

        assert [record.levelname for record in _outage_records(caplog)] == ["WARNING", "INFO"]
        assert max(record.levelno for record in caplog.records) == logging.WARNING
        assert all(record.exc_info is None for record in caplog.records)


def _outage_records(caplog) -> list[logging.LogRecord]:
    """The worker loop's own records, without its started and stopped lines."""
    lifecycle = {"Durable job worker started", "Durable job worker stopped"}
    return [
        record
        for record in caplog.records
        if record.name == "jasil.jobs.worker" and record.getMessage() not in lifecycle
    ]


def _job_runner(session_factory) -> JobRunner:
    """A real runner with one subscriber: a worker with no handler claims nothing and never reaches the database."""
    registry = JobHandlerRegistry()
    registry.register("work.created", "handler", lambda _event: None)
    return JobRunner(
        registry=registry,
        clock=StepClock(),
        session_factory=session_factory,
        worker_id="00000000-0000-4000-8000-000000000009",
        lease_seconds=60,
        max_runtime_seconds=600,
        heartbeat_interval_seconds=15,
        batch_size=1,
        backoff_base_seconds=1,
        backoff_max_seconds=2,
    )


class SwitchableDatabase:
    """A session factory that refuses every connection while ``is_down``, and opens real sessions otherwise."""

    def __init__(self, *, up, down) -> None:
        self._up = up
        self._down = down
        self.is_down = False

    def __call__(self):
        return (self._down if self.is_down else self._up)()


class OutageRunner:
    """Run the real runner once per entry of ``down``, with the database down or up as it says, then stop."""

    def __init__(self, runner: JobRunner, database: SwitchableDatabase, stop: threading.Event, *, down) -> None:
        self._runner = runner
        self._database = database
        self._stop = stop
        self._down = list(down)

    def run_once(self, stop=None) -> int:
        if not self._down:
            self._stop.set()
            return 0
        self._database.is_down = self._down.pop(0)
        return self._runner.run_once(stop)


class SignallingRunner:
    """Reports that it ran, on whichever thread the worker put it on."""

    def __init__(self) -> None:
        self.ran = threading.Event()
        self.thread_name: str | None = None

    def run_once(self, stop=None) -> int:
        self.thread_name = threading.current_thread().name
        self.ran.set()
        return 0


class BlockingRunner:
    """Hold one handler-like iteration until the test explicitly releases it."""

    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def run_once(self, stop=None) -> int:
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

    def test_stop_allows_the_worker_thread_time_to_join_its_heartbeat(self, monkeypatch):
        """The outer join must outlast the heartbeat join the worker thread does on exit."""
        joined = []

        class Heartbeat:
            def start(self):
                pass

            def stop(self):
                return True

        def record_join(thread, stop, *, timeout):
            joined.append(timeout)
            return True

        monkeypatch.setattr("jasil.jobs.worker.signal_and_join", record_join)
        worker = BackgroundWorker(SignallingRunner(), poll_interval_seconds=0.01, heartbeat=Heartbeat())

        worker.stop(timeout=1.0)

        assert joined == [1.0 + STOP_JOIN_TIMEOUT_SECONDS]

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
        """Starting while it finishes its handler raises: it is about to exit, so returning would leave no worker."""
        runner = BlockingRunner()
        worker = BackgroundWorker(runner, poll_interval_seconds=0)
        worker.start()
        assert runner.started.wait(timeout=WAIT_TIMEOUT)
        original = worker._thread

        assert worker.stop(timeout=0.01) is False
        assert worker._thread is original
        with pytest.raises(RuntimeError, match="still stopping"):
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

    def monotonic(self) -> float:
        return self.now().timestamp()


class TestWorkerTelemetry:
    def test_a_stopped_database_is_warned_about_without_tracebacks(self, unreachable_session_factory, caplog):
        """Every beat of an outage would otherwise print the same traceback again."""
        runner = _job_runner(unreachable_session_factory)
        runner.leases.hold({"job-1": 1})
        telemetry = WorkerTelemetry(
            instance_id="00000000-0000-4000-8000-000000000009",
            clock=StepClock(),
            session_factory=unreachable_session_factory,
            queues=None,
        )
        heartbeat = WorkerHeartbeat(leases=runner.leases, interval_seconds=NEVER, telemetry=telemetry)

        with caplog.at_level("DEBUG", logger="jasil"):
            telemetry.record_start()
            heartbeat.beat()
            telemetry.record_stop()

        assert [(record.levelname, record.getMessage()) for record in caplog.records] == [
            ("WARNING", "Durable worker start telemetry failed; the database is unavailable"),
            ("WARNING", "Durable job lease renewal failed; the database is unavailable"),
            ("WARNING", "Durable worker heartbeat failed; the database is unavailable"),
            ("WARNING", "Durable worker stop telemetry failed; the database is unavailable"),
        ]
        assert all(record.exc_info is None for record in caplog.records)

    def test_heartbeat_advances_during_a_long_handler_and_stop_is_recorded(
        self,
        threaded_session_factory,
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
        with threaded_session_factory() as db:
            jobs_crud.insert_job(
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
            session_factory=threaded_session_factory,
            worker_id="00000000-0000-4000-8000-000000000001",
            lease_seconds=60,
            max_runtime_seconds=600,
            heartbeat_interval_seconds=15,
            batch_size=1,
            backoff_base_seconds=1,
            backoff_max_seconds=2,
            queues=("campaign",),
        )
        heartbeat = WorkerHeartbeat(
            leases=runner.leases,
            interval_seconds=0.01,
            telemetry=WorkerTelemetry(
                instance_id=runner._worker_id,
                clock=clock,
                session_factory=threaded_session_factory,
                queues=runner._queues,
                role="processor",
                label="campaign-1",
                metadata={"zone": "test"},
            ),
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
            kwargs={"poll_interval_seconds": NEVER, "stop": stop, "heartbeat": heartbeat},
        )
        thread.start()
        assert handler_started.wait(timeout=WAIT_TIMEOUT)
        assert heartbeat_written.wait(timeout=WAIT_TIMEOUT)

        with threaded_session_factory() as db:
            running = db.get(JobWorker, runner._worker_id)
            assert running.last_heartbeat_at > running.started_at
            assert running.queues == ["campaign"]
            assert running.role == "processor"

        stop.set()
        release_handler.set()
        thread.join(timeout=WAIT_TIMEOUT)
        assert not thread.is_alive()
        with threaded_session_factory() as db:
            stopped = db.get(JobWorker, runner._worker_id)
            assert stopped.stopped_at is not None

    def test_heartbeat_recreates_a_missing_start_record(self, threaded_session_factory):
        heartbeat_at = T0 + timedelta(seconds=5)
        with threaded_session_factory() as db:
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

        with threaded_session_factory() as db:
            worker = db.get(JobWorker, "00000000-0000-4000-8000-000000000004")
            assert worker is not None
            assert as_utc(worker.last_heartbeat_at) == heartbeat_at
            assert worker.queues == ["campaign"]
            assert worker.worker_metadata == {"zone": "test"}

    def test_stopping_an_unknown_worker_is_a_no_op(self, threaded_session_factory):
        with threaded_session_factory() as db:
            worker_registry.record_worker_stop("missing", now=T0, db=db)
            assert db.get(JobWorker, "missing") is None

    def test_a_heartbeat_failure_does_not_kill_job_execution(self, threaded_session_factory, monkeypatch, caplog):
        heartbeat_attempted = threading.Event()
        handler_started = threading.Event()
        release_handler = threading.Event()
        handled = threading.Event()
        stop = threading.Event()
        clock = StepClock()
        registry = JobHandlerRegistry()

        def handle(_event):
            handler_started.set()
            assert release_handler.wait(timeout=WAIT_TIMEOUT)
            handled.set()

        registry.register("work.created", "handler", handle)
        with threaded_session_factory() as db:
            jobs_crud.insert_job(
                new_event("work.created", {}, source="test"),
                "handler",
                max_attempts=3,
                now=T0,
                db=db,
            )
        runner = JobRunner(
            registry=registry,
            clock=clock,
            session_factory=threaded_session_factory,
            worker_id="00000000-0000-4000-8000-000000000002",
            lease_seconds=60,
            max_runtime_seconds=600,
            heartbeat_interval_seconds=15,
            batch_size=1,
            backoff_base_seconds=1,
            backoff_max_seconds=2,
        )
        heartbeat = WorkerHeartbeat(
            leases=runner.leases,
            interval_seconds=0.01,
            telemetry=WorkerTelemetry(
                instance_id=runner._worker_id,
                clock=clock,
                session_factory=threaded_session_factory,
                queues=None,
            ),
        )

        def fail_heartbeat(*args, **kwargs):
            heartbeat_attempted.set()
            raise RuntimeError("telemetry database unavailable")

        monkeypatch.setattr(worker_registry, "record_worker_heartbeat", fail_heartbeat)
        with caplog.at_level("WARNING"):
            thread = threading.Thread(
                target=run_worker,
                args=(runner,),
                kwargs={"poll_interval_seconds": NEVER, "stop": stop, "heartbeat": heartbeat},
            )
            thread.start()
            assert heartbeat_attempted.wait(timeout=WAIT_TIMEOUT)
            # Only stop once the job is running: the heartbeat can fail before the
            # worker has claimed anything, and a stopped worker never claims.
            assert handler_started.wait(timeout=WAIT_TIMEOUT)
            stop.set()
            release_handler.set()
            thread.join(timeout=WAIT_TIMEOUT)

        assert handled.is_set()
        assert not thread.is_alive()
        assert "Durable worker heartbeat failed" in caplog.text

    @pytest.mark.parametrize(("field", "value"), [("role", "r" * 101), ("label", "l" * 201)])
    def test_invalid_metadata_fails_before_a_thread_starts(self, threaded_session_factory, field, value):
        kwargs = {field: value}

        with pytest.raises(ValueError, match=field):
            WorkerTelemetry(
                instance_id="00000000-0000-4000-8000-000000000003",
                clock=StepClock(),
                session_factory=threaded_session_factory,
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
    def test_invalid_worker_metadata_fails_before_a_thread_starts(self, threaded_session_factory, metadata):
        with pytest.raises(ValueError, match="worker metadata"):
            WorkerTelemetry(
                instance_id="00000000-0000-4000-8000-000000000003",
                clock=StepClock(),
                session_factory=threaded_session_factory,
                queues=None,
                metadata=metadata,
            )

    def test_worker_metadata_is_deeply_detached(self, threaded_session_factory):
        metadata = {"deployment": {"zone": "eu-1"}}
        telemetry = WorkerTelemetry(
            instance_id="00000000-0000-4000-8000-000000000003",
            clock=StepClock(),
            session_factory=threaded_session_factory,
            queues=None,
            metadata=metadata,
        )

        metadata["deployment"]["zone"] = object()

        assert telemetry._metadata == {"deployment": {"zone": "eu-1"}}


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
