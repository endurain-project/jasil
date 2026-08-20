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

import pytest

from jasil._core.threads import signal_and_join
from jasil.jobs.worker import BackgroundWorker, run_worker

# Long enough that an unwanted poll would hang the test rather than pass slowly.
NEVER = 3600.0
WAIT_TIMEOUT = 5.0


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
