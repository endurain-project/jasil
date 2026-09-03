"""Ordered shutdown of what JASIL owns in a process.

The value of this module is entirely in the ordering and in what it tolerates:
the worker has to stop before the bus it publishes through, and every step has
to survive being called when the thing it is stopping was never started, or is
already broken. Shutdown runs while something else is often already going wrong.
"""

import threading

import pytest

import jasil.container as container
import jasil.jobs.service as jobs_service
import jasil.lifecycle as jasil_lifecycle
import jasil.runtime as platform_runtime
import jasil.settings as settings


class RecordingPlatform:
    """Stands in for the assembled platform, recording that it was released."""

    def __init__(self, error: Exception | None = None) -> None:
        self.closed = False
        self._error = error

    def close(self) -> None:
        if self._error is not None:
            raise self._error
        self.closed = True


@pytest.fixture(autouse=True)
def _no_leaked_platform():
    """The published platform is process-wide, so a test that sets one clears it."""
    yield
    platform_runtime.reset()


@pytest.fixture(autouse=True)
def _no_leaked_worker(monkeypatch):
    """So is the worker handle."""
    monkeypatch.setattr(jobs_service, "_worker", None)


class TestShutdown:
    def test_it_releases_the_platform(self, monkeypatch):
        platform = RecordingPlatform()
        monkeypatch.setattr(platform_runtime, "_active_platform", platform)

        jasil_lifecycle.shutdown()

        assert platform.closed is True

    def test_it_unpublishes_the_platform(self, monkeypatch):
        monkeypatch.setattr(platform_runtime, "_active_platform", RecordingPlatform())

        jasil_lifecycle.shutdown()

        assert platform_runtime.is_platform_active() is False

    def test_a_publish_after_shutdown_fails_loudly(self, monkeypatch):
        """Better than silently dispatching onto a bus that has already stopped."""
        monkeypatch.setattr(platform_runtime, "_active_platform", RecordingPlatform())

        jasil_lifecycle.shutdown()

        with pytest.raises(RuntimeError, match="not initialized"):
            platform_runtime.get_active_platform()

    def test_it_stops_the_durable_job_worker_first(self, monkeypatch):
        """The worker runs subscribers, and a subscriber that publishes needs the bus."""
        order: list[str] = []
        platform = RecordingPlatform()
        monkeypatch.setattr(platform_runtime, "_active_platform", platform)
        monkeypatch.setattr(jobs_service, "stop_job_worker", lambda: order.append("worker"))
        monkeypatch.setattr(type(platform), "close", lambda _self: order.append("platform"))

        jasil_lifecycle.shutdown()

        assert order == ["worker", "platform"]

    @pytest.mark.asyncio
    async def test_async_shutdown_runs_off_the_event_loop_thread(self, monkeypatch):
        calling_thread = threading.current_thread()
        shutdown_thread = None

        def record_shutdown():
            nonlocal shutdown_thread
            shutdown_thread = threading.current_thread()

        monkeypatch.setattr(jasil_lifecycle, "shutdown", record_shutdown)

        await jasil_lifecycle.shutdown_async()

        assert shutdown_thread is not calling_thread


class TestItToleratesAPartialStartup:
    def test_shutting_down_before_anything_started_is_a_no_op(self):
        jasil_lifecycle.shutdown()

        assert platform_runtime.is_platform_active() is False

    def test_it_is_idempotent(self, monkeypatch):
        monkeypatch.setattr(platform_runtime, "_active_platform", RecordingPlatform())

        jasil_lifecycle.shutdown()
        jasil_lifecycle.shutdown()

        assert platform_runtime.is_platform_active() is False

    def test_a_worker_that_will_not_stop_does_not_block_the_platform(self, monkeypatch, caplog):
        platform = RecordingPlatform()
        monkeypatch.setattr(platform_runtime, "_active_platform", platform)
        monkeypatch.setattr(jobs_service, "stop_job_worker", _raise(RuntimeError("wedged")))

        with caplog.at_level("WARNING"):
            jasil_lifecycle.shutdown()

        assert platform.closed is True
        assert "Failed to stop the durable-job worker" in caplog.text

    def test_the_jobs_extra_being_absent_is_not_an_error(self, monkeypatch):
        """A deployment that never enabled durable jobs has no apscheduler installed."""
        monkeypatch.setitem(__import__("sys").modules, "jasil.jobs.service", None)
        platform = RecordingPlatform()
        monkeypatch.setattr(platform_runtime, "_active_platform", platform)

        jasil_lifecycle.shutdown()

        assert platform.closed is True


class TestAgainstARealPlatform:
    def test_it_shuts_a_built_platform_down(self, tmp_path, monkeypatch):
        """``Platform.close`` already never raises; this is the composition working."""
        built = container.build_platform(settings.JasilSettings(data_dir=str(tmp_path)))
        platform_runtime.set_active_platform(built)

        jasil_lifecycle.shutdown()

        assert platform_runtime.is_platform_active() is False


def _raise(error: Exception):
    def _fail() -> None:
        raise error

    return _fail
