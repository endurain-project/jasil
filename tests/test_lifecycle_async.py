"""Ordered shutdown of what JASIL owns in a process, for the async face.

The async counterpart of ``test_lifecycle.py``, and the value is the same: the
ordering, and what it tolerates. The async worker has to stop before the async
bus it publishes through, and every step has to survive being called when the
thing it is stopping was never started, or is already broken.

The one thing this file protects that the sync file cannot is the *separation* of
the two slots: ``shutdown`` must not touch the async platform's resources, and
``ashutdown`` must not touch the sync one's — a process running an async API
beside a synchronous worker calls both, and neither may release the other's
connections early.
"""

import pytest

import jasil.container_async as container_async
import jasil.jobs.service as jobs_service
import jasil.lifecycle as jasil_lifecycle
import jasil.runtime as platform_runtime
import jasil.settings as settings


class RecordingAsyncPlatform:
    """Stands in for the assembled async platform, recording that it was released."""

    def __init__(self, error: Exception | None = None) -> None:
        self.closed = False
        self._error = error

    async def aclose(self) -> None:
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
    monkeypatch.setattr(jobs_service, "_async_worker", None, raising=False)


class TestAshutdown:
    async def test_it_releases_the_async_platform(self, monkeypatch):
        platform = RecordingAsyncPlatform()
        monkeypatch.setattr(platform_runtime, "_active_async_platform", platform)

        await jasil_lifecycle.ashutdown()

        assert platform.closed is True

    async def test_it_unpublishes_the_async_platform(self, monkeypatch):
        monkeypatch.setattr(platform_runtime, "_active_async_platform", RecordingAsyncPlatform())

        await jasil_lifecycle.ashutdown()

        assert platform_runtime.is_async_platform_active() is False

    async def test_a_publish_after_shutdown_fails_loudly(self, monkeypatch):
        """Better than silently dispatching onto a bus that has already stopped."""
        monkeypatch.setattr(platform_runtime, "_active_async_platform", RecordingAsyncPlatform())

        await jasil_lifecycle.ashutdown()

        with pytest.raises(RuntimeError, match="not initialized"):
            platform_runtime.get_active_async_platform()

    async def test_it_stops_the_async_durable_job_worker_first(self, monkeypatch):
        """The worker runs subscribers, and a subscriber that publishes needs the bus."""
        order: list[str] = []
        platform = RecordingAsyncPlatform()
        monkeypatch.setattr(platform_runtime, "_active_async_platform", platform)

        async def _stop_worker():
            order.append("worker")

        async def _close(_self):
            order.append("platform")

        monkeypatch.setattr(jobs_service, "stop_async_job_worker", _stop_worker)
        monkeypatch.setattr(type(platform), "aclose", _close)

        await jasil_lifecycle.ashutdown()

        assert order == ["worker", "platform"]


class TestItToleratesAPartialStartup:
    async def test_shutting_down_before_anything_started_is_a_no_op(self):
        await jasil_lifecycle.ashutdown()

        assert platform_runtime.is_async_platform_active() is False

    async def test_it_is_idempotent(self, monkeypatch):
        monkeypatch.setattr(platform_runtime, "_active_async_platform", RecordingAsyncPlatform())

        await jasil_lifecycle.ashutdown()
        await jasil_lifecycle.ashutdown()

        assert platform_runtime.is_async_platform_active() is False

    async def test_a_worker_that_will_not_stop_does_not_block_the_platform(self, monkeypatch, caplog):
        platform = RecordingAsyncPlatform()
        monkeypatch.setattr(platform_runtime, "_active_async_platform", platform)

        async def _wedged():
            raise RuntimeError("wedged")

        monkeypatch.setattr(jobs_service, "stop_async_job_worker", _wedged)

        with caplog.at_level("WARNING"):
            await jasil_lifecycle.ashutdown()

        assert platform.closed is True
        assert "Failed to stop the async durable-job worker" in caplog.text

    async def test_the_jobs_extra_being_absent_is_not_an_error(self, monkeypatch):
        """A deployment that never enabled durable jobs has no apscheduler installed."""
        monkeypatch.setitem(__import__("sys").modules, "jasil.jobs.service", None)
        platform = RecordingAsyncPlatform()
        monkeypatch.setattr(platform_runtime, "_active_async_platform", platform)

        await jasil_lifecycle.ashutdown()

        assert platform.closed is True


class TestTheTwoFacesDoNotReleaseEachOther:
    """A process may hold both platforms; neither shutdown may close the other's."""

    async def test_the_sync_shutdown_leaves_the_async_platform_alone(self, monkeypatch):
        async_platform = RecordingAsyncPlatform()
        monkeypatch.setattr(platform_runtime, "_active_async_platform", async_platform)

        jasil_lifecycle.shutdown()

        assert async_platform.closed is False

    async def test_the_async_shutdown_leaves_the_sync_platform_alone(self, monkeypatch):
        closed: list[str] = []

        class RecordingPlatform:
            def close(self) -> None:
                closed.append("sync")

        monkeypatch.setattr(platform_runtime, "_active_platform", RecordingPlatform())

        await jasil_lifecycle.ashutdown()

        assert closed == []


class TestAgainstARealPlatform:
    async def test_it_shuts_a_built_async_platform_down(self, tmp_path, monkeypatch):
        """``AsyncPlatform.aclose`` already never raises; this is the composition working."""
        platform = await container_async.build_async_platform(settings.JasilSettings(data_dir=str(tmp_path)))
        platform_runtime.set_active_async_platform(platform)

        await jasil_lifecycle.ashutdown()

        assert platform_runtime.is_async_platform_active() is False
