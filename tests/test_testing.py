"""The test-support module and platform teardown.

These exist so a host's suite does not have to rediscover which process-wide
slots JASIL installs, so they are worth testing for the same reason: a slot added
later and forgotten here would leak between a host's tests, and the symptom would
appear in *their* suite, not this one.
"""

from datetime import UTC, datetime, timedelta

import pytest

import jasil._core.redis_clients as redis_clients
import jasil.correlation as correlation
import jasil.jobs.registry as jobs_registry
import jasil.runtime as platform_runtime
import jasil.settings as jasil_settings
import jasil.testing as jasil_testing
from jasil.backends.state_memory import MemoryState
from jasil.backends.storage_local import LocalStorage
from jasil.profile import DeploymentProfile
from jasil.providers import ClockProvider


@pytest.fixture(autouse=True)
def _clean_process_state():
    yield
    jasil_testing.reset_all()


class TestFixedClock:
    def test_it_satisfies_the_clock_provider(self):
        assert isinstance(jasil_testing.FixedClock(), ClockProvider)

    def test_time_does_not_move_on_its_own(self):
        """Lease expiry and backoff are measured against this, so drift would
        make every timing assertion flaky."""
        clock = jasil_testing.FixedClock()

        assert clock.now() == clock.now()

    def test_advancing_moves_time_forward(self):
        clock = jasil_testing.FixedClock(datetime(2026, 6, 1, tzinfo=UTC))

        clock.advance(90)

        assert clock.now() == datetime(2026, 6, 1, 0, 1, 30, tzinfo=UTC)

    def test_it_can_move_backwards(self):
        clock = jasil_testing.FixedClock()
        start = clock.now()

        clock.advance(-60)

        assert clock.now() == start - timedelta(seconds=60)

    def test_the_monotonic_reading_tracks_the_advance(self):
        """Code timing an elapsed duration must see the time the test moved."""
        clock = jasil_testing.FixedClock()
        before = clock.monotonic()

        clock.advance(5)

        assert clock.monotonic() - before == pytest.approx(5.0)

    def test_it_starts_at_a_timezone_aware_instant(self):
        assert jasil_testing.FixedClock().now().tzinfo is not None


class TestInstallTestPlatform:
    def test_it_assembles_process_local_backends(self, tmp_path):
        platform = jasil_testing.install_test_platform(tmp_path)

        assert isinstance(platform.state, MemoryState)
        assert isinstance(platform.storage, LocalStorage)
        assert platform.profile is DeploymentProfile.LOCAL

    def test_it_publishes_the_platform(self, tmp_path):
        """A platform that is built but never published makes every publish raise."""
        platform = jasil_testing.install_test_platform(tmp_path)

        assert platform_runtime.get_active_platform() is platform

    def test_storage_is_rooted_in_the_given_directory(self, tmp_path):
        """Otherwise a test writes into the working directory and survives itself."""
        platform = jasil_testing.install_test_platform(tmp_path)

        platform.storage.save("avatars", "1.bin", b"x")

        assert (tmp_path / "avatars" / "1.bin").read_bytes() == b"x"

    def test_the_clock_is_controllable(self, tmp_path):
        clock = jasil_testing.FixedClock()

        platform = jasil_testing.install_test_platform(tmp_path, clock=clock)

        assert platform.clock is clock

    def test_a_default_clock_is_installed(self, tmp_path):
        assert isinstance(jasil_testing.install_test_platform(tmp_path).clock, jasil_testing.FixedClock)

    def test_extra_settings_are_honoured(self, tmp_path):
        platform = jasil_testing.install_test_platform(
            tmp_path, settings=jasil_settings.JasilSettings(event_log=jasil_settings.EventLogSettings(enabled=True))
        )

        assert platform.recorder is not None

    def test_the_settings_it_built_from_are_installed(self, tmp_path):
        """Code reading settings directly has to agree with the platform."""
        jasil_testing.install_test_platform(tmp_path)

        assert jasil_settings.get_settings().data_dir == str(tmp_path)


class TestResetAll:
    def test_it_clears_the_settings(self, tmp_path):
        jasil_settings.configure(jasil_settings.JasilSettings(data_dir=str(tmp_path)))

        jasil_testing.reset_all()

        assert jasil_settings.is_configured() is False

    def test_it_clears_the_correlation_provider(self):
        correlation.configure_provider(lambda: "req-1")

        jasil_testing.reset_all()

        assert correlation.get_correlation_id() is None

    def test_it_unpublishes_the_platform(self, tmp_path):
        jasil_testing.install_test_platform(tmp_path)

        jasil_testing.reset_all()

        with pytest.raises(RuntimeError, match="Platform is not initialized"):
            platform_runtime.get_active_platform()

    def test_it_clears_the_durable_subscriber_registry(self):
        """A registration leaking into the next test changes how events route."""
        jobs_registry.registry.register("order.created", "invoice.render", lambda _e: None)

        jasil_testing.reset_all()

        assert jobs_registry.registry.subscribers_for("order.created") == ()

    def test_it_discards_the_memoized_redis_clients(self, monkeypatch):
        monkeypatch.setattr(redis_clients, "create_redis_client", lambda *a, **kw: object())
        redis_clients.get_shared_client("redis://c:6379/0", purpose="test")

        jasil_testing.reset_all()

        assert redis_clients._shared_clients == {}

    def test_it_leaves_the_orm_mapping_alone(self, mapped_base):
        """Models capture the base at import time; clearing it strands every one."""
        import jasil.orm as orm

        jasil_testing.reset_all()

        assert orm.is_models_mapped() is True
        assert orm.get_active_base() is mapped_base

    def test_it_is_safe_to_call_on_untouched_state(self):
        jasil_testing.reset_all()

        jasil_testing.reset_all()


class TestPlatformClose:
    def test_it_stops_the_event_bus(self, tmp_path, monkeypatch):
        platform = jasil_testing.install_test_platform(tmp_path)
        stopped = []
        monkeypatch.setattr(platform.events, "stop", lambda: stopped.append(True))

        platform.close()

        assert stopped == [True]

    def test_it_closes_the_shared_redis_clients(self, tmp_path, monkeypatch):
        closed = []

        class Client:
            def close(self):
                closed.append(True)

        monkeypatch.setattr(redis_clients, "create_redis_client", lambda *a, **kw: Client())
        redis_clients.get_shared_client("redis://c:6379/0", purpose="test")
        platform = jasil_testing.install_test_platform(tmp_path)

        platform.close()

        assert closed == [True]
        assert redis_clients._shared_clients == {}

    def test_a_bus_that_fails_to_stop_does_not_break_shutdown(self, tmp_path, monkeypatch, caplog):
        """Shutdown must not mask whatever prompted it."""
        platform = jasil_testing.install_test_platform(tmp_path)

        def explode():
            raise RuntimeError("consumer wedged")

        monkeypatch.setattr(platform.events, "stop", explode)

        with caplog.at_level("WARNING"):
            platform.close()

        assert "Failed to stop the event bus" in caplog.text

    def test_a_client_that_fails_to_close_is_still_discarded(self, monkeypatch, caplog):
        class Client:
            def close(self):
                raise OSError("already gone")

        monkeypatch.setattr(redis_clients, "create_redis_client", lambda *a, **kw: Client())
        redis_clients.get_shared_client("redis://c:6379/0", purpose="test")

        with caplog.at_level("WARNING"):
            redis_clients.close_shared_clients()

        assert redis_clients._shared_clients == {}
        assert "Failed to close the shared redis client" in caplog.text

    def test_closing_twice_is_safe(self, tmp_path):
        platform = jasil_testing.install_test_platform(tmp_path)

        platform.close()

        platform.close()
