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
from jasil.profile import DeploymentProfile, JobWorkerMode
from jasil.providers import ClockProvider, MailError, MailMessage, MailNotConfiguredError, ServeFile


@pytest.fixture(autouse=True)
def _clean_process_state():
    yield
    jasil_testing.reset_all()


class TestFixedClock:
    def test_mail_is_recorded_with_the_fixed_clock(self, tmp_path):
        from jasil.providers import MailMessage

        clock = jasil_testing.FixedClock()
        platform = jasil_testing.install_test_platform(tmp_path, clock=clock)

        platform.mail.send(MailMessage(to=["a@example.com"], subject="s", text="t"))

        assert jasil_testing.memory_mail(platform).built[0]["Date"] == "Thu, 01 Jan 2026 12:00:00 +0000"


class TestTestMail:
    def test_an_empty_mail_uri_is_recorded_too(self, tmp_path):
        """An empty value (an unset environment variable) means unset, as in the build."""
        configured = jasil_settings.JasilSettings(mail=jasil_settings.MailSettings(uri=""))

        platform = jasil_testing.install_test_platform(tmp_path, settings=configured)

        assert isinstance(jasil_testing.memory_mail(platform), jasil_testing.MemoryMail)

    def test_an_unset_uri_with_secure_false_is_still_recorded(self, tmp_path):
        """``secure=False`` is valid with no URI (the build ignores it); it must not break the helper."""
        configured = jasil_settings.JasilSettings(mail=jasil_settings.MailSettings(secure=False))

        platform = jasil_testing.install_test_platform(tmp_path, settings=configured)

        assert isinstance(jasil_testing.memory_mail(platform), jasil_testing.MemoryMail)

    def test_a_real_backend_is_refused_by_memory_mail(self, tmp_path):
        configured = jasil_settings.JasilSettings(
            mail=jasil_settings.MailSettings(uri="smtp://relay.test", from_address="a@b.c")
        )
        platform = jasil_testing.install_test_platform(tmp_path, settings=configured)

        with pytest.raises(TypeError, match="memory://"):
            jasil_testing.memory_mail(platform)

    @pytest.mark.parametrize(("enforce", "expected"), [(True, None), (False, "memory://")])
    def test_a_multi_process_deployment_gets_recording_mail_only_without_enforcement(
        self, tmp_path, monkeypatch, enforce, expected
    ):
        """``memory://`` would fail an enforced multi-process build, so it is not injected there."""
        built = []
        monkeypatch.setattr(jasil_testing, "build_platform", lambda configured, **_: built.append(configured))
        monkeypatch.setattr(platform_runtime, "set_active_platform", lambda _platform: None)
        configured = jasil_settings.JasilSettings(web_workers=4, enforce_deployment_consistency=enforce)

        jasil_testing.install_test_platform(tmp_path, settings=configured)

        assert built[0].mail.uri == expected

    @pytest.mark.parametrize(("enforce", "expected"), [(True, None), (False, "memory://")])
    def test_standalone_job_workers_count_as_a_multi_process_deployment(self, tmp_path, monkeypatch, enforce, expected):
        """A worker process is one more process, so recorded mail would go nowhere there too."""
        built = []
        monkeypatch.setattr(jasil_testing, "build_platform", lambda configured, **_: built.append(configured))
        monkeypatch.setattr(platform_runtime, "set_active_platform", lambda _platform: None)
        configured = jasil_settings.JasilSettings(
            job_worker_mode=JobWorkerMode.STANDALONE,
            jobs=jasil_settings.JobSettings(enabled=True),
            enforce_deployment_consistency=enforce,
        )

        jasil_testing.install_test_platform(tmp_path, settings=configured)

        assert built[0].mail.uri == expected

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

        plan = platform.storage.serve("avatars", "1.bin")
        assert isinstance(plan, ServeFile)
        assert plan.path.is_relative_to(tmp_path)
        assert plan.path.read_bytes() == b"x"

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


class TestTestRealtime:
    def test_published_hints_are_captured(self, tmp_path):
        platform = jasil_testing.install_test_platform(tmp_path)
        hints = jasil_testing.capture_hints(platform)

        platform.realtime.notify("7", {"kind": "activity", "ids": (1, 2)})

        assert hints == [("7", {"kind": "activity", "ids": [1, 2]})]

    def test_hints_before_capture_are_not_recorded(self, tmp_path):
        platform = jasil_testing.install_test_platform(tmp_path)
        platform.realtime.notify("1", {})

        hints = jasil_testing.capture_hints(platform)

        assert hints == []

    def test_an_invalid_hint_fails_the_test(self, tmp_path):
        """The programmer error a production publish would raise surfaces here."""
        platform = jasil_testing.install_test_platform(tmp_path)

        with pytest.raises(ValueError):
            platform.realtime.notify("1", {"p": "x" * 5000})

    def test_an_unset_uri_is_in_process_even_when_events_are_redis(self, tmp_path, monkeypatch):
        built = []
        monkeypatch.setattr(jasil_testing, "build_platform", lambda configured, **_: built.append(configured))
        monkeypatch.setattr(platform_runtime, "set_active_platform", lambda _platform: None)

        jasil_testing.install_test_platform(
            tmp_path, settings=jasil_settings.JasilSettings(events_uri="redis://cache:6379/1")
        )

        assert built[0].realtime_uri == "memory://"

    @pytest.mark.parametrize(("enforce", "expected"), [(True, None), (False, "memory://")])
    def test_a_multi_process_deployment_gets_in_process_realtime_only_without_enforcement(
        self, tmp_path, monkeypatch, enforce, expected
    ):
        built = []
        monkeypatch.setattr(jasil_testing, "build_platform", lambda configured, **_: built.append(configured))
        monkeypatch.setattr(platform_runtime, "set_active_platform", lambda _platform: None)
        configured = jasil_settings.JasilSettings(web_workers=4, enforce_deployment_consistency=enforce)

        jasil_testing.install_test_platform(tmp_path, settings=configured)

        assert built[0].realtime_uri == expected

    def test_an_explicit_uri_is_respected(self, tmp_path, monkeypatch):
        built = []
        monkeypatch.setattr(jasil_testing, "build_platform", lambda configured, **_: built.append(configured))
        monkeypatch.setattr(platform_runtime, "set_active_platform", lambda _platform: None)

        jasil_testing.install_test_platform(
            tmp_path, settings=jasil_settings.JasilSettings(realtime_uri="redis://cache:6379/2")
        )

        assert built[0].realtime_uri == "redis://cache:6379/2"

    def test_a_real_backend_is_refused_by_capture_hints(self, tmp_path):
        from jasil.backends.realtime_redis import RedisRealtime

        platform = jasil_testing.install_test_platform(tmp_path)
        bare = jasil_testing.Platform(
            profile=platform.profile,
            state=platform.state,
            storage=platform.storage,
            events=platform.events,
            lock=platform.lock,
            clock=platform.clock,
            realtime=RedisRealtime(None),
        )

        with pytest.raises(TypeError, match="memory://"):
            jasil_testing.capture_hints(bare)


class TestResetAll:
    def test_it_leaves_no_realtime_handler_behind(self, tmp_path):
        """A test holding the old platform must not feed the next test's capture list."""
        platform = jasil_testing.install_test_platform(tmp_path)
        hints = jasil_testing.capture_hints(platform)

        jasil_testing.reset_all()
        platform.realtime.notify("7", {})
        fresh = jasil_testing.install_test_platform(tmp_path)

        assert hints == []
        assert fresh.realtime is not platform.realtime

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

    def test_it_leaves_open_a_shared_redis_client_it_did_not_take(self, tmp_path, monkeypatch):
        """Another platform in the process may still be using it."""
        closed = []

        class Client:
            def close(self):
                closed.append(True)

        monkeypatch.setattr(redis_clients, "create_redis_client", lambda *a, **kw: Client())
        redis_clients.get_shared_client("redis://c:6379/0", purpose="test")
        platform = jasil_testing.install_test_platform(tmp_path)

        platform.close()

        assert closed == []
        assert list(redis_clients._shared_clients) == [("redis://c:6379/0", True)]
        redis_clients.reset_shared_clients()

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
            redis_clients.release_shared_clients([("redis://c:6379/0", True)])

        assert redis_clients._shared_clients == {}
        assert "Failed to close the shared redis client" in caplog.text

    def test_closing_twice_is_safe(self, tmp_path):
        platform = jasil_testing.install_test_platform(tmp_path)

        platform.close()

        platform.close()


class TestSources:
    @staticmethod
    def _sources(value):
        return jasil_settings.RuntimeSources(mail=lambda: value)

    def test_sources_reach_the_platform(self, tmp_path):
        mail = jasil_settings.MailSettings(uri="memory://", from_address="a@example.com")

        platform = jasil_testing.install_test_platform(tmp_path, sources=self._sources(mail))

        from jasil._reloading import ReloadingMail

        assert isinstance(platform.mail, ReloadingMail)

    def test_the_source_decides_what_mail_does(self, tmp_path):
        """No ``memory://`` is substituted for a sourced capability: an unset source stays unset."""
        platform = jasil_testing.install_test_platform(tmp_path, sources=self._sources(jasil_settings.MailSettings()))

        with pytest.raises(MailNotConfiguredError):
            platform.mail.send(MailMessage(to=["a@b.c"], subject="s", text="t"))

    def test_memory_mail_unwraps_a_sourced_memory_backend(self, tmp_path):
        mail = jasil_settings.MailSettings(uri="memory://", from_address="a@example.com")
        platform = jasil_testing.install_test_platform(tmp_path, sources=self._sources(mail))

        platform.mail.send(MailMessage(to=["a@b.c"], subject="s", text="t"))

        assert len(jasil_testing.memory_mail(platform).outbox) == 1

    def test_memory_mail_refuses_a_sourced_smtp_backend(self, tmp_path, monkeypatch):
        mail = jasil_settings.MailSettings(uri="smtp://relay.test", from_address="a@b.c")
        monkeypatch.setattr("socket.getaddrinfo", lambda *args, **kwargs: [])
        platform = jasil_testing.install_test_platform(tmp_path, sources=self._sources(mail))
        with pytest.raises(MailError):
            platform.mail.send(MailMessage(to=["a@b.c"], subject="s", text="t"))

        with pytest.raises(TypeError, match="memory://"):
            jasil_testing.memory_mail(platform)
