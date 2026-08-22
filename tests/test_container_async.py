"""The async composition root: every capability URI resolves to the right async backend.

The sync counterpart of this file is ``test_container.py``, and the resemblance is
the point. ``jasil.container_async`` deliberately owns no selection logic — it
delegates the topology check, the scheme mapping, the error wording and the
geocoding decision to ``jasil.container`` — so what these tests protect is that
*delegation*: the same URI must reach the same capability under either face, with
only the concrete class differing.

Redis is faked at the client boundary so no server is needed.
"""

import fakeredis.aioredis
import pytest

import jasil._core.redis_clients as redis_clients
import jasil.container as container
import jasil.container_async as container_async
import jasil.settings as settings
from jasil.backends.clock_system import SystemClock
from jasil.backends.events_inprocess_async import AsyncInProcessEventBus
from jasil.backends.events_redis_async import AsyncRedisStreamEventBus
from jasil.backends.geocoding_http_async import AsyncHttpGeocoding, AsyncNullGeocoding
from jasil.backends.lock_noop_async import AsyncNoopLock
from jasil.backends.lock_pg_async import AsyncPgAdvisoryLock
from jasil.backends.state_memory_async import AsyncMemoryState
from jasil.backends.state_redis_async import AsyncRedisState
from jasil.backends.storage_local_async import AsyncLocalStorage
from jasil.profile import DeploymentProfile


@pytest.fixture(autouse=True)
async def fake_async_redis(monkeypatch):
    """Serve every shared async-client request from fakeredis."""

    async def _client(uri, *, purpose, decode_responses=True):
        return fakeredis.aioredis.FakeRedis(decode_responses=decode_responses)

    monkeypatch.setattr(redis_clients, "get_shared_async_client", _client)
    redis_clients.reset_shared_async_clients()
    yield
    redis_clients.reset_shared_async_clients()


def _settings(**kwargs) -> settings.JasilSettings:
    return settings.JasilSettings(**kwargs)


class TestStateResolution:
    async def test_memory_uri_selects_the_in_process_backend(self):
        assert isinstance(await container_async._build_state(_settings(state_uri="memory://")), AsyncMemoryState)

    @pytest.mark.parametrize("uri", ["redis://c:6379/0", "rediss://c:6379/0", "unix:///run/redis.sock"])
    async def test_every_redis_scheme_selects_the_redis_backend(self, uri):
        assert isinstance(await container_async._build_state(_settings(state_uri=uri)), AsyncRedisState)

    async def test_an_unknown_scheme_is_refused(self):
        """Failing to start beats silently running on the wrong backend."""
        with pytest.raises(ValueError, match="Unsupported state_uri scheme"):
            await container_async._build_state(_settings(state_uri="mysql://db"))


class TestStorageResolution:
    async def test_local_uri_selects_the_filesystem_backend(self):
        assert isinstance(await container_async._build_storage(_settings(storage_uri="local://")), AsyncLocalStorage)

    async def test_the_data_dir_roots_the_local_backend(self, tmp_path):
        storage = await container_async._build_storage(_settings(storage_uri="local://", data_dir=str(tmp_path)))

        assert str(tmp_path) in str(storage._base)

    async def test_an_unknown_scheme_is_refused(self):
        with pytest.raises(ValueError, match="Unsupported storage_uri scheme"):
            await container_async._build_storage(_settings(storage_uri="ftp://files"))


class TestEventsResolution:
    async def test_memory_uri_selects_the_in_process_bus(self):
        bus = await container_async._build_events(_settings(events_uri="memory://"), None)

        assert isinstance(bus, AsyncInProcessEventBus)

    async def test_redis_uri_selects_the_stream_bus(self):
        bus = await container_async._build_events(_settings(events_uri="redis://c:6379/0"), None)

        assert isinstance(bus, AsyncRedisStreamEventBus)
        await bus.stop()

    async def test_an_unknown_scheme_is_refused(self):
        with pytest.raises(ValueError, match="Unsupported events_uri scheme"):
            await container_async._build_events(_settings(events_uri="kafka://broker"), None)


class TestLockResolution:
    async def test_noop_uri_selects_the_no_op_lock(self):
        assert isinstance(container_async._build_lock(_settings(lock_uri="noop://")), AsyncNoopLock)

    async def test_an_unknown_scheme_is_refused(self):
        with pytest.raises(ValueError, match="Unsupported lock_uri scheme"):
            container_async._build_lock(_settings(lock_uri="zookeeper://zk"))


class TestGeocodingResolution:
    def test_unconfigured_geocoding_falls_back_to_the_no_op_backend(self):
        """A capability that is always present is one callers never branch on."""
        assert isinstance(container_async._build_geocoding(_settings()), AsyncNullGeocoding)

    def test_a_configured_service_selects_the_http_backend(self, monkeypatch):
        monkeypatch.setattr(
            container,
            "resolve_geocoding",
            lambda _settings: container.GeocodingChoice(
                service="photon",
                base_url="https://photon.example.test/reverse",
                api_key=None,
                min_interval_seconds=1.0,
                user_agent="jasil-test",
            ),
        )

        assert isinstance(container_async._build_geocoding(_settings()), AsyncHttpGeocoding)


class TestBuildAsyncPlatform:
    async def test_the_local_profile_builds_an_all_in_process_platform(self, tmp_path):
        platform = await container_async.build_async_platform(
            _settings(profile=DeploymentProfile.LOCAL, data_dir=str(tmp_path))
        )

        assert isinstance(platform.state, AsyncMemoryState)
        assert isinstance(platform.storage, AsyncLocalStorage)
        assert isinstance(platform.events, AsyncInProcessEventBus)
        assert isinstance(platform.lock, AsyncNoopLock)
        assert isinstance(platform.clock, SystemClock)
        await platform.aclose()

    async def test_the_clock_is_shared_with_the_sync_face_unchanged(self, tmp_path):
        """Reading a clock does no I/O, so there is nothing to make async."""
        async_platform = await container_async.build_async_platform(_settings(data_dir=str(tmp_path)))
        sync_platform = container.build_platform(_settings(data_dir=str(tmp_path)))

        assert type(async_platform.clock) is type(sync_platform.clock)
        await async_platform.aclose()

    async def test_the_field_names_match_the_sync_platform_exactly(self, tmp_path):
        """Host code reading ``platform.state`` must read the same name either way."""
        async_fields = set(container_async.AsyncPlatform.__dataclass_fields__)
        sync_fields = set(container.Platform.__dataclass_fields__)

        assert async_fields == sync_fields

    async def test_the_event_log_recorder_is_absent_by_default(self, tmp_path):
        platform = await container_async.build_async_platform(_settings(data_dir=str(tmp_path)))

        assert platform.recorder is None
        await platform.aclose()

    async def test_enabling_the_event_log_attaches_a_recorder(self, tmp_path, session_factory):
        platform = await container_async.build_async_platform(
            _settings(data_dir=str(tmp_path), event_log=settings.EventLogSettings(enabled=True))
        )

        assert platform.recorder is not None
        await platform.aclose()

    async def test_it_falls_back_to_the_installed_settings(self, tmp_path):
        """Callers that configured settings at startup should not have to pass them."""
        settings.configure(_settings(data_dir=str(tmp_path)))

        platform = await container_async.build_async_platform()

        assert isinstance(platform.storage, AsyncLocalStorage)
        await platform.aclose()

    async def test_the_platform_is_immutable(self, tmp_path):
        """It is shared process-wide; a mutation would be visible everywhere."""
        platform = await container_async.build_async_platform(_settings(data_dir=str(tmp_path)))

        with pytest.raises(Exception, match=r"frozen|immutable|cannot assign"):
            platform.state = AsyncMemoryState()  # type: ignore[misc]
        await platform.aclose()

    async def test_a_distributed_profile_without_uris_fails_fast(self):
        with pytest.raises(ValueError, match="must be set explicitly"):
            await container_async.build_async_platform(_settings(profile=DeploymentProfile.DISTRIBUTED))

    async def test_a_distributed_profile_assembles_shared_backends(self, session_factory, async_session_factory):
        platform = await container_async.build_async_platform(
            _settings(
                profile=DeploymentProfile.DISTRIBUTED,
                state_uri="redis://c:6379/0",
                storage_uri="s3://bucket",
                events_uri="redis://c:6379/1",
                lock_uri="postgres-advisory://",
            )
        )

        assert isinstance(platform.state, AsyncRedisState)
        assert isinstance(platform.events, AsyncRedisStreamEventBus)
        assert isinstance(platform.lock, AsyncPgAdvisoryLock)
        assert platform.profile is DeploymentProfile.DISTRIBUTED
        await platform.aclose()


class TestDeploymentConsistencyGate:
    """``build_async_platform`` applies the *same* topology rules as the sync root.

    The rules live in :mod:`jasil.capabilities` and the gate in
    :mod:`jasil.container`; what matters here is that the async root actually
    calls it, and calls it *before* constructing a backend, so the failure names
    the setting rather than surfacing as a connection error further down.
    """

    async def test_an_inconsistent_wiring_stops_the_build(self, tmp_path):
        with pytest.raises(ValueError, match="deployment wiring is inconsistent"):
            await container_async.build_async_platform(_settings(data_dir=str(tmp_path), web_workers=4))

    async def test_it_can_be_downgraded_to_a_warning(self, tmp_path, caplog):
        """A development machine may knowingly run a multi-worker memory setup."""
        with caplog.at_level("WARNING"):
            platform = await container_async.build_async_platform(
                _settings(data_dir=str(tmp_path), web_workers=4, enforce_deployment_consistency=False)
            )

        assert isinstance(platform.state, AsyncMemoryState)
        assert "Inconsistent deployment wiring" in caplog.text
        await platform.aclose()

    async def test_a_consistent_deployment_logs_the_capability_report(self, tmp_path, caplog):
        with caplog.at_level("INFO"):
            platform = await container_async.build_async_platform(_settings(data_dir=str(tmp_path)))

        assert "JASIL async platform capabilities" in caplog.text
        assert "Deployment profile: local" in caplog.text
        await platform.aclose()


class TestAclose:
    async def test_closing_stops_the_bus(self, tmp_path):
        platform = await container_async.build_async_platform(_settings(data_dir=str(tmp_path)))
        stopped = []

        async def _stop():
            stopped.append(True)

        object.__setattr__(platform.events, "stop", _stop)
        await platform.aclose()

        assert stopped == [True]

    async def test_closing_twice_is_harmless(self, tmp_path):
        """Shutdown paths get run twice more often than anyone plans for."""
        platform = await container_async.build_async_platform(_settings(data_dir=str(tmp_path)))

        await platform.aclose()
        await platform.aclose()

    async def test_a_failing_bus_does_not_mask_the_shutdown(self, tmp_path, caplog):
        platform = await container_async.build_async_platform(_settings(data_dir=str(tmp_path)))

        async def _boom():
            raise RuntimeError("bus is wedged")

        object.__setattr__(platform.events, "stop", _boom)
        await platform.aclose()

        assert "Failed to stop the event bus" in caplog.text

    async def test_a_failing_geocoding_client_does_not_mask_the_shutdown(self, tmp_path, caplog, monkeypatch):
        monkeypatch.setattr(
            container,
            "resolve_geocoding",
            lambda _settings: container.GeocodingChoice(
                service="photon",
                base_url="https://photon.example.test/reverse",
                api_key=None,
                min_interval_seconds=1.0,
                user_agent="jasil-test",
            ),
        )
        platform = await container_async.build_async_platform(_settings(data_dir=str(tmp_path)))

        async def _boom():
            raise RuntimeError("client is wedged")

        platform.geocoding.aclose = _boom  # type: ignore[method-assign]
        await platform.aclose()

        assert "Failed to close the geocoding client" in caplog.text
