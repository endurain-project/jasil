"""The composition root: every capability URI resolves to the right backend.

A wrong resolution here is a whole-deployment failure — the substrate silently
running on process-local memory across replicas, or failing to start. Redis is
faked at the client boundary so no server is needed.
"""

import fakeredis
import pytest

import jasil._core.redis_clients as redis_clients
import jasil.container as container
import jasil.settings as settings
from jasil.backends.clock_system import SystemClock
from jasil.backends.events_inprocess import InProcessEventBus
from jasil.backends.events_redis import RedisStreamEventBus
from jasil.backends.geocoding_http import HttpGeocoding, NullGeocoding
from jasil.backends.lock_noop import NoopLock
from jasil.backends.lock_pg import PgAdvisoryLock
from jasil.backends.state_memory import MemoryState
from jasil.backends.state_redis import RedisState
from jasil.backends.storage_local import LocalStorage
from jasil.profile import DeploymentProfile


@pytest.fixture(autouse=True)
def fake_redis(monkeypatch):
    """Serve every shared-client request from fakeredis."""
    monkeypatch.setattr(
        redis_clients,
        "get_shared_client",
        lambda uri, *, purpose, decode_responses=True: fakeredis.FakeStrictRedis(decode_responses=decode_responses),
    )
    redis_clients.reset_shared_clients()
    yield
    redis_clients.reset_shared_clients()


def _settings(**kwargs) -> settings.JasilSettings:
    return settings.JasilSettings(**kwargs)


class TestStateResolution:
    def test_memory_uri_selects_the_in_process_backend(self):
        assert isinstance(container._build_state(_settings(state_uri="memory://")), MemoryState)

    @pytest.mark.parametrize("uri", ["redis://c:6379/0", "rediss://c:6379/0", "unix:///run/redis.sock"])
    def test_every_redis_scheme_selects_the_redis_backend(self, uri):
        assert isinstance(container._build_state(_settings(state_uri=uri)), RedisState)

    def test_an_unknown_scheme_is_refused(self):
        """Failing to start beats silently running on the wrong backend."""
        with pytest.raises(ValueError, match="Unsupported state_uri scheme"):
            container._build_state(_settings(state_uri="mysql://db"))


class TestStorageResolution:
    def test_local_uri_selects_the_filesystem_backend(self):
        assert isinstance(container._build_storage(_settings(storage_uri="local://")), LocalStorage)

    def test_the_data_dir_roots_the_local_backend(self, tmp_path):
        storage = container._build_storage(_settings(storage_uri="local://", data_dir=str(tmp_path)))

        assert str(tmp_path) in str(storage._base)

    def test_a_path_in_the_uri_overrides_the_data_dir(self, tmp_path):
        storage = container._build_storage(_settings(storage_uri=f"local://{tmp_path}", data_dir="/unused"))

        assert "/unused" not in str(storage._base)

    def test_an_unknown_scheme_is_refused(self):
        with pytest.raises(ValueError, match="Unsupported storage_uri scheme"):
            container._build_storage(_settings(storage_uri="ftp://files"))


class TestEventsResolution:
    def test_memory_uri_selects_the_in_process_bus(self):
        assert isinstance(container._build_events(_settings(events_uri="memory://"), None), InProcessEventBus)

    def test_redis_uri_selects_the_stream_bus(self):
        bus = container._build_events(_settings(events_uri="redis://c:6379/1"), None)

        assert isinstance(bus, RedisStreamEventBus)

    def test_an_unknown_scheme_is_refused(self):
        with pytest.raises(ValueError, match="Unsupported events_uri scheme"):
            container._build_events(_settings(events_uri="kafka://broker"), None)


class TestLockResolution:
    def test_noop_uri_selects_the_no_op_lock(self):
        assert isinstance(container._build_lock(_settings(lock_uri="noop://")), NoopLock)

    def test_postgres_advisory_uri_uses_the_hosts_engine(self, session_factory):
        lock = container._build_lock(_settings(lock_uri="postgres-advisory://"))

        assert isinstance(lock, PgAdvisoryLock)

    def test_an_unknown_scheme_is_refused(self):
        with pytest.raises(ValueError, match="Unsupported lock_uri scheme"):
            container._build_lock(_settings(lock_uri="zookeeper://"))


class TestGeocodingResolution:
    """Geocoding never fails startup — a misconfiguration disables the capability.

    That makes silent misconfiguration the risk, so every branch is covered.
    """

    def test_it_is_disabled_by_default(self):
        assert isinstance(container._build_geocoding(_settings()), NullGeocoding)

    def test_leaving_it_unconfigured_is_quiet(self, caplog):
        """An optional capability nobody asked for must not warn at every startup."""
        with caplog.at_level("WARNING"):
            container._build_geocoding(_settings())

        assert caplog.text == ""

    def test_an_unsupported_provider_disables_it(self, caplog):
        """A non-empty typo *is* worth warning about \u2014 someone meant to enable it."""
        with caplog.at_level("WARNING"):
            provider = container._build_geocoding(_settings(geocoding=settings.GeocodingSettings(provider="mapquest")))

        assert isinstance(provider, NullGeocoding)
        assert "not a supported service" in caplog.text

    def test_geocode_without_an_api_key_is_disabled(self, caplog):
        with caplog.at_level("WARNING"):
            provider = container._build_geocoding(_settings(geocoding=settings.GeocodingSettings(provider="geocode")))

        assert isinstance(provider, NullGeocoding)
        assert "api_key" in caplog.text

    def test_geocode_with_an_api_key_is_enabled(self):
        provider = container._build_geocoding(
            _settings(geocoding=settings.GeocodingSettings(provider="geocode", api_key="k"))
        )

        assert isinstance(provider, HttpGeocoding)

    def test_a_host_failing_ssrf_validation_disables_it(self, monkeypatch):
        """A private or malformed host must not be dialed, and must not stop startup."""
        provider = container._build_geocoding(
            _settings(geocoding=settings.GeocodingSettings(provider="nominatim", nominatim_host="http://bad"))
        )

        assert isinstance(provider, NullGeocoding)

    def test_a_valid_host_is_enabled(self, monkeypatch):
        monkeypatch.setattr(container, "build_reverse_endpoint", lambda host, **kw: f"https://{host}/reverse")

        provider = container._build_geocoding(
            _settings(geocoding=settings.GeocodingSettings(provider="nominatim", nominatim_host="nominatim.test"))
        )

        assert isinstance(provider, HttpGeocoding)

    def test_the_configured_user_agent_is_used(self, monkeypatch):
        """Nominatim's usage policy requires an identifying value, and the
        library must not hard-code a host application's name."""
        monkeypatch.setattr(container, "build_reverse_endpoint", lambda host, **kw: "https://x/reverse")

        provider = container._build_geocoding(
            _settings(
                geocoding=settings.GeocodingSettings(
                    provider="photon", photon_host="photon.test", user_agent="MyApp/1.0"
                )
            )
        )

        assert provider._user_agent == "MyApp/1.0"

    def test_the_rate_limit_becomes_a_minimum_interval(self, monkeypatch):
        monkeypatch.setattr(container, "build_reverse_endpoint", lambda host, **kw: "https://x/reverse")

        provider = container._build_geocoding(
            _settings(geocoding=settings.GeocodingSettings(provider="photon", photon_host="p.test", rate_limit=2.0))
        )

        assert provider._min_interval == 0.5

    def test_a_zero_rate_limit_disables_throttling(self, monkeypatch):
        monkeypatch.setattr(container, "build_reverse_endpoint", lambda host, **kw: "https://x/reverse")

        provider = container._build_geocoding(
            _settings(geocoding=settings.GeocodingSettings(provider="photon", photon_host="p.test", rate_limit=0))
        )

        assert provider._min_interval == 0


class TestBuildPlatform:
    def test_the_local_profile_assembles_single_process_backends(self, tmp_path):
        platform = container.build_platform(_settings(data_dir=str(tmp_path)))

        assert isinstance(platform.state, MemoryState)
        assert isinstance(platform.storage, LocalStorage)
        assert isinstance(platform.events, InProcessEventBus)
        assert isinstance(platform.lock, NoopLock)
        assert isinstance(platform.clock, SystemClock)

    def test_the_event_log_recorder_is_absent_by_default(self, tmp_path):
        platform = container.build_platform(_settings(data_dir=str(tmp_path)))

        assert platform.recorder is None

    def test_enabling_the_event_log_attaches_a_recorder(self, tmp_path, session_factory):
        platform = container.build_platform(
            _settings(data_dir=str(tmp_path), event_log=settings.EventLogSettings(enabled=True))
        )

        assert platform.recorder is not None

    def test_it_falls_back_to_the_installed_settings(self, tmp_path):
        """Callers that configured settings at startup should not have to pass them."""
        settings.configure(_settings(data_dir=str(tmp_path)))

        platform = container.build_platform()

        assert isinstance(platform.storage, LocalStorage)

    def test_the_platform_is_immutable(self, tmp_path):
        """It is shared process-wide; a mutation would be visible everywhere."""
        platform = container.build_platform(_settings(data_dir=str(tmp_path)))

        with pytest.raises(Exception, match=r"frozen|immutable|cannot assign"):
            platform.state = MemoryState()  # type: ignore[misc]

    def test_a_distributed_profile_without_uris_fails_fast(self):
        with pytest.raises(ValueError, match="must be set explicitly"):
            container.build_platform(_settings(profile=DeploymentProfile.DISTRIBUTED))

    def test_a_distributed_profile_assembles_shared_backends(self, session_factory):
        platform = container.build_platform(
            _settings(
                profile=DeploymentProfile.DISTRIBUTED,
                state_uri="redis://c:6379/0",
                storage_uri="s3://bucket",
                events_uri="redis://c:6379/1",
                lock_uri="postgres-advisory://",
            )
        )

        assert isinstance(platform.state, RedisState)
        assert isinstance(platform.events, RedisStreamEventBus)
        assert isinstance(platform.lock, PgAdvisoryLock)
        assert platform.profile is DeploymentProfile.DISTRIBUTED


class TestDeploymentConsistencyGate:
    """``build_platform`` refuses wiring that would diverge across processes.

    The individual rules live in :mod:`jasil.capabilities`; what matters here is
    that the composition root actually applies them, and applies them *before*
    constructing a backend, so the failure names the setting rather than surfacing
    as a connection error further down.
    """

    def test_an_inconsistent_wiring_stops_the_build(self, tmp_path):
        with pytest.raises(ValueError, match="deployment wiring is inconsistent"):
            container.build_platform(_settings(data_dir=str(tmp_path), web_workers=4))

    def test_the_message_names_every_offending_setting(self, tmp_path):
        with pytest.raises(ValueError) as failure:
            container.build_platform(_settings(data_dir=str(tmp_path), web_workers=4))

        assert "state_uri" in str(failure.value)
        assert "events_uri" in str(failure.value)
        assert "lock_uri" in str(failure.value)

    def test_it_can_be_downgraded_to_a_warning(self, tmp_path, caplog):
        """A development machine may knowingly run a multi-worker memory setup."""
        with caplog.at_level("WARNING"):
            platform = container.build_platform(
                _settings(data_dir=str(tmp_path), web_workers=4, enforce_deployment_consistency=False)
            )

        assert isinstance(platform.state, MemoryState)
        assert "Inconsistent deployment wiring" in caplog.text

    def test_a_consistent_deployment_logs_the_capability_report(self, tmp_path, caplog):
        with caplog.at_level("INFO"):
            container.build_platform(_settings(data_dir=str(tmp_path)))

        assert "JASIL platform capabilities" in caplog.text
        assert "Deployment profile: local" in caplog.text
