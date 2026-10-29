"""The composition root: every capability URI resolves to the right backend.

A wrong resolution here is a whole-deployment failure — the substrate silently
running on process-local memory across replicas, or failing to start. Redis is
faked at the client boundary so no server is needed.
"""

import dataclasses
import logging

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
from jasil.backends.mail_memory import MemoryMail, NullMail
from jasil.backends.mail_smtp import SmtpMail
from jasil.backends.realtime_inprocess import InProcessRealtime
from jasil.backends.realtime_redis import RedisRealtime
from jasil.backends.state_memory import MemoryState
from jasil.backends.state_redis import RedisState
from jasil.backends.storage_local import LocalStorage
from jasil.backends.storage_s3 import S3Storage
from jasil.profile import DeploymentProfile, JobWorkerMode


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


#: Every cross-process capability on shared backends, so a test isolates one more.
SHARED_WIRING = {
    "state_uri": "redis://cache:6379/0",
    "events_uri": "redis://cache:6379/1",
    "lock_uri": "postgres-advisory://",
}


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

    def test_s3_ignores_local_plain_area_settings(self):
        storage = container._build_storage(
            _settings(
                storage_uri="s3://bucket",
                storage=settings.StorageSettings(local_plain_areas=frozenset({"activity_files"})),
            )
        )

        assert isinstance(storage, S3Storage)


class TestLocalStorageSettings:
    def test_the_defaults_serve_from_the_root_with_no_flat_layout(self, tmp_path):
        storage = container._build_storage(_settings(data_dir=str(tmp_path)))

        assert storage._url_prefix == ""
        assert storage._legacy_layout is False
        assert storage._plain_areas == frozenset()

    def test_the_configured_prefix_and_layout_reach_the_backend(self, tmp_path):
        storage = container._build_storage(
            _settings(
                data_dir=str(tmp_path),
                storage=settings.StorageSettings(
                    local_url_prefix="/media/",
                    local_legacy_layout=True,
                    local_plain_areas=frozenset({"activity_files"}),
                ),
            )
        )

        assert storage._url_prefix == "/media"
        assert storage._legacy_layout is True
        assert storage._plain_areas == frozenset({"activity_files"})
        assert storage.url("avatars", "1.webp").startswith("/media/")


class TestEventsResolution:
    def test_memory_uri_selects_the_in_process_bus(self):
        assert isinstance(container._build_events(_settings(events_uri="memory://"), None), InProcessEventBus)

    def test_redis_uri_selects_the_stream_bus(self):
        bus = container._build_events(_settings(events_uri="redis://c:6379/1"), None)

        assert isinstance(bus, RedisStreamEventBus)

    def test_an_unknown_scheme_is_refused(self):
        with pytest.raises(ValueError, match="Unsupported events_uri scheme"):
            container._build_events(_settings(events_uri="kafka://broker"), None)

    def test_the_default_stream_and_group_reach_the_stream_bus(self):
        bus = container._build_events(_settings(events_uri="redis://c:6379/1"), None)

        assert (bus._stream, bus._group) == ("jasil:events", "jasil")

    def test_the_configured_stream_and_group_reach_the_stream_bus(self):
        """Two deployments on one Redis database must be able to keep their events apart."""
        bus = container._build_events(
            _settings(events_uri="redis://c:6379/1", events_stream="tenant-a:events", events_group="tenant-a"), None
        )

        assert (bus._stream, bus._group) == ("tenant-a:events", "tenant-a")

    @pytest.mark.parametrize("override", [{"events_stream": ""}, {"events_group": ""}])
    def test_an_empty_stream_or_group_is_refused(self, override):
        with pytest.raises(ValueError, match="events_stream and events_group must not be empty"):
            container._build_events(_settings(events_uri="redis://c:6379/1", **override), None)


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

    def test_an_unsupported_scheme_disables_it(self, caplog):
        """A non-empty typo *is* worth warning about \u2014 someone meant to enable it."""
        with caplog.at_level("WARNING"):
            provider = container._build_geocoding(_settings(geocoding=settings.GeocodingSettings(uri="mapquest://x")))

        assert isinstance(provider, NullGeocoding)
        assert "not supported" in caplog.text

    @pytest.mark.parametrize(
        "uri",
        [
            "nominatim.test",
            "nominatim://",
            "nominatim://user:pw@nominatim.test",
            "nominatim://nominatim.test/reverse",
            "nominatim://nominatim.test?x=1",
            "nominatim://nominatim.test:notaport",
            "geocode://geocode.maps.co",
        ],
    )
    def test_a_malformed_uri_disables_it(self, uri):
        provider = container._build_geocoding(_settings(geocoding=settings.GeocodingSettings(uri=uri, api_key="k")))

        assert isinstance(provider, NullGeocoding)

    def test_geocode_without_an_api_key_is_disabled(self, caplog):
        with caplog.at_level("WARNING"):
            provider = container._build_geocoding(_settings(geocoding=settings.GeocodingSettings(uri="geocode://")))

        assert isinstance(provider, NullGeocoding)
        assert "api_key" in caplog.text

    def test_geocode_with_an_api_key_is_enabled(self):
        provider = container._build_geocoding(
            _settings(geocoding=settings.GeocodingSettings(uri="geocode://", api_key="k"))
        )

        assert isinstance(provider, HttpGeocoding)

    def test_a_host_failing_ssrf_validation_disables_it(self, monkeypatch):
        """A private or malformed host must not be dialed, and must not stop startup."""
        provider = container._build_geocoding(
            _settings(geocoding=settings.GeocodingSettings(uri="nominatim://127.0.0.1"))
        )

        assert isinstance(provider, NullGeocoding)

    def test_a_valid_host_is_enabled(self, monkeypatch):
        monkeypatch.setattr(container, "resolve_reverse_endpoint", lambda host, **kw: (f"https://{host}/reverse", None))

        provider = container._build_geocoding(
            _settings(geocoding=settings.GeocodingSettings(uri="nominatim://nominatim.test:8080"))
        )

        assert isinstance(provider, HttpGeocoding)
        assert provider._service == "nominatim"
        assert provider._base_url == "https://nominatim.test:8080/reverse"

    @pytest.mark.parametrize(
        ("uri", "service", "use_https"),
        [
            ("nominatim://h.test", "nominatim", True),
            ("nominatim+http://h.test", "nominatim", False),
            ("photon://h.test", "photon", True),
            ("photon+http://h.test", "photon", False),
        ],
    )
    def test_the_scheme_selects_the_service_and_transport(self, monkeypatch, uri, service, use_https):
        seen = {}

        def fake(host, *, use_https, allowed_hosts=()):
            seen["host"], seen["https"] = host, use_https
            return "https://x/reverse", None

        monkeypatch.setattr(container, "resolve_reverse_endpoint", fake)

        provider = container._build_geocoding(_settings(geocoding=settings.GeocodingSettings(uri=uri)))

        assert provider._service == service
        assert seen == {"host": "h.test", "https": use_https}

    def test_the_configured_user_agent_is_used(self, monkeypatch):
        """Nominatim's usage policy requires an identifying value, and the
        library must not hard-code a host application's name."""
        monkeypatch.setattr(container, "resolve_reverse_endpoint", lambda host, **kw: ("https://x/reverse", None))

        provider = container._build_geocoding(
            _settings(geocoding=settings.GeocodingSettings(uri="photon://photon.test", user_agent="MyApp/1.0"))
        )

        assert provider._user_agent == "MyApp/1.0"

    def test_the_rate_limit_becomes_a_minimum_interval(self, monkeypatch):
        monkeypatch.setattr(container, "resolve_reverse_endpoint", lambda host, **kw: ("https://x/reverse", None))

        provider = container._build_geocoding(
            _settings(geocoding=settings.GeocodingSettings(uri="photon://p.test", rate_limit=2.0))
        )

        assert provider._min_interval == 0.5

    def test_a_zero_rate_limit_disables_throttling(self, monkeypatch):
        monkeypatch.setattr(container, "resolve_reverse_endpoint", lambda host, **kw: ("https://x/reverse", None))

        provider = container._build_geocoding(
            _settings(geocoding=settings.GeocodingSettings(uri="photon://p.test", rate_limit=0))
        )

        assert provider._min_interval == 0


class TestRealtimeResolution:
    def test_memory_uri_selects_the_in_process_backend(self):
        assert isinstance(container._build_realtime(_settings(realtime_uri="memory://")), InProcessRealtime)

    @pytest.mark.parametrize("uri", ["redis://c:6379/0", "rediss://c:6379/0", "unix:///run/redis.sock"])
    def test_every_redis_scheme_selects_the_pub_sub_backend(self, uri):
        assert isinstance(container._build_realtime(_settings(realtime_uri=uri)), RedisRealtime)

    def test_an_unset_uri_follows_the_event_bus(self):
        assert isinstance(container._build_realtime(_settings(events_uri="redis://c:6379/1")), RedisRealtime)

    def test_an_unknown_scheme_is_refused(self):
        with pytest.raises(ValueError, match="Unsupported realtime_uri scheme"):
            container._build_realtime(_settings(realtime_uri="nats://bus"))

    def test_a_local_platform_carries_the_in_process_backend(self, tmp_path):
        platform = container.build_platform(_settings(data_dir=str(tmp_path)))

        assert isinstance(platform.realtime, InProcessRealtime)

    def test_the_configured_channel_reaches_the_redis_backend(self):
        realtime = container._build_realtime(
            _settings(realtime_uri="redis://c:6379/1", realtime_channel="tenant-a:realtime")
        )

        assert isinstance(realtime, RedisRealtime)
        assert realtime._channel == "tenant-a:realtime"

    def test_a_failed_build_releases_the_shared_clients(self, tmp_path, monkeypatch):
        closed = []
        monkeypatch.setattr(container.redis_clients, "close_shared_clients", lambda: closed.append(True))

        with pytest.raises(ValueError, match="Unsupported realtime_uri scheme"):
            container.build_platform(_settings(data_dir=str(tmp_path), realtime_uri="nats://bus"))

        assert closed == [True]

    def test_close_stops_the_provider(self, tmp_path, monkeypatch):
        platform = container.build_platform(_settings(data_dir=str(tmp_path)))
        stopped = []
        monkeypatch.setattr(platform.realtime, "stop", lambda: stopped.append(True))

        platform.close()

        assert stopped == [True]

    def test_close_joins_a_started_redis_subscriber(self, tmp_path, monkeypatch):
        import jasil.backends.realtime_redis as realtime_redis

        monkeypatch.setattr(realtime_redis, "_POLL_TIMEOUT_SECONDS", 0.02)  # how long stop waits for the poll
        server = fakeredis.FakeServer()
        realtime = RedisRealtime(fakeredis.FakeStrictRedis(server=server, decode_responses=True))
        base = container.build_platform(_settings(data_dir=str(tmp_path)))
        platform = container.Platform(
            profile=base.profile,
            state=base.state,
            storage=base.storage,
            events=base.events,
            lock=base.lock,
            clock=base.clock,
            realtime=realtime,
        )
        realtime.start()
        thread = realtime._thread

        platform.close()

        assert not thread.is_alive()


class TestAUriIsNeverEchoed:
    """A Redis URI carries its password; a malformed one must not end up in an error or a log line."""

    SECRET = "S3CRETPW"

    @pytest.mark.parametrize(
        ("build", "setting"),
        [
            (container._build_state, "state_uri"),
            (container._build_storage, "storage_uri"),
            (container._build_events, "events_uri"),
            (container._build_realtime, "realtime_uri"),
            (container._build_lock, "lock_uri"),
        ],
    )
    @pytest.mark.parametrize("uri", ["redis:/:S3CRETPW@cache:6379/0", "mysql://:S3CRETPW@db/x"])
    def test_an_unsupported_uri_is_refused_without_its_secret(self, build, setting, uri):
        arguments = (_settings(**{setting: uri}),) + ((None,) if build is container._build_events else ())

        with pytest.raises(ValueError, match=setting) as excinfo:
            build(*arguments)

        assert self.SECRET not in str(excinfo.value)

    def test_the_audit_sink_uri_is_not_echoed_either(self):
        audit = settings.AuditSettings(external_sink_uri="ftp:/user:S3CRETPW@host")

        with pytest.raises(ValueError, match="external_sink_uri") as excinfo:
            container._build_audit_sink(_settings(audit=audit))

        assert self.SECRET not in str(excinfo.value)

    def test_the_settings_repr_hides_the_redis_uris(self):
        rendered = repr(
            _settings(
                state_uri="redis://:S3CRETPW@c/0",
                events_uri="redis://:S3CRETPW@c/1",
                realtime_uri="redis://:S3CRETPW@c/2",
            )
        )

        assert self.SECRET not in rendered

    def test_the_startup_report_names_no_malformed_uri(self):
        from jasil.capabilities import build_capability_report

        report = build_capability_report(_settings(state_uri="redis:/:S3CRETPW@cache:6379/0")).render()

        assert self.SECRET not in report
        assert "state" in report


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

        with pytest.raises(dataclasses.FrozenInstanceError):
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
        assert isinstance(platform.realtime, RedisRealtime)
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

    def test_in_process_realtime_across_workers_stops_the_build(self, tmp_path):
        with pytest.raises(ValueError, match="realtime_uri resolves to process-local memory"):
            container.build_platform(
                _settings(data_dir=str(tmp_path), web_workers=4, **SHARED_WIRING, realtime_uri="memory://")
            )

    def test_in_process_realtime_across_workers_can_be_downgraded_to_a_warning(self, tmp_path, caplog):
        with caplog.at_level("WARNING"):
            platform = container.build_platform(
                _settings(
                    data_dir=str(tmp_path),
                    web_workers=4,
                    **SHARED_WIRING,
                    realtime_uri="memory://",
                    enforce_deployment_consistency=False,
                )
            )

        assert isinstance(platform.realtime, InProcessRealtime)
        assert "realtime_uri resolves to process-local memory" in caplog.text

    def test_redis_realtime_across_workers_builds(self, tmp_path):
        platform = container.build_platform(
            _settings(data_dir=str(tmp_path), web_workers=4, **SHARED_WIRING, realtime_uri="redis://cache:6379/2")
        )

        assert isinstance(platform.realtime, RedisRealtime)

    def test_standalone_job_workers_stop_the_build_on_in_memory_wiring(self, tmp_path):
        """One API process, one worker process, the default ``local`` profile: two processes."""
        with pytest.raises(ValueError, match="job_worker_mode=standalone") as failure:
            container.build_platform(
                _settings(
                    data_dir=str(tmp_path),
                    job_worker_mode=JobWorkerMode.STANDALONE,
                    jobs=settings.JobSettings(enabled=True),
                    mail=settings.MailSettings(uri="memory://"),
                )
            )

        for name in ("state_uri", "events_uri", "lock_uri", "mail.uri"):
            assert name in str(failure.value)

    def test_standalone_job_workers_build_on_shared_wiring(self, tmp_path):
        platform = container.build_platform(
            _settings(
                data_dir=str(tmp_path),
                job_worker_mode=JobWorkerMode.STANDALONE,
                jobs=settings.JobSettings(enabled=True),
                state_uri="redis://cache:6379/0",
                events_uri="redis://cache:6379/1",
                lock_uri="postgres-advisory://",
                mail=settings.MailSettings(uri="smtp://relay.test", from_address="a@b.c"),
            )
        )

        assert isinstance(platform.mail, SmtpMail)

    def test_standalone_job_workers_without_jobs_stop_the_build(self, tmp_path):
        with pytest.raises(ValueError, match=r"jobs\.enabled is false"):
            container.build_platform(
                _settings(
                    data_dir=str(tmp_path),
                    job_worker_mode=JobWorkerMode.STANDALONE,
                    state_uri="redis://cache:6379/0",
                    events_uri="redis://cache:6379/1",
                    lock_uri="postgres-advisory://",
                )
            )

    def test_a_consistent_deployment_logs_the_capability_report(self, tmp_path, caplog):
        with caplog.at_level("INFO"):
            container.build_platform(_settings(data_dir=str(tmp_path)))

        assert "JASIL platform capabilities" in caplog.text
        assert "Deployment profile: local" in caplog.text


class TestMailResolution:
    """A mail misconfiguration fails the build; an unset one never drops silently."""

    @pytest.mark.parametrize("uri", ["memory:", " memory://", "memory://\n"])
    def test_a_non_literal_memory_uri_is_refused(self, uri):
        with pytest.raises(ValueError, match="not a valid URI"):
            container._build_mail(_settings(mail=settings.MailSettings(uri=uri)), SystemClock())

    def test_a_multi_process_memory_uri_variant_cannot_slip_past_the_gate(self):
        for uri in ("memory:", "Memory://"):
            s = _settings(web_workers=4, mail=settings.MailSettings(uri=uri))
            try:
                container.build_platform(s)
            except ValueError:
                continue
            raise AssertionError(f"{uri!r} built in a multi-process deployment")

    def test_a_platform_built_by_hand_defaults_geocoding_and_mail(self):
        from jasil.backends.geocoding_http import NullGeocoding

        base = container.build_platform(_settings())
        bare = container.Platform(
            profile=base.profile,
            state=base.state,
            storage=base.storage,
            events=base.events,
            lock=base.lock,
            clock=base.clock,
        )

        assert isinstance(bare.mail, NullMail)
        assert isinstance(bare.geocoding, NullGeocoding)
        assert isinstance(bare.realtime, InProcessRealtime)
        assert bare.recorder is None

    @staticmethod
    def _build(**mail):
        return container._build_mail(_settings(mail=settings.MailSettings(**mail)), SystemClock())

    def test_a_runtime_cap_not_above_the_smtp_deadline_warns(self, caplog):
        """A running job keeps its lease only up to ``max_runtime_seconds``."""
        jobs = settings.JobSettings(enabled=True, lease_seconds=60, max_runtime_seconds=60)
        cfg = _settings(
            mail=settings.MailSettings(uri="smtps://mail.example.com", from_address="a@example.com"), jobs=jobs
        )
        with caplog.at_level("WARNING"):
            container._build_mail(cfg, SystemClock())

        assert "max_runtime_seconds" in caplog.text

    def test_a_longer_runtime_cap_does_not_warn(self, caplog):
        jobs = settings.JobSettings(enabled=True, lease_seconds=60, max_runtime_seconds=1800)
        cfg = _settings(
            mail=settings.MailSettings(uri="smtps://mail.example.com", from_address="a@example.com"), jobs=jobs
        )
        with caplog.at_level("WARNING"):
            container._build_mail(cfg, SystemClock())

        assert "max_runtime_seconds" not in caplog.text

    def test_a_short_lease_alone_does_not_warn(self, caplog):
        """The lease is renewed while a handler runs, so it is no cap on one send."""
        jobs = settings.JobSettings(enabled=True, lease_seconds=60)
        cfg = _settings(
            mail=settings.MailSettings(uri="smtps://mail.example.com", from_address="a@example.com"), jobs=jobs
        )
        with caplog.at_level("WARNING"):
            container._build_mail(cfg, SystemClock())

        assert "jobs." not in caplog.text

    def test_unset_selects_the_disabled_backend(self):
        assert isinstance(self._build(), NullMail)

    def test_memory_selects_the_recording_backend(self):
        assert isinstance(self._build(uri="memory://"), MemoryMail)

    def test_smtp_defaults_to_starttls_on_587(self):
        mail = self._build(uri="smtp://relay.test", from_address="a@example.com")

        assert isinstance(mail, SmtpMail)
        assert (mail._host, mail._port, mail._implicit_tls) == ("relay.test", 587, False)

    def test_smtps_defaults_to_implicit_tls_on_465(self):
        mail = self._build(uri="smtps://relay.test", from_address="a@example.com")

        assert (mail._port, mail._implicit_tls) == (465, True)

    def test_an_explicit_port_wins(self):
        mail = self._build(uri="smtp://relay.test:2525", from_address="a@example.com")

        assert mail._port == 2525

    @pytest.mark.parametrize(
        ("mail", "match"),
        [
            pytest.param({"uri": "http://relay.test"}, "Unsupported mail.uri scheme", id="scheme"),
            pytest.param({"uri": "smtp://u:p@relay.test", "from_address": "a@b.c"}, "credentials", id="userinfo"),
            pytest.param({"uri": "smtp://relay.test/x", "from_address": "a@b.c"}, "no path", id="path"),
            pytest.param({"uri": "smtp://relay.test?x=1", "from_address": "a@b.c"}, "no path", id="query"),
            pytest.param({"uri": "smtp://bad_host!", "from_address": "a@b.c"}, "valid host", id="host"),
            pytest.param({"uri": "smtp://relay.test:abc", "from_address": "a@b.c"}, "not a valid URI", id="port"),
            pytest.param({"uri": "smtp://relay.test:0", "from_address": "a@b.c"}, "invalid port", id="port-zero"),
            pytest.param({"uri": "memory://host"}, "takes no host", id="memory-host"),
            pytest.param({"uri": "smtp://:25", "from_address": "a@b.c"}, "valid host", id="port-only"),
            pytest.param({"uri": "smtp://relay.test"}, "from_address is required", id="no-sender"),
            pytest.param(
                {"uri": "smtp://relay.test", "from_address": "not an address"}, "from_address", id="bad-sender"
            ),
            pytest.param({"uri": "memory://", "from_address": "nope"}, "from_address", id="bad-memory-sender"),
            pytest.param({"uri": "memory://", "secure": False}, "only to smtp://", id="memory-cleartext"),
            pytest.param(
                {"uri": "smtp://relay.test", "from_address": "a@b.c", "ehlo_hostname": "not a host"},
                "ehlo_hostname",
                id="bad-ehlo",
            ),
            pytest.param(
                {"uri": "smtp://relay.test", "from_address": "a@b.c", "username": "u"}, "together", id="user-only"
            ),
            pytest.param(
                {"uri": "smtp://relay.test", "from_address": "a@b.c", "password": "p"}, "together", id="password-only"
            ),
        ],
    )
    def test_a_bad_configuration_fails_the_build(self, mail, match):
        with pytest.raises(ValueError, match=match):
            self._build(**mail)

    @pytest.mark.parametrize("host", ["127.0.0.1", "10.0.0.5:25", "[::1]"])
    def test_an_ip_literal_host_is_accepted(self, host):
        assert isinstance(self._build(uri=f"smtp://{host}", from_address="a@b.c"), SmtpMail)

    def test_cleartext_to_a_loopback_relay_is_allowed_and_warns(self, caplog):
        with caplog.at_level(logging.WARNING):
            mail = self._build(uri="smtp://127.0.0.1:25", from_address="a@b.c", secure=False)

        assert mail._tls is False
        assert "CLEARTEXT" in caplog.text

    @pytest.mark.parametrize(
        ("mail", "match"),
        [
            pytest.param({"uri": "smtp://8.8.8.8"}, "private or loopback", id="public-ip"),
            pytest.param({"uri": "smtps://127.0.0.1"}, "smtps", id="smtps"),
            pytest.param({"uri": "smtp://127.0.0.1", "username": "u", "password": "p"}, "credentials", id="creds"),
        ],
    )
    def test_cleartext_is_refused_outside_its_narrow_case(self, mail, match):
        with pytest.raises(ValueError, match=match):
            self._build(from_address="a@b.c", secure=False, **mail)

    def test_non_ascii_credentials_fail_the_build(self):
        with pytest.raises(ValueError, match="ASCII"):
            self._build(uri="smtp://relay.test", from_address="a@b.c", username="u", password="pässword")

    def test_the_ehlo_name_reaches_the_backend(self):
        mail = self._build(uri="smtp://relay.test", from_address="a@b.c", ehlo_hostname="mta.example.com")

        assert mail._local_hostname == "mta.example.com"

    def test_memory_in_a_multi_process_deployment_fails_the_build(self, tmp_path):
        shared = _settings(data_dir=str(tmp_path), mail=settings.MailSettings(uri="memory://"), web_workers=2)

        with pytest.raises(ValueError, match=r"mail\.uri resolves to the in-memory mail recorder"):
            container.build_platform(shared)

    def test_the_failure_does_not_echo_the_password(self):
        with pytest.raises(ValueError) as caught:
            self._build(uri="smtp://u:hunter2@relay.test", from_address="a@b.c")

        assert "hunter2" not in str(caught.value)

    def test_the_platform_carries_the_provider(self, tmp_path):
        built = container.build_platform(_settings(data_dir=str(tmp_path), mail=settings.MailSettings(uri="memory://")))

        assert isinstance(built.mail, MemoryMail)

    def test_the_password_is_not_in_the_settings_repr(self):
        assert "hunter2" not in repr(settings.MailSettings(username="u", password="hunter2"))
