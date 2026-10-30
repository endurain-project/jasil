"""Settings, correlation ids, and the config slot they are both built on."""

import dataclasses

import pytest

import jasil.correlation as correlation
import jasil.settings as settings
from jasil._core.registry import ConfigSlot
from jasil.profile import DeploymentProfile, JobWorkerMode


class TestConfigSlot:
    def test_a_required_slot_raises_until_configured(self):
        slot: ConfigSlot[str] = ConfigSlot(missing_message="not set up")

        assert slot.is_configured() is False
        with pytest.raises(RuntimeError, match="not set up"):
            slot.get()

    def test_a_required_slot_returns_the_configured_value(self):
        slot: ConfigSlot[str] = ConfigSlot(missing_message="not set up")

        slot.configure("value")

        assert slot.get() == "value"
        assert slot.is_configured() is True

    def test_resetting_a_required_slot_clears_it(self):
        slot: ConfigSlot[str] = ConfigSlot(missing_message="not set up")
        slot.configure("value")

        slot.reset()

        assert slot.is_configured() is False

    def test_a_defaulted_slot_never_raises(self):
        slot: ConfigSlot[list[int]] = ConfigSlot(default_factory=list)

        assert slot.get() == []

    def test_a_defaulted_slot_reports_unconfigured_until_configured(self):
        """Holding a default is not the same as the host having chosen it."""
        slot: ConfigSlot[list[int]] = ConfigSlot(default_factory=list)

        assert slot.is_configured() is False

        slot.configure([1])

        assert slot.is_configured() is True

    def test_resetting_a_defaulted_slot_rebuilds_a_fresh_default(self):
        slot: ConfigSlot[list[int]] = ConfigSlot(default_factory=list)
        slot.get().append(1)

        slot.reset()

        assert slot.get() == []
        assert slot.is_configured() is False


class TestCapabilityUriResolution:
    def test_the_local_profile_defaults_every_capability(self):
        resolved = settings.JasilSettings(profile=DeploymentProfile.LOCAL)

        assert resolved.resolved_state_uri == "memory://"
        assert resolved.resolved_storage_uri == "local://"
        assert resolved.resolved_events_uri == "memory://"
        assert resolved.resolved_lock_uri == "noop://"
        assert resolved.resolved_realtime_uri == "memory://"

    def test_an_explicit_uri_wins_over_the_profile_default(self):
        resolved = settings.JasilSettings(profile=DeploymentProfile.LOCAL, state_uri="redis://cache:6379/0")

        assert resolved.resolved_state_uri == "redis://cache:6379/0"

    @pytest.mark.parametrize("profile", [DeploymentProfile.DISTRIBUTED, DeploymentProfile.CUSTOM])
    @pytest.mark.parametrize("capability", ["state", "storage", "events", "lock"])
    def test_a_non_local_profile_refuses_to_guess(self, profile, capability):
        """Silently defaulting to a process-local backend across replicas is the
        exact failure the profile system exists to prevent, so this raises."""
        resolved = settings.JasilSettings(profile=profile)

        with pytest.raises(ValueError, match=f"{capability}_uri must be set explicitly"):
            getattr(resolved, f"resolved_{capability}_uri")

    def test_a_non_local_profile_accepts_explicit_uris(self):
        resolved = settings.JasilSettings(
            profile=DeploymentProfile.DISTRIBUTED,
            state_uri="redis://cache:6379/0",
            storage_uri="s3://bucket",
            events_uri="redis://cache:6379/1",
            lock_uri="postgres-advisory://",
        )

        assert resolved.resolved_state_uri == "redis://cache:6379/0"
        assert resolved.resolved_storage_uri == "s3://bucket"
        assert resolved.resolved_events_uri == "redis://cache:6379/1"
        assert resolved.resolved_lock_uri == "postgres-advisory://"

    @pytest.mark.parametrize("profile", list(DeploymentProfile))
    def test_an_unset_realtime_uri_follows_the_event_bus(self, profile):
        """A deployment configured before ``realtime_uri`` existed must keep building."""
        resolved = settings.JasilSettings(profile=profile, events_uri="redis://cache:6379/1")

        assert resolved.resolved_realtime_uri == "redis://cache:6379/1"

    @pytest.mark.parametrize("profile", [DeploymentProfile.DISTRIBUTED, DeploymentProfile.CUSTOM])
    def test_the_realtime_fallback_still_refuses_to_guess_without_events(self, profile):
        with pytest.raises(ValueError, match="events_uri must be set explicitly"):
            _ = settings.JasilSettings(profile=profile).resolved_realtime_uri

    def test_an_explicit_realtime_uri_wins_over_the_fallback(self):
        resolved = settings.JasilSettings(
            profile=DeploymentProfile.DISTRIBUTED, events_uri="redis://cache:6379/1", realtime_uri="redis://hints:6379"
        )

        assert resolved.resolved_realtime_uri == "redis://hints:6379"


class TestProfileValues:
    """Every rule compares by identity, so a plain string must not slip through as a profile nobody matches."""

    def test_a_profile_given_as_its_name_is_the_profile(self):
        assert settings.JasilSettings(profile="distributed").profile is DeploymentProfile.DISTRIBUTED

    def test_a_worker_mode_given_as_its_name_is_the_mode(self):
        assert settings.JasilSettings(job_worker_mode="standalone").job_worker_mode is JobWorkerMode.STANDALONE

    @pytest.mark.parametrize("field", ["profile", "job_worker_mode"])
    def test_a_misspelt_value_is_refused(self, field):
        with pytest.raises(ValueError, match="Valid values"):
            settings.JasilSettings(**{field: "distributd"})


class TestSettingsInstallation:
    def test_get_settings_returns_defaults_before_configure(self):
        """Settings are optional: a host running the local profile configures nothing."""
        assert settings.is_configured() is False
        assert settings.get_settings().profile is DeploymentProfile.LOCAL

    def test_configure_installs_the_hosts_settings(self):
        settings.configure(settings.JasilSettings(data_dir="/srv/data"))

        assert settings.get_settings().data_dir == "/srv/data"
        assert settings.is_configured() is True

    def test_reset_restores_the_defaults(self):
        settings.configure(settings.JasilSettings(data_dir="/srv/data"))

        settings.reset()

        assert settings.get_settings().data_dir == "data"

    def test_settings_are_immutable(self):
        """Frozen so a component cannot mutate configuration another already read."""
        with pytest.raises(dataclasses.FrozenInstanceError):
            settings.get_settings().data_dir = "/srv/elsewhere"  # type: ignore[misc]

    def test_grouped_defaults_are_not_shared_between_instances(self):
        """A mutable default would let one instance's group leak into another's."""
        first = settings.JasilSettings()
        second = settings.JasilSettings()

        assert first.jobs is not second.jobs
        assert first.jobs == second.jobs

    def test_durable_jobs_and_the_event_log_are_off_by_default(self):
        """Both write to the database, so neither may switch itself on."""
        defaults = settings.JasilSettings()

        assert defaults.jobs.enabled is False
        assert defaults.event_log.enabled is False


class TestJobSettingsValidation:
    """Values the job pipeline cannot honour fail where the host configures JASIL, not when a worker starts."""

    def test_the_defaults_are_valid(self):
        settings.JobSettings()

    @pytest.mark.parametrize(
        ("values", "message"),
        [
            pytest.param({"batch_size": 0}, r"jobs\.batch_size must be at least 1", id="no-batch"),
            pytest.param({"batch_size": 1001}, r"jobs\.batch_size must be at most 1000", id="huge-batch"),
            pytest.param(
                {"poll_interval_seconds": 0}, r"poll_interval_seconds must be greater than zero", id="hot-poll"
            ),
            pytest.param({"max_attempts": 0}, r"jobs\.max_attempts must be at least 1", id="no-attempts"),
            pytest.param({"backoff_base_seconds": -1}, r"backoff_base_seconds must be at least 0", id="negative-base"),
            pytest.param(
                {"backoff_base_seconds": 120, "backoff_max_seconds": 60},
                r"backoff_max_seconds \(60\) must not be below jobs\.backoff_base_seconds \(120\)",
                id="ceiling-below-base",
            ),
            pytest.param({"defer_max_seconds": -1}, r"defer_max_seconds must be at least 0", id="negative-defer"),
            pytest.param({"max_deferrals": -1}, r"max_deferrals must be at least 0", id="negative-deferrals"),
            pytest.param(
                {"lease_seconds": 44, "heartbeat_interval_seconds": 15},
                r"at least three heartbeat intervals \(45 s",
                id="lease-below-three-beats",
            ),
            pytest.param(
                {"max_runtime_seconds": 299},
                r"max_runtime_seconds \(299\) must not be below jobs.lease_seconds \(300\)",
                id="cap-below-lease",
            ),
            pytest.param(
                {"heartbeat_interval_seconds": 0},
                r"heartbeat_interval_seconds must be greater than zero",
                id="no-interval",
            ),
            pytest.param({"poll_interval_seconds": float("nan")}, r"poll_interval_seconds must be", id="nan-poll"),
            pytest.param(
                {"heartbeat_interval_seconds": float("inf")}, r"heartbeat_interval_seconds must be", id="inf-beat"
            ),
            pytest.param({"lease_seconds": 31_536_001}, r"jobs\.lease_seconds must be at most", id="endless-lease"),
            pytest.param(
                {"lease_seconds": 31_536_000, "max_runtime_seconds": 31_536_001},
                r"jobs\.max_runtime_seconds must be at most",
                id="endless-runtime",
            ),
            pytest.param(
                {"backoff_base_seconds": 31_536_001, "backoff_max_seconds": 31_536_001},
                r"jobs\.backoff_base_seconds must be at most",
                id="endless-backoff-base",
            ),
            pytest.param(
                {"backoff_max_seconds": 31_536_001}, r"jobs\.backoff_max_seconds must be at most", id="endless-backoff"
            ),
            pytest.param(
                {"defer_max_seconds": 31_536_001}, r"jobs\.defer_max_seconds must be at most", id="endless-deferral"
            ),
            pytest.param(
                {"retention_days": 36_501}, r"jobs\.retention_days must be at most 36500", id="huge-retention"
            ),
        ],
    )
    def test_a_value_the_pipeline_cannot_honour_is_refused(self, values, message):
        with pytest.raises(ValueError, match=message):
            settings.JobSettings(**values)

    def test_the_bounds_themselves_are_accepted(self):
        settings.JobSettings(
            batch_size=1000,
            max_attempts=1,
            backoff_base_seconds=0,
            backoff_max_seconds=0,
            defer_max_seconds=0,
            max_deferrals=0,
            lease_seconds=45,
            heartbeat_interval_seconds=15,
            max_runtime_seconds=45,
        )


class TestEventLogSettingsValidation:
    def test_the_defaults_are_valid(self):
        settings.EventLogSettings()

    def test_retention_beyond_a_representable_cutoff_is_refused(self):
        with pytest.raises(ValueError, match=r"event_log\.retention_days must be at most 36500"):
            settings.EventLogSettings(retention_days=36_501)

    @pytest.mark.parametrize("days", [-1, 0, 36_500])
    def test_disabling_and_the_ceiling_are_accepted(self, days):
        settings.EventLogSettings(retention_days=days)


class TestCorrelationId:
    def test_there_is_no_correlation_id_by_default(self):
        assert correlation.get_correlation_id() is None

    def test_the_context_var_is_used_when_no_provider_is_installed(self):
        correlation.set_correlation_id("req-1")

        assert correlation.get_correlation_id() == "req-1"

    def test_an_installed_provider_takes_precedence(self):
        correlation.set_correlation_id("from-contextvar")

        correlation.configure_provider(lambda: "from-host")

        assert correlation.get_correlation_id() == "from-host"

    def test_a_failing_provider_yields_no_id_rather_than_raising(self):
        """A correlation id is diagnostic metadata; it must never break publishing."""

        def _broken() -> str:
            raise RuntimeError("middleware exploded")

        correlation.configure_provider(_broken)

        assert correlation.get_correlation_id() is None

    def test_clearing_the_provider_restores_the_context_var(self):
        correlation.set_correlation_id("req-1")
        correlation.configure_provider(lambda: "from-host")

        correlation.configure_provider(None)

        assert correlation.get_correlation_id() == "req-1"

    def test_reset_clears_both_the_provider_and_the_context_var(self):
        correlation.set_correlation_id("req-1")
        correlation.configure_provider(lambda: "from-host")

        correlation.reset()

        assert correlation.get_correlation_id() is None
