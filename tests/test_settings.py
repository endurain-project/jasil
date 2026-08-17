"""Settings, correlation ids, and the config slot they are both built on."""

import pytest

import jasil.correlation as correlation
import jasil.settings as settings
from jasil._core.registry import ConfigSlot
from jasil.profile import DeploymentProfile


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
        with pytest.raises(Exception, match=r"frozen|immutable|cannot assign"):
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
