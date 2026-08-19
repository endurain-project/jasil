"""Host-supplied configuration for JASIL.

JASIL never reads environment variables or secret files itself. The host builds a
:class:`JasilSettings` from whatever configuration source it likes and installs
it once at startup::

    import jasil.settings as jasil_settings

    jasil_settings.configure(
        jasil_settings.JasilSettings(
            profile=jasil.DeploymentProfile.DISTRIBUTED,
            state_uri="redis://cache:6379/0",
            storage_uri="s3://bucket",
            events_uri="redis://cache:6379/1",
        )
    )

Every component reads the installed settings through :func:`get_settings`.

**Shape.** Configuration is grouped by concern — :class:`JobSettings`,
:class:`EventLogSettings`, :class:`GeocodingSettings`, :class:`NetworkSettings` —
rather than one flat list, so a host enabling durable jobs reads one small class.
Only the values describing the *deployment shape* sit at the top level.

**Profile defaults.** The four capability URIs may be left unset, in which case
the deployment profile supplies the default: ``local`` resolves to the
single-process backends, while ``distributed`` and ``custom`` refuse to guess and
raise, because a Redis host or bucket name cannot be inferred.
"""

from dataclasses import dataclass, field

from jasil._core.registry import ConfigSlot
from jasil.profile import DeploymentProfile

__all__ = [
    "EventLogSettings",
    "GeocodingSettings",
    "JasilSettings",
    "JobSettings",
    "NetworkSettings",
    "configure",
    "get_settings",
    "is_configured",
    "reset",
]


@dataclass(frozen=True)
class JobSettings:
    """Durable-job pipeline configuration.

    Attributes:
        enabled: Route events through the transactional outbox instead of the
            event bus. When false the whole jobs layer stays dormant.
        lease_seconds: How long a claimed job is leased to a worker before the
            reaper may reclaim it.
        batch_size: Maximum rows claimed or relayed per pass.
        backoff_base_seconds: First retry delay; doubles per attempt.
        backoff_max_seconds: Ceiling for the exponential backoff.
        poll_interval_seconds: Idle wait between empty polls.
        max_attempts: Attempts before a job is dead-lettered.
        retention_days: Age at which relayed outbox rows and completed jobs are
            pruned. ``<= 0`` disables pruning.
    """

    enabled: bool = False
    lease_seconds: int = 300
    batch_size: int = 20
    backoff_base_seconds: int = 60
    backoff_max_seconds: int = 3600
    poll_interval_seconds: float = 5.0
    max_attempts: int = 5
    retention_days: int = 30


@dataclass(frozen=True)
class EventLogSettings:
    """Event-observability trail configuration.

    Attributes:
        enabled: Record every event's lifecycle to the ``event_log`` table.
        retention_days: Age at which trail rows are pruned. ``<= 0`` disables
            pruning.
    """

    enabled: bool = False
    retention_days: int = 30


@dataclass(frozen=True)
class GeocodingSettings:
    """Reverse-geocoding backend configuration.

    Attributes:
        provider: ``"nominatim"``, ``"photon"``, or ``"geocode"``. Any other
            value (including the empty default) disables the capability.
        rate_limit: Maximum requests per second; ``<= 0`` disables throttling.
        api_key: API key, for the services requiring one (geocode.maps.co).
        nominatim_host: Bare ``host[:port]`` authority for Nominatim.
        nominatim_use_https: Address Nominatim over HTTPS.
        photon_host: Bare ``host[:port]`` authority for Photon.
        photon_use_https: Address Photon over HTTPS.
        user_agent: ``User-Agent`` sent upstream. Nominatim's usage policy
            requires an identifying value, so hosts should set their own.
    """

    provider: str = ""
    rate_limit: float = 1.0
    api_key: str | None = None
    nominatim_host: str = ""
    nominatim_use_https: bool = True
    photon_host: str = ""
    photon_use_https: bool = True
    user_agent: str = "jasil (ReverseGeocoding)"


@dataclass(frozen=True)
class NetworkSettings:
    """Outbound-egress configuration.

    Attributes:
        ssrf_allowed_hosts: Hostnames and CIDRs exempt from the SSRF address
            denylist, so a self-hosted service on a private network stays
            reachable. Every use is logged. Prefer a CIDR: a *hostname* entry
            exempts every address that name resolves to, including a cloud
            metadata endpoint.
    """

    ssrf_allowed_hosts: tuple[str, ...] = ()


# Capability defaults for the single-process profile. The distributed and custom
# profiles have no equivalent: a Redis host, bucket name, or DSN cannot be
# guessed, so leaving one unset there is a configuration error rather than a
# silent fallback to a process-local backend.
_LOCAL_DEFAULT_URIS = {
    "state_uri": "memory://",
    "storage_uri": "local://",
    "events_uri": "memory://",
    "lock_uri": "noop://",
}


@dataclass(frozen=True)
class JasilSettings:
    """The full JASIL configuration.

    Attributes:
        profile: The deployment shape; supplies the capability-URI defaults.
        web_workers: How many web-server worker processes the host runs. Only the
            *count* matters: four workers under the ``local`` profile are still
            four processes, and process-local state cannot be shared between them,
            so this drives the consistency checks as much as the profile does.
        enforce_deployment_consistency: Refuse to build a platform whose wiring
            contradicts its topology (see :mod:`jasil.capabilities`). Set False to
            log the issues as warnings instead — useful on a development machine
            running the distributed profile without Redis.
        data_dir: Root directory for the local storage backend.
        state_uri: ``memory://`` or ``redis://`` / ``rediss://`` / ``unix://``.
        storage_uri: ``local://`` or ``s3://``.
        events_uri: ``memory://`` or ``redis://`` / ``rediss://`` / ``unix://``.
        lock_uri: ``noop://`` or ``postgres-advisory://``.
        jobs: Durable-job pipeline configuration.
        event_log: Event-observability trail configuration.
        geocoding: Reverse-geocoding backend configuration.
        network: Outbound-egress configuration.
    """

    profile: DeploymentProfile = DeploymentProfile.LOCAL
    web_workers: int = 1
    enforce_deployment_consistency: bool = True
    data_dir: str = "data"
    state_uri: str | None = None
    storage_uri: str | None = None
    events_uri: str | None = None
    lock_uri: str | None = None
    jobs: JobSettings = field(default_factory=JobSettings)
    event_log: EventLogSettings = field(default_factory=EventLogSettings)
    geocoding: GeocodingSettings = field(default_factory=GeocodingSettings)
    network: NetworkSettings = field(default_factory=NetworkSettings)

    def _resolve(self, name: str) -> str:
        """Return an explicit capability URI, or the profile's default.

        Raises:
            ValueError: When the URI is unset and the profile has no default.
        """
        configured = getattr(self, name)
        if configured:
            return str(configured)
        if self.profile is DeploymentProfile.LOCAL:
            return _LOCAL_DEFAULT_URIS[name]
        raise ValueError(
            f"{name} must be set explicitly for the {self.profile.value!r} deployment profile; "
            f"only the 'local' profile has a default ({_LOCAL_DEFAULT_URIS[name]})."
        )

    @property
    def resolved_state_uri(self) -> str:
        """The effective state-backend URI."""
        return self._resolve("state_uri")

    @property
    def resolved_storage_uri(self) -> str:
        """The effective storage-backend URI."""
        return self._resolve("storage_uri")

    @property
    def resolved_events_uri(self) -> str:
        """The effective event-bus URI."""
        return self._resolve("events_uri")

    @property
    def resolved_lock_uri(self) -> str:
        """The effective lock-backend URI."""
        return self._resolve("lock_uri")


_settings: ConfigSlot[JasilSettings] = ConfigSlot(default_factory=JasilSettings)


def configure(settings: JasilSettings) -> None:
    """Install the host's settings for the process.

    Args:
        settings: The configuration to install.
    """
    _settings.configure(settings)


def get_settings() -> JasilSettings:
    """Return the installed settings, or the all-defaults instance."""
    return _settings.get()


def is_configured() -> bool:
    """Return whether :func:`configure` has been called."""
    return _settings.is_configured()


def reset() -> None:
    """Restore the all-defaults settings.

    For tests; production code configures once at startup and never resets.
    """
    _settings.reset()
