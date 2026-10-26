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
:class:`EventLogSettings`, :class:`GeocodingSettings`, :class:`MailSettings`,
:class:`NetworkSettings`, :class:`StorageSettings` —
rather than one flat list, so a host enabling durable jobs reads one small class.
Only the values describing the *deployment shape* sit at the top level.

**Profile defaults.** The capability URIs may be left unset, in which case
the deployment profile supplies the default: ``local`` resolves to the
single-process backends, while ``distributed`` and ``custom`` refuse to guess and
raise, because a Redis host or bucket name cannot be inferred. The one exception
is ``realtime_uri``, which follows ``events_uri`` in every profile when unset.
"""

from collections.abc import Callable
from dataclasses import dataclass, field

from jasil._core.mail_problems import MailProblem, MailProblemCode, mail_problems
from jasil._core.registry import ConfigSlot
from jasil.profile import DeploymentProfile, JobWorkerMode

__all__ = [
    "EventLogSettings",
    "GeocodingSettings",
    "JasilSettings",
    "JobSettings",
    "MailProblem",
    "MailProblemCode",
    "MailSettings",
    "NetworkSettings",
    "RuntimeSources",
    "StorageSettings",
    "configure",
    "get_settings",
    "is_configured",
    "mail_problems",
    "reset",
]


@dataclass(frozen=True)
class JobSettings:
    """Durable-job pipeline configuration.

    Attributes:
        enabled: Route events through the transactional outbox instead of the
            event bus. When false the whole jobs layer stays dormant.
        lease_seconds: How long a worker may go silent before the reaper may
            reclaim the jobs it holds. A running worker renews its leases on every
            heartbeat, so this is *not* a cap on handler runtime (see
            ``max_runtime_seconds``). Must be at least three
            ``heartbeat_interval_seconds``.
        batch_size: Maximum rows claimed or relayed per pass.
        backoff_base_seconds: First retry delay; doubles per attempt.
        backoff_max_seconds: Ceiling for the exponential backoff.
        defer_max_seconds: Furthest ahead a handler may defer itself with
            ``DeferJob``; a later ``retry_at`` is clamped to it.
        max_deferrals: Deferrals a job may take before a further ``DeferJob``
            is treated as an ordinary failure.
        poll_interval_seconds: Idle wait between empty polls.
        max_runtime_seconds: The longest a single handler is kept leased. Past it
            the job is abandoned to the reaper, the handler is told to stop
            (``jasil.jobs.lease_lost``), and the rest of the worker's batch is
            released to run elsewhere. Must not be below ``lease_seconds``.
        record_attempts: Write one ``job_attempts`` row per attempt to run a job:
            which worker ran it, when, for how long, and how it ended. On by
            default; ``False`` skips every attempt write (job processing is
            unaffected) and the attempt-derived admin fields read ``None``.
        heartbeat_interval_seconds: Interval between worker heartbeats, each of
            which renews held leases and refreshes the worker-registry row.
        max_attempts: Attempts before a job is dead-lettered.
        retention_days: Age at which relayed outbox rows, completed jobs, their
            attempt history, and stopped and stale worker rows are pruned. A job
            that is pending, claimed or dead-lettered keeps its whole history.
            ``<= 0`` disables pruning.
    """

    enabled: bool = False
    lease_seconds: int = 300
    max_runtime_seconds: int = 1800
    record_attempts: bool = True
    batch_size: int = 20
    backoff_base_seconds: int = 60
    backoff_max_seconds: int = 3600
    defer_max_seconds: int = 86400
    max_deferrals: int = 100
    poll_interval_seconds: float = 5.0
    heartbeat_interval_seconds: float = 15.0
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
        uri: ``nominatim://host[:port]`` or ``photon://host[:port]`` (HTTPS),
            ``nominatim+http://host[:port]`` or ``photon+http://host[:port]``
            (cleartext, for a self-hosted instance on a private network), or
            ``geocode://`` (geocode.maps.co, a fixed host; needs ``api_key``).
            The host must be a bare authority — no userinfo, path, query or
            fragment — and passes SSRF validation (see
            :class:`NetworkSettings`). Unset disables the capability; so does
            any value that cannot be used, which is logged. Geocoding never
            fails the platform build.
        rate_limit: Maximum requests per second; ``<= 0`` disables throttling.
        api_key: API key, for the services requiring one (geocode.maps.co).
        user_agent: ``User-Agent`` sent upstream. Nominatim's usage policy
            requires an identifying value, so hosts should set their own.
    """

    uri: str | None = None
    rate_limit: float = 1.0
    api_key: str | None = field(default=None, repr=False)
    user_agent: str = "jasil (ReverseGeocoding)"


@dataclass(frozen=True)
class MailSettings:
    """Outbound-mail backend configuration.

    Attributes:
        uri: ``memory://`` (tests), ``smtp://host[:port]`` (STARTTLS, default
            port 587) or ``smtps://host[:port]`` (implicit TLS, default port
            465). Unset disables mail: sending raises
            :class:`~jasil.providers.MailNotConfiguredError`. Credentials never
            belong in the URI. The host is trusted operator configuration and
            is not checked against the SSRF address denylist (a relay is
            normally on a private network): never derive it from user input.
            ``memory://`` is refused in a multi-process deployment (see
            ``enforce_deployment_consistency``), where no message would ever be
            delivered.
        from_address: Sender identity, ``"addr"`` or ``"Name <addr>"``. Required
            for the SMTP schemes. Independent of ``username``.
        secure: ``True`` (the default) requires verified TLS. ``False`` sends
            over a cleartext connection and is accepted only for ``smtp://``,
            only when the host resolves exclusively to private, loopback or
            link-local addresses (re-checked on every send), and never together with ``username`` / ``password``.
            That check resolves the host while the platform is built, so it
            needs a working resolver at startup.
        username: SMTP login, when the server requires authentication.
        password: SMTP password. Required with ``username``, and the reverse.
        ehlo_hostname: Name announced in ``EHLO``. Defaults to this machine's
            hostname; set a fully qualified one for a relay that rejects a bare
            name.
    """

    uri: str | None = None
    from_address: str = ""
    secure: bool = True
    username: str | None = None
    password: str | None = field(default=None, repr=False)
    ehlo_hostname: str | None = None


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


@dataclass(frozen=True)
class StorageSettings:
    """Settings of the ``local://`` storage backend; ``s3://`` takes none.

    Attributes:
        local_url_prefix: URL path prefix of the links ``local://`` builds, for a
            host that serves the data directory under a path such as ``/media``.
            Empty serves from the root. Ignored for ``s3://``.
        local_legacy_layout: Also read, list and delete objects in the flat
            ``{data_dir}/{area}/{key}`` layout written by JASIL 0.3 and earlier
            and by the application JASIL was extracted from. Off by default, so
            no other file under ``data_dir`` can be served as an object. Ignored
            for ``s3://``.
    """

    local_url_prefix: str = ""
    local_legacy_layout: bool = False


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
        job_worker_mode: Where durable-job consumers run: ``in_process`` (each API
            process calls ``start_job_worker``) or ``standalone`` (dedicated
            processes call ``run_job_worker``). Like ``web_workers`` it describes
            the deployment's shape, and **every process of the deployment must
            load the same value**: a standalone worker is one more process that
            reaches every capability, and the consistency checks can only see it
            if the API processes know about it too.
        enforce_deployment_consistency: Refuse to build a platform whose wiring
            contradicts its topology (see :mod:`jasil.capabilities`). Set False to
            log the issues as warnings instead — useful on a development machine
            running the distributed profile without Redis.
        data_dir: Root directory for the local storage backend.
        state_uri: ``memory://`` or ``redis://`` / ``rediss://`` / ``unix://``.
        storage_uri: ``local://`` or ``s3://``.
        events_uri: ``memory://`` or ``redis://`` / ``rediss://`` / ``unix://``.
        lock_uri: ``noop://`` or ``postgres-advisory://``.
        realtime_uri: ``memory://`` or ``redis://`` / ``rediss://`` / ``unix://``
            — the channel that carries keyed realtime hints to every process
            (see :class:`~jasil.providers.RealtimeProvider`). Unset, it follows
            the resolved ``events_uri`` in every profile — ``memory://`` under
            the ``local`` defaults, the event bus's Redis otherwise — because
            both need the same reach across processes, and a deployment
            upgrading from a release without this setting must keep building.
            Set it to put hints on a different Redis.
        events_stream: The Redis Stream key carrying best-effort events. Streams
            live in one Redis database, so two deployments sharing a database must
            use different streams (and consumer groups) or they would consume each
            other's events. Ignored for ``memory://``. Must not be empty.
        events_group: The Redis consumer group the event bus reads as. Replicas of
            one deployment share a group, so each event is handled once; two
            deployments reading the same stream must not. Ignored for
            ``memory://``. Must not be empty.
        realtime_channel: The Redis Pub/Sub channel carrying realtime hints.
            Pub/Sub channels are server-wide — the database number in the URI
            does not separate them — so two deployments sharing one Redis server
            must use different channels. Ignored for ``memory://``.
        jobs: Durable-job pipeline configuration.
        event_log: Event-observability trail configuration.
        geocoding: Reverse-geocoding backend configuration.
        mail: Outbound-mail backend configuration.
        network: Outbound-egress configuration.
        storage: Settings of the ``local://`` storage backend.
    """

    profile: DeploymentProfile = DeploymentProfile.LOCAL
    web_workers: int = 1
    job_worker_mode: JobWorkerMode = JobWorkerMode.IN_PROCESS
    enforce_deployment_consistency: bool = True
    data_dir: str = "data"
    state_uri: str | None = None
    storage_uri: str | None = None
    events_uri: str | None = None
    lock_uri: str | None = None
    realtime_uri: str | None = None
    realtime_channel: str = "jasil:realtime"
    events_stream: str = "jasil:events"
    events_group: str = "jasil"
    jobs: JobSettings = field(default_factory=JobSettings)
    event_log: EventLogSettings = field(default_factory=EventLogSettings)
    geocoding: GeocodingSettings = field(default_factory=GeocodingSettings)
    mail: MailSettings = field(default_factory=MailSettings)
    network: NetworkSettings = field(default_factory=NetworkSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)

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

    @property
    def resolved_realtime_uri(self) -> str:
        """The effective realtime-hint URI: ``realtime_uri``, else the resolved ``events_uri``.

        Raises:
            ValueError: When both are unset and the profile has no default.
        """
        return self.realtime_uri or self.resolved_events_uri


_settings: ConfigSlot[JasilSettings] = ConfigSlot(default_factory=JasilSettings)


@dataclass(frozen=True)
class RuntimeSources:
    """Callables returning the current settings of capabilities the host edits at runtime.

    Passed to :func:`jasil.container.build_platform`. For a capability with a
    source, :attr:`JasilSettings.mail` / :attr:`JasilSettings.geocoding` are
    **ignored**: the platform asks the source instead and rebuilds the backend
    when the returned settings change.

    Each callable is invoked on every use of the provider (every ``send``, every
    ``reverse``), outside any lock, and may raise. Making that cheap — a
    process-local cache, a version stamp — is the host's job; JASIL has no
    refresh interval.

    Attributes:
        mail: Returns the current :class:`MailSettings`.
        geocoding: Returns the current :class:`GeocodingSettings`.
    """

    mail: Callable[[], MailSettings] | None = None
    geocoding: Callable[[], GeocodingSettings] | None = None


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
