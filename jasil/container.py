"""The composition root: build the platform substrate from settings.

``build_platform`` resolves each capability (state, storage, events, lock,
clock) to a concrete backend based on the deployment profile and returns a
frozen ``Platform`` holding the providers. It is called once at startup and
published process-wide via ``jasil.runtime`` so both request and non-request
code resolve the same instance.

Every capability resolves its backend by URI scheme, independently of the
profile: ``memory``/``redis`` for the state URI; ``local``/``s3`` for the storage
URI; ``memory``/``redis`` for the events URI; ``noop``/``postgres-advisory`` for
the lock URI. The deployment profile only shapes the *defaults* those URIs
resolve to (see the ``resolved_*`` properties on
:class:`~jasil.settings.JasilSettings`), so ``local``, ``distributed``, and
``custom`` all build the same way — the profile just picks memory-vs-Redis and
local-fs-vs-S3 defaults.

Before anything is constructed, the resolved wiring is checked against the
deployment topology (see :mod:`jasil.capabilities`): a combination that would
silently diverge across processes or nodes stops the build rather than becoming a
production mystery.

The *selection* rules — the topology check, the scheme-to-backend mapping, the
error wording, and the whole "is geocoding configured, and is its host allowed"
decision — are shared with the async composition root in
:mod:`jasil.container_async` rather than restated there. Only the construction
differs: one root instantiates the synchronous backends, the other the async
ones. A second copy of the URI rules would be a deployment that behaves
differently depending on which face the host built, which is precisely the class
of surprise this module exists to prevent.
"""

import logging
from dataclasses import dataclass

import jasil._core.redis_clients as redis_clients
import jasil.capabilities as capabilities
from jasil.backends.clock_system import SystemClock
from jasil.backends.events_inprocess import InProcessEventBus
from jasil.backends.events_redis import RedisStreamEventBus
from jasil.backends.geocoding_http import HttpGeocoding, NullGeocoding, build_reverse_endpoint
from jasil.backends.lock_noop import NoopLock
from jasil.backends.lock_pg import PgAdvisoryLock
from jasil.backends.state_memory import MemoryState
from jasil.backends.state_redis import RedisState
from jasil.backends.storage_local import LocalStorage
from jasil.profile import DeploymentProfile
from jasil.providers import (
    ClockProvider,
    EventBusProvider,
    EventRecorder,
    GeocodingProvider,
    LockProvider,
    StateProvider,
    StorageProvider,
)
from jasil.settings import JasilSettings, get_settings

logger = logging.getLogger(__name__)

#: Reverse-geocoding services this build knows how to talk to.
_GEOCODING_SERVICES = ("nominatim", "photon", "geocode")

#: URI schemes that select a Redis-backed capability.
REDIS_SCHEMES = ("redis", "rediss", "unix")


@dataclass(frozen=True)
class GeocodingChoice:
    """A validated reverse-geocoding configuration, ready to construct a backend from.

    Produced by :func:`resolve_geocoding`, which does all the deciding and
    logging; both composition roots then just build their own backend class from
    it. ``None`` from that function means "use the no-op backend", so the absence
    of a choice is itself the answer and neither root has to re-derive why.

    Attributes:
        service: The upstream to call — ``"nominatim"``, ``"photon"`` or ``"geocode"``.
        base_url: The validated, fully-qualified reverse endpoint.
        api_key: API key, for the services that require one.
        min_interval_seconds: Minimum wall-clock gap between requests.
        user_agent: ``User-Agent`` to send upstream.
    """

    service: str
    base_url: str
    api_key: str | None
    min_interval_seconds: float
    user_agent: str


def unsupported_scheme_error(setting_name: str, uri: str) -> ValueError:
    """Build the error raised when a capability URI names a scheme no backend claims.

    Shared by both composition roots so a misconfigured URI reads the same
    whichever face the host is building.

    Args:
        setting_name: The setting the URI came from, e.g. ``"state_uri"``.
        uri: The offending URI.

    Returns:
        The ``ValueError`` to raise.
    """
    scheme, _, _ = uri.partition("://")
    return ValueError(f"Unsupported {setting_name} scheme: {scheme or uri!r}")


def check_deployment_consistency(settings: JasilSettings) -> None:
    """Stop a build whose wiring contradicts its topology, unless the host opted out.

    Checked before any backend is constructed, so the failure names the setting
    rather than surfacing later as a connection error — or, worse, as a
    deployment that starts happily and diverges across replicas.

    Args:
        settings: The configuration being built from.

    Raises:
        ValueError: When the wiring is inconsistent and enforcement is on.
    """
    issues = capabilities.check_deployment_consistency(settings)
    if not issues:
        return
    if not settings.enforce_deployment_consistency:
        for issue in issues:
            logger.warning("Inconsistent deployment wiring: %s", issue)
        return
    detail = "\n  - ".join(issues)
    raise ValueError(
        f"JASIL's deployment wiring is inconsistent:\n  - {detail}\n"
        "Fix the setting, or pass enforce_deployment_consistency=False to downgrade this to a warning."
    )


def resolve_geocoding(settings: JasilSettings) -> GeocodingChoice | None:
    """Decide whether reverse geocoding is usable, and with what configuration.

    Unlike the other capabilities this never raises on a bad configuration:
    geocoding is optional enrichment, so an unsupported provider, an unset API
    key, or a host that fails SSRF validation disables the capability rather than
    preventing the application from starting.

    Because a disabled capability is otherwise invisible — lookups simply return
    nothing and nothing says why — every outcome is logged at startup, including
    the successful one. An operator who set the config and saw no locations appear
    can tell from one line whether the setting was rejected and for what reason.

    Args:
        settings: The application settings.

    Returns:
        The validated choice, or ``None`` when geocoding is unconfigured or
        misconfigured and the caller should use its no-op backend.
    """
    geo = settings.geocoding
    min_interval = 1.0 / geo.rate_limit if geo.rate_limit > 0 else 0.0
    provider = geo.provider

    if not provider:
        # Unset means "not wanted", not "misconfigured" — so this stays quiet.
        logger.debug("Reverse geocoding is not configured; using the no-op backend")
        return None

    if provider not in _GEOCODING_SERVICES:
        logger.warning(
            "geocoding.provider %r is not a supported service (expected one of: %s); reverse geocoding is disabled",
            provider,
            ", ".join(_GEOCODING_SERVICES),
        )
        return None

    if provider == "geocode":
        if not geo.api_key:
            logger.warning(
                "geocoding.provider is 'geocode' but no geocoding.api_key is set; reverse geocoding is disabled"
            )
            return None
        # Fixed, vendor-operated host — nothing operator-supplied to validate.
        base_url = "https://geocode.maps.co/reverse"
        api_key = geo.api_key
    else:
        if provider == "nominatim":
            host, use_https = geo.nominatim_host, geo.nominatim_use_https
        else:
            host, use_https = geo.photon_host, geo.photon_use_https
        # Logs its own reason when it rejects the host.
        resolved = build_reverse_endpoint(host, use_https=use_https, allowed_hosts=settings.network.ssrf_allowed_hosts)
        if resolved is None:
            return None
        base_url = resolved
        api_key = None

    logger.info("Reverse geocoding enabled via %s (%s), rate limit %s/s", provider, base_url, geo.rate_limit)
    return GeocodingChoice(
        service=provider,
        base_url=base_url,
        api_key=api_key,
        min_interval_seconds=min_interval,
        user_agent=geo.user_agent,
    )


@dataclass(frozen=True)
class Platform:
    """The assembled platform substrate — one instance per process.

    Attributes:
        profile: The active deployment profile.
        state: Ephemeral keyed-state provider.
        storage: Blob-storage provider.
        events: Publish/subscribe provider.
        lock: Coordination-lock provider.
        clock: Time-source provider.
        geocoding: Reverse-geocoding provider. Always present — when geocoding is
            unconfigured or misconfigured this is a no-op backend, so callers
            never branch on whether the capability exists.
        recorder: Event-log recorder, or ``None`` when event logging is disabled.
            Shared by the event bus (best-effort delivery) and the publish facade
            (durable delivery) so both paths land in the event_log dashboard.
    """

    profile: DeploymentProfile
    state: StateProvider
    storage: StorageProvider
    events: EventBusProvider
    lock: LockProvider
    clock: ClockProvider
    geocoding: GeocodingProvider
    recorder: EventRecorder | None

    def close(self) -> None:
        """Release what the platform owns: the bus consumer and the Redis clients.

        Call once on shutdown, after the host has stopped producing events. Only
        the two capabilities that hold something beyond the process are affected —
        the rest are pure or borrow the host's engine, which the host closes.

        Two things it deliberately does *not* stop, because the platform does not
        own them: the durable-job worker and its scheduled maintenance (see
        ``jasil.jobs.service.stop_job_worker``), and the database engine.

        Safe to call more than once, and never raises: a failure to shut a
        connection down must not mask whatever prompted the shutdown.
        """
        try:
            self.events.stop()
        except Exception as error:
            logger.warning("Failed to stop the event bus during shutdown: %r", error)
        redis_clients.close_shared_clients()


def build_platform(settings: JasilSettings | None = None) -> Platform:
    """Assemble the ``Platform`` for the configured deployment profile.

    Args:
        settings: The configuration to build from. Defaults to the settings the
            host installed via ``jasil.settings.configure``.

    Returns:
        A frozen ``Platform`` wiring each provider to its selected backend.

    Raises:
        ValueError: When a capability URI uses an unsupported scheme, is unset
            under a profile that has no default for it, or when the resolved
            wiring contradicts the deployment topology.
        RuntimeError: When a selected Redis backend cannot be reached.
    """
    settings = settings if settings is not None else get_settings()
    check_deployment_consistency(settings)
    logger.info("JASIL platform capabilities:\n%s", capabilities.build_capability_report(settings).render())
    profile = settings.profile
    # Build the recorder once and share it: the event bus records the lifecycle
    # of best-effort (bus-delivered) events, while the publish facade uses the
    # same recorder to record 'published' for durable (outbox-delivered) events
    # so the event_log dashboard never goes dark when durable jobs are enabled.
    recorder = _build_event_recorder(settings)
    return Platform(
        profile=profile,
        state=_build_state(settings),
        storage=_build_storage(settings),
        events=_build_events(settings, recorder),
        lock=_build_lock(settings),
        clock=SystemClock(),
        geocoding=_build_geocoding(settings),
        recorder=recorder,
    )


def _build_state(settings: JasilSettings) -> StateProvider:
    state_uri = settings.resolved_state_uri
    scheme, _, _ = state_uri.partition("://")
    if scheme == "memory":
        return MemoryState()
    if scheme in REDIS_SCHEMES:
        return RedisState.from_uri(state_uri)
    raise unsupported_scheme_error("state_uri", state_uri)


def _build_storage(settings: JasilSettings) -> StorageProvider:
    storage_uri = settings.resolved_storage_uri
    scheme, _, rest = storage_uri.partition("://")
    if scheme == "local":
        # Root the backend at the configured data dir; each storage *area* is a
        # subdirectory under it.
        return LocalStorage(rest or settings.data_dir)
    if scheme == "s3":
        # Imported lazily: boto3 is the optional `s3` extra and is absent from the
        # default image, so a top-level import would break non-S3 deployments.
        from jasil.backends.storage_s3 import S3Storage

        return S3Storage.from_uri(storage_uri)
    raise unsupported_scheme_error("storage_uri", storage_uri)


def _build_events(settings: JasilSettings, recorder: EventRecorder | None) -> EventBusProvider:
    events_uri = settings.resolved_events_uri
    scheme, _, _ = events_uri.partition("://")
    if scheme == "memory":
        return InProcessEventBus(recorder=recorder)
    if scheme in REDIS_SCHEMES:
        return RedisStreamEventBus.from_uri(events_uri, recorder=recorder)
    raise unsupported_scheme_error("events_uri", events_uri)


def _build_event_recorder(settings: JasilSettings) -> EventRecorder | None:
    if not settings.event_log.enabled:
        return None
    # Imported lazily: the recorder pulls in the ORM/session layer, which the
    # pure providers/events modules deliberately do not depend on.
    from jasil.event_log.recorder import EventLogRecorder

    return EventLogRecorder()


def _build_lock(settings: JasilSettings) -> LockProvider:
    lock_uri = settings.resolved_lock_uri
    scheme, _, _ = lock_uri.partition("://")
    if scheme == "noop":
        return NoopLock()
    if scheme == "postgres-advisory":
        return PgAdvisoryLock.from_main_database()
    raise unsupported_scheme_error("lock_uri", lock_uri)


def _build_geocoding(settings: JasilSettings) -> GeocodingProvider:
    """Resolve the reverse-geocoding backend, falling back to a no-op.

    Args:
        settings: The application settings.

    Returns:
        The configured :class:`HttpGeocoding`, or :class:`NullGeocoding` when
        geocoding is unconfigured or misconfigured.
    """
    choice = resolve_geocoding(settings)
    if choice is None:
        return NullGeocoding()
    return HttpGeocoding(
        choice.service,
        choice.base_url,
        api_key=choice.api_key,
        min_interval_seconds=choice.min_interval_seconds,
        user_agent=choice.user_agent,
    )
