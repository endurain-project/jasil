"""The composition root: build the platform substrate from settings.

``build_platform`` resolves each capability (state, storage, events, realtime,
lock, clock) to a concrete backend based on the deployment profile and returns a
frozen ``Platform`` holding the providers. It is called once at startup and
published process-wide via ``jasil.runtime`` so both request and non-request
code resolve the same instance.

Every capability resolves its backend by URI scheme, independently of the
profile: ``memory``/``redis`` for the state URI; ``local``/``s3`` for the storage
URI; ``memory``/``redis`` for the events and realtime URIs;
``noop``/``postgres-advisory`` for the lock URI. The deployment profile only shapes the *defaults* those URIs
resolve to (see the ``resolved_*`` properties on
:class:`~jasil.settings.JasilSettings`), so ``local``, ``distributed``, and
``custom`` all build the same way — the profile just picks memory-vs-Redis and
local-fs-vs-S3 defaults.

Before anything is constructed, the resolved wiring is checked against the
deployment topology (see :mod:`jasil.capabilities`): a combination that would
silently diverge across processes or nodes stops the build rather than becoming a
production mystery.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from urllib.parse import urlsplit

import jasil._core.network as network
import jasil._core.redis_clients as redis_clients
import jasil.capabilities as capabilities
from jasil._core.mail_problems import SMTP_PORTS, MailProblemCode, mail_problems
from jasil._reloading import ReloadingGeocoding, ReloadingMail, TransientBuildError
from jasil.backends.clock_system import SystemClock
from jasil.backends.events_inprocess import InProcessEventBus
from jasil.backends.events_redis import RedisStreamEventBus
from jasil.backends.geocoding_http import HttpGeocoding, NullGeocoding, resolve_reverse_endpoint
from jasil.backends.lock_noop import NoopLock
from jasil.backends.lock_pg import PgAdvisoryLock
from jasil.backends.mail_memory import MemoryMail, NullMail
from jasil.backends.mail_smtp import SEND_DEADLINE_SECONDS as SMTP_SEND_DEADLINE_SECONDS
from jasil.backends.mail_smtp import SmtpMail
from jasil.backends.realtime_inprocess import InProcessRealtime
from jasil.backends.state_memory import MemoryState
from jasil.backends.state_redis import RedisState
from jasil.backends.storage_local import LocalStorage
from jasil.profile import DeploymentProfile
from jasil.providers import (
    AuditSink,
    ClockProvider,
    EventBusProvider,
    EventRecorder,
    GeocodingProvider,
    LockProvider,
    MailProvider,
    RealtimeProvider,
    StateProvider,
    StorageProvider,
)
from jasil.settings import GeocodingSettings, JasilSettings, MailSettings, RuntimeSources, get_settings

logger = logging.getLogger(__name__)

#: ``geocoding.uri`` schemes: the service each selects, and whether it is reached over HTTPS.
_GEOCODING_SCHEMES = {
    "nominatim": ("nominatim", True),
    "nominatim+http": ("nominatim", False),
    "photon": ("photon", True),
    "photon+http": ("photon", False),
    "geocode": ("geocode", True),
}


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
        mail: Outbound-mail provider. Always present — when mail is unconfigured
            this is a backend whose ``send`` raises
            :class:`~jasil.providers.MailNotConfiguredError`, so a message is
            never dropped silently.
        audit_sink: Write-once external copy of the audit checkpoints, or ``None`` when
            ``audit.external_sink_uri`` is unset (or auditing is disabled).
        realtime: Cross-process keyed hint channel. Defaults to an
            :class:`~jasil.backends.realtime_inprocess.InProcessRealtime`, so a
            hand-built ``Platform`` need not supply it. The host registers its
            handler with ``subscribe`` and calls ``start`` itself (only the
            processes holding client connections need to); :meth:`close` stops it.
    """

    profile: DeploymentProfile
    state: StateProvider
    storage: StorageProvider
    events: EventBusProvider
    lock: LockProvider
    clock: ClockProvider
    geocoding: GeocodingProvider = field(default_factory=NullGeocoding)
    recorder: EventRecorder | None = None
    mail: MailProvider = field(default_factory=NullMail)
    realtime: RealtimeProvider = field(default_factory=InProcessRealtime)
    audit_sink: AuditSink | None = None

    def close(self) -> None:
        """Release what the platform owns: the bus consumer, the realtime subscriber and the Redis clients.

        Call once on shutdown, after the host has stopped producing events. Only
        the capabilities that hold something beyond the process are affected —
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
        self.realtime.stop()  # the provider contract: never raises
        redis_clients.close_shared_clients()


def build_platform(
    settings: JasilSettings | None = None,
    *,
    clock: ClockProvider | None = None,
    sources: RuntimeSources | None = None,
) -> Platform:
    """Assemble the ``Platform`` for the configured deployment profile.

    Args:
        settings: The configuration to build from. Defaults to the settings the
            host installed via ``jasil.settings.configure``.
        clock: Time source to wire into the platform and the backends that stamp
            time (mail ``Date``). Defaults to :class:`SystemClock`; a test
            helper passes a controllable one.
        sources: Callables returning the current mail and/or geocoding settings.
            For a capability with a source, ``settings.mail`` / ``settings.geocoding``
            are ignored and ``Platform.mail`` / ``Platform.geocoding`` follow the
            source without a restart (see :class:`~jasil.settings.RuntimeSources`).
            No source is called here: the first call happens on first use.

    Returns:
        A frozen ``Platform`` wiring each provider to its selected backend.

    Raises:
        ValueError: When a capability URI uses an unsupported scheme, is unset
            under a profile that has no default for it, when the resolved
            wiring contradicts the deployment topology, or when ``mail`` is
            configured incorrectly.
        RuntimeError: When a selected Redis backend cannot be reached.
    """
    settings = settings if settings is not None else get_settings()
    sources = sources if sources is not None else RuntimeSources()
    _check_deployment_consistency(settings, sources)
    logger.info("JASIL platform capabilities:\n%s", capabilities.build_capability_report(settings, sources).render())
    profile = settings.profile
    # Build the recorder once and share it: the event bus records the lifecycle
    # of best-effort (bus-delivered) events, while the publish facade uses the
    # same recorder to record 'published' for durable (outbox-delivered) events
    # so the event_log dashboard never goes dark when durable jobs are enabled.
    recorder = _build_event_recorder(settings)
    clock = clock if clock is not None else SystemClock()
    # Built first: it does no I/O beyond a DNS lookup (``secure=False`` only), so a
    # mistyped mail setting fails before any Redis client is connected and left open.
    mail = _build_runtime_mail(settings, clock, sources.mail) if sources.mail else _build_mail(settings, clock)
    geocoding = (
        _build_runtime_geocoding(settings, sources.geocoding) if sources.geocoding else _build_geocoding(settings)
    )
    try:
        return Platform(
            profile=profile,
            state=_build_state(settings),
            storage=_build_storage(settings),
            events=_build_events(settings, recorder),
            lock=_build_lock(settings),
            clock=clock,
            geocoding=geocoding,
            mail=mail,
            recorder=recorder,
            realtime=_build_realtime(settings),
            audit_sink=_build_audit_sink(settings),
        )
    except Exception:
        # No Platform exists to close, so release the Redis clients a backend
        # that did build has already opened.
        redis_clients.close_shared_clients()
        raise


def _check_deployment_consistency(settings: JasilSettings, sources: RuntimeSources | None = None) -> None:
    """Stop a build whose wiring contradicts its topology, unless the host opted out.

    Checked before any backend is constructed, so the failure names the setting
    rather than surfacing later as a connection error — or, worse, as a
    deployment that starts happily and diverges across replicas.
    """
    # A sourced capability's static value is not what will be used.
    checked = replace(settings, mail=MailSettings()) if sources is not None and sources.mail else settings
    issues = capabilities.check_deployment_consistency(checked)
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


def _build_state(settings: JasilSettings) -> StateProvider:
    state_uri = settings.resolved_state_uri
    scheme, _, _ = state_uri.partition("://")
    if scheme == "memory":
        return MemoryState()
    if scheme in ("redis", "rediss", "unix"):
        return RedisState.from_uri(state_uri)
    raise ValueError(f"Unsupported state_uri scheme: {scheme or state_uri!r}")


def _build_storage(settings: JasilSettings) -> StorageProvider:
    storage_uri = settings.resolved_storage_uri
    scheme, _, rest = storage_uri.partition("://")
    if scheme == "local":
        # Root the backend at the configured data dir; each storage *area* is a
        # subdirectory under it.
        return LocalStorage(
            rest or settings.data_dir,
            url_prefix=settings.storage.local_url_prefix,
            legacy_layout=settings.storage.local_legacy_layout,
            plain_areas=settings.storage.local_plain_areas,
        )
    if scheme == "s3":
        # Imported lazily: boto3 is the optional `s3` extra and is absent from the
        # default image, so a top-level import would break non-S3 deployments.
        from jasil.backends.storage_s3 import S3Storage

        return S3Storage.from_uri(storage_uri)
    raise ValueError(f"Unsupported storage_uri scheme: {scheme or storage_uri!r}")


def _build_events(settings: JasilSettings, recorder: EventRecorder | None) -> EventBusProvider:
    events_uri = settings.resolved_events_uri
    scheme, _, _ = events_uri.partition("://")
    if scheme == "memory":
        return InProcessEventBus(recorder=recorder)
    if scheme in ("redis", "rediss", "unix"):
        if not settings.events_stream or not settings.events_group:
            raise ValueError("events_stream and events_group must not be empty")
        return RedisStreamEventBus.from_uri(
            events_uri, stream=settings.events_stream, group=settings.events_group, recorder=recorder
        )
    raise ValueError(f"Unsupported events_uri scheme: {scheme or events_uri!r}")


def _build_realtime(settings: JasilSettings) -> RealtimeProvider:
    realtime_uri = settings.resolved_realtime_uri
    scheme, _, _ = realtime_uri.partition("://")
    if scheme == "memory":
        return InProcessRealtime()
    if scheme in ("redis", "rediss", "unix"):
        # Imported lazily, unlike the stream bus: nothing else needs it, and a
        # ``local`` deployment then never loads the module at all.
        from jasil.backends.realtime_redis import RedisRealtime

        return RedisRealtime.from_uri(realtime_uri, channel=settings.realtime_channel)
    raise ValueError(f"Unsupported realtime_uri scheme: {scheme or realtime_uri!r}")


def _build_audit_sink(settings: JasilSettings) -> AuditSink | None:
    uri = settings.audit.external_sink_uri
    if not uri or not settings.audit.enabled:
        return None
    scheme, _, _ = uri.partition("://")
    if scheme == "file":
        from jasil.backends.audit_sink_file import FileAuditSink

        return FileAuditSink.from_uri(uri)
    if scheme == "s3":
        from jasil.backends.audit_sink_storage import StorageAuditSink
        from jasil.backends.storage_s3 import S3Storage

        return StorageAuditSink(S3Storage.from_uri(uri))
    raise ValueError(f"Unsupported audit.external_sink_uri scheme: {scheme or 'missing'!r}")


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
    raise ValueError(f"Unsupported lock_uri scheme: {scheme or lock_uri!r}")


def _build_runtime_geocoding(settings: JasilSettings, source: Callable[[], GeocodingSettings]) -> ReloadingGeocoding:
    """Wrap ``source`` so geocoding is rebuilt, with the captured network policy, when it changes."""
    return ReloadingGeocoding(
        source, lambda fresh: _build_geocoding(replace(settings, geocoding=fresh), retry_unresolved=True)
    )


def _build_runtime_mail(
    settings: JasilSettings, clock: ClockProvider, source: Callable[[], MailSettings]
) -> ReloadingMail:
    """Wrap ``source`` so mail is rebuilt, with the captured topology and clock, when it changes."""

    def build(fresh: MailSettings) -> MailProvider:
        # The startup consistency check cannot see a value that arrives later, so
        # the same rule runs on every rebuild: a runtime edit cannot bypass it.
        issues = (
            capabilities.check_mail_consistency(
                topology=capabilities.topology_of(settings), mail_uri=fresh.uri, mail_label="mail.uri"
            )
            if fresh.uri
            else []
        )
        for issue in issues:
            if settings.enforce_deployment_consistency:
                raise ValueError(issue)
            logger.warning("Inconsistent deployment wiring: %s", issue)
        return _build_mail(replace(settings, mail=fresh), clock)

    return ReloadingMail(source, build)


def _build_geocoding(settings: JasilSettings, *, retry_unresolved: bool = False) -> GeocodingProvider:
    """Resolve the reverse-geocoding backend, falling back to a no-op.

    Unlike the other capabilities this never raises on a bad configuration:
    geocoding is optional enrichment, so an unsupported scheme, an unset API
    key, or a host that fails SSRF validation disables the capability rather than
    preventing the application from starting.

    Because a disabled capability is otherwise invisible — lookups simply return
    nothing and nothing says why — every outcome is logged at
    startup, including the successful one. An operator who set the config and saw
    no locations appear can tell from one line whether the setting was rejected
    and for what reason.

    Args:
        settings: The application settings.
        retry_unresolved: Raise :class:`TransientBuildError` instead of falling back
            when the host does not resolve, for a caller that will try again.

    Returns:
        The configured :class:`HttpGeocoding`, or :class:`NullGeocoding` when
        geocoding is unconfigured or misconfigured.
    """
    geo = settings.geocoding
    min_interval = 1.0 / geo.rate_limit if geo.rate_limit > 0 else 0.0

    if not geo.uri:
        # Unset means "not wanted", not "misconfigured" — so this stays quiet.
        logger.debug("Reverse geocoding is not configured; using the no-op backend")
        return NullGeocoding()

    # The URI is never echoed: it is operator input, and keeping it out of the
    # log costs nothing.
    try:
        parts = urlsplit(geo.uri)
        _ = parts.port  # validates the port
    except ValueError:
        logger.warning("geocoding.uri is not a valid URI; reverse geocoding is disabled")
        return NullGeocoding()
    if parts.scheme not in _GEOCODING_SCHEMES or "://" not in geo.uri:
        logger.warning(
            "geocoding.uri scheme %r is not supported (expected one of: %s); reverse geocoding is disabled",
            parts.scheme,
            ", ".join(_GEOCODING_SCHEMES),
        )
        return NullGeocoding()
    if "@" in parts.netloc or parts.path.strip("/") or parts.query or parts.fragment:
        logger.warning(
            "geocoding.uri must be scheme://host[:port] with no userinfo, path, query or fragment; reverse geocoding is disabled"
        )
        return NullGeocoding()

    service, use_https = _GEOCODING_SCHEMES[parts.scheme]
    if service == "geocode":
        if parts.netloc:
            logger.warning("geocoding.uri for geocode:// takes no host; reverse geocoding is disabled")
            return NullGeocoding()
        if not geo.api_key:
            logger.warning("geocoding.uri is geocode:// but no geocoding.api_key is set; reverse geocoding is disabled")
            return NullGeocoding()
        # Fixed, vendor-operated host — nothing operator-supplied to validate.
        base_url = "https://geocode.maps.co/reverse"
        api_key = geo.api_key
    else:
        if not parts.netloc:
            logger.warning("geocoding.uri has no host; reverse geocoding is disabled")
            return NullGeocoding()
        # Logs its own reason when it rejects the host.
        resolved, reason = resolve_reverse_endpoint(
            parts.netloc, use_https=use_https, allowed_hosts=settings.network.ssrf_allowed_hosts
        )
        if resolved is None:
            if retry_unresolved and network.is_unresolvable(reason):
                raise TransientBuildError("geocoding host could not be resolved")
            return NullGeocoding()
        base_url = resolved
        api_key = None

    logger.info("Reverse geocoding enabled via %s (%s), rate limit %s/s", service, base_url, geo.rate_limit)
    return HttpGeocoding(
        service,
        base_url,
        api_key=api_key,
        min_interval_seconds=min_interval,
        user_agent=geo.user_agent,
    )


def _build_mail(settings: JasilSettings, clock: ClockProvider) -> MailProvider:
    """Resolve the mail backend.

    Unlike geocoding, a bad mail configuration fails the build: a deployment that
    believes it can send mail and cannot is worse than one that refuses to start.
    No connection is attempted, so a mail server that is down does not block boot.
    The one network call is a DNS lookup of the host when ``secure`` is off, to
    confirm it is private; a resolver that cannot answer fails the build.

    Args:
        settings: The application settings.
        clock: The platform clock, used for the ``Date`` header.

    Returns:
        :class:`NullMail` when unset, :class:`MemoryMail` for ``memory://``,
        else :class:`SmtpMail`.

    Raises:
        ValueError: On an unsupported scheme; a URI carrying userinfo, a path, a
            query or a fragment; an invalid host or port; a missing or
            malformed ``from_address`` or ``ehlo_hostname``; ``username``
            without ``password`` (or the reverse); or ``secure=False`` outside
            its narrow case.
    """
    mail = settings.mail
    if not mail.uri:
        logger.debug("Mail is not configured; sending will raise MailNotConfiguredError")
        return NullMail()
    # The rules live in one place, shared with hosts that validate on save. A
    # multi-process memory:// is refused by the deployment-consistency check,
    # which has already run.
    problems = mail_problems(mail)
    if problems:
        if problems[0].code is MailProblemCode.CLEARTEXT_HOST_UNRESOLVABLE:
            raise TransientBuildError(problems[0].message)
        raise ValueError(problems[0].message)
    parts = urlsplit(mail.uri)
    scheme = parts.scheme
    # ``mail_problems`` has rejected an SMTP URI without a valid host.
    hostname = parts.hostname or ""
    if scheme == "memory":
        logger.warning("Mail is memory:// (messages are recorded in the process outbox, never delivered)")
        return MemoryMail(clock, mail.from_address)
    if not mail.secure:
        logger.warning("Mail over smtp:// is CLEARTEXT (mail.secure=False); use only on a trusted private network")
    if settings.jobs.enabled and settings.jobs.max_runtime_seconds <= SMTP_SEND_DEADLINE_SECONDS:
        # A running job keeps its lease, but only up to ``max_runtime_seconds``: past
        # it the job is abandoned and a send in flight is delivered by a worker that
        # no longer owns the job. Only the floor is checkable here: a handler
        # sending N messages needs N times one send.
        logger.warning(
            "jobs.max_runtime_seconds (%d) is not above the SMTP send deadline (%d s, per "
            "message); a slow send can be abandoned mid-delivery and sent again by a retry",
            settings.jobs.max_runtime_seconds,
            SMTP_SEND_DEADLINE_SECONDS,
        )
    resolved_port = parts.port or SMTP_PORTS[scheme]
    logger.info("Mail enabled via %s%s", scheme, " (authenticated)" if mail.username else "")
    return SmtpMail(
        hostname,
        resolved_port,
        implicit_tls=scheme == "smtps",
        tls=mail.secure,
        from_address=mail.from_address,
        clock=clock,
        username=mail.username,
        password=mail.password,
        local_hostname=mail.ehlo_hostname,
    )
