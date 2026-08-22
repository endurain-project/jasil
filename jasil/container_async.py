"""The async composition root: build the async platform substrate from settings.

The asynchronous twin of :mod:`jasil.container`. ``build_async_platform``
resolves each capability to a concrete *async* backend and returns a frozen
:class:`AsyncPlatform` holding the providers.

Nothing about *selection* is decided here. The topology check, the
scheme-to-backend mapping, the error wording and the geocoding decision all come
from :mod:`jasil.container`, so a given configuration resolves to the same
capabilities whichever face the host builds. What differs, and all that differs,
is which class is instantiated at the end of each branch.

The one structural difference from the sync root is that building is a
coroutine. Several async backends have to connect before they are usable — a
``redis.asyncio`` client's connectivity check is itself awaitable — and doing
that I/O in a constructor is what forces the sync root's ``from_uri`` classmethods
to block. Making the root ``async`` instead keeps the "a provider that exists is
a provider that works" guarantee without hiding a blocking call inside
``__init__``.

A process may hold a sync platform and an async platform at once — an async API
beside a synchronous worker is a perfectly reasonable topology — but a single
platform is entirely one or the other. There is no mode detection anywhere here:
the host chooses a root by calling one function or the other.
"""

import logging
from dataclasses import dataclass

import jasil._core.redis_clients as redis_clients
import jasil.capabilities as capabilities
import jasil.container as container
from jasil.backends.clock_system import SystemClock
from jasil.backends.events_inprocess_async import AsyncInProcessEventBus
from jasil.backends.geocoding_http_async import AsyncHttpGeocoding, AsyncNullGeocoding
from jasil.backends.lock_noop_async import AsyncNoopLock
from jasil.backends.state_memory_async import AsyncMemoryState
from jasil.backends.storage_local_async import AsyncLocalStorage
from jasil.profile import DeploymentProfile
from jasil.providers import ClockProvider
from jasil.providers_async import (
    AsyncEventBusProvider,
    AsyncEventRecorder,
    AsyncGeocodingProvider,
    AsyncLockProvider,
    AsyncStateProvider,
    AsyncStorageProvider,
)
from jasil.settings import JasilSettings, get_settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AsyncPlatform:
    """The assembled async platform substrate — one instance per process.

    The field names match :class:`jasil.container.Platform` exactly, so host code
    that only reads ``platform.state`` or ``platform.clock`` reads the same names
    under either face and the difference is confined to the ``await``.

    Attributes:
        profile: The active deployment profile.
        state: Ephemeral keyed-state provider.
        storage: Blob-storage provider.
        events: Publish/subscribe provider.
        lock: Coordination-lock provider.
        clock: Time-source provider. Shared with the sync platform unchanged —
            reading a clock does no I/O, so there is nothing to make async.
        geocoding: Reverse-geocoding provider. Always present — when geocoding is
            unconfigured or misconfigured this is a no-op backend, so callers
            never branch on whether the capability exists.
        recorder: Event-log recorder, or ``None`` when event logging is disabled.
    """

    profile: DeploymentProfile
    state: AsyncStateProvider
    storage: AsyncStorageProvider
    events: AsyncEventBusProvider
    lock: AsyncLockProvider
    clock: ClockProvider
    geocoding: AsyncGeocodingProvider
    recorder: AsyncEventRecorder | None

    async def aclose(self) -> None:
        """Release what the platform owns: the bus consumer, HTTP and Redis clients.

        Call once on shutdown, after the host has stopped producing events. Only
        the capabilities that hold something beyond the process are affected — the
        rest are pure or borrow the host's engine, which the host closes.

        Two things it deliberately does *not* stop, because the platform does not
        own them: the durable-job worker and its scheduled maintenance (see
        ``jasil.jobs.service.stop_async_job_worker``), and the database engine.

        Safe to call more than once, and never raises: a failure to shut a
        connection down must not mask whatever prompted the shutdown.

        Returns:
            None.
        """
        try:
            await self.events.stop()
        except Exception as error:
            logger.warning("Failed to stop the event bus during shutdown: %r", error)
        # The HTTP geocoding backend pools connections; the no-op one has nothing
        # to close, hence the guarded lookup rather than a protocol method — a
        # provider that holds no resources should not have to implement one.
        closer = getattr(self.geocoding, "aclose", None)
        if closer is not None:
            try:
                await closer()
            except Exception as error:
                logger.warning("Failed to close the geocoding client during shutdown: %r", error)
        await redis_clients.close_shared_async_clients()


async def build_async_platform(settings: JasilSettings | None = None) -> AsyncPlatform:
    """Assemble the ``AsyncPlatform`` for the configured deployment profile.

    Args:
        settings: The configuration to build from. Defaults to the settings the
            host installed via ``jasil.settings.configure``.

    Returns:
        A frozen ``AsyncPlatform`` wiring each provider to its selected async backend.

    Raises:
        ValueError: When a capability URI uses an unsupported scheme, is unset
            under a profile that has no default for it, or when the resolved
            wiring contradicts the deployment topology.
        RuntimeError: When a selected Redis backend cannot be reached.
    """
    settings = settings if settings is not None else get_settings()
    container.check_deployment_consistency(settings)
    logger.info("JASIL async platform capabilities:\n%s", capabilities.build_capability_report(settings).render())
    # Built once and shared, for the same reason as the sync root: the bus records
    # the lifecycle of best-effort events while the publish facade records
    # 'published' for durable ones, so the dashboard never goes dark.
    recorder = _build_event_recorder(settings)
    return AsyncPlatform(
        profile=settings.profile,
        state=await _build_state(settings),
        storage=await _build_storage(settings),
        events=await _build_events(settings, recorder),
        lock=_build_lock(settings),
        clock=SystemClock(),
        geocoding=_build_geocoding(settings),
        recorder=recorder,
    )


async def _build_state(settings: JasilSettings) -> AsyncStateProvider:
    state_uri = settings.resolved_state_uri
    scheme, _, _ = state_uri.partition("://")
    if scheme == "memory":
        return AsyncMemoryState()
    if scheme in container.REDIS_SCHEMES:
        # Imported lazily so a deployment that never resolves a redis:// URI does
        # not load ``redis.asyncio``, mirroring the sync root's optional-extra rule.
        from jasil.backends.state_redis_async import create_async_redis_state

        return await create_async_redis_state(state_uri)
    raise container.unsupported_scheme_error("state_uri", state_uri)


async def _build_storage(settings: JasilSettings) -> AsyncStorageProvider:
    storage_uri = settings.resolved_storage_uri
    scheme, _, rest = storage_uri.partition("://")
    if scheme == "local":
        # Root the backend at the configured data dir; each storage *area* is a
        # subdirectory under it.
        return AsyncLocalStorage(rest or settings.data_dir)
    if scheme == "s3":
        # Imported lazily: boto3 is the optional `s3` extra and is absent from the
        # default image, so a top-level import would break non-S3 deployments.
        from jasil.backends.storage_s3_async import AsyncS3Storage

        return AsyncS3Storage.from_uri(storage_uri)
    raise container.unsupported_scheme_error("storage_uri", storage_uri)


async def _build_events(settings: JasilSettings, recorder: AsyncEventRecorder | None) -> AsyncEventBusProvider:
    events_uri = settings.resolved_events_uri
    scheme, _, _ = events_uri.partition("://")
    if scheme == "memory":
        return AsyncInProcessEventBus(recorder=recorder)
    if scheme in container.REDIS_SCHEMES:
        from jasil.backends.events_redis_async import create_async_redis_event_bus

        return await create_async_redis_event_bus(events_uri, recorder=recorder)
    raise container.unsupported_scheme_error("events_uri", events_uri)


def _build_event_recorder(settings: JasilSettings) -> AsyncEventRecorder | None:
    if not settings.event_log.enabled:
        return None
    # Imported lazily: the recorder pulls in the ORM/session layer, which the
    # pure providers/events modules deliberately do not depend on.
    from jasil.event_log.recorder_async import AsyncEventLogRecorder

    return AsyncEventLogRecorder()


def _build_lock(settings: JasilSettings) -> AsyncLockProvider:
    lock_uri = settings.resolved_lock_uri
    scheme, _, _ = lock_uri.partition("://")
    if scheme == "noop":
        return AsyncNoopLock()
    if scheme == "postgres-advisory":
        from jasil.backends.lock_pg_async import AsyncPgAdvisoryLock

        return AsyncPgAdvisoryLock.from_main_database()
    raise container.unsupported_scheme_error("lock_uri", lock_uri)


def _build_geocoding(settings: JasilSettings) -> AsyncGeocodingProvider:
    """Resolve the async reverse-geocoding backend, falling back to a no-op.

    Args:
        settings: The application settings.

    Returns:
        The configured :class:`AsyncHttpGeocoding`, or :class:`AsyncNullGeocoding`
        when geocoding is unconfigured or misconfigured.
    """
    # The deciding and the logging happen in the sync root's resolver; a second
    # copy of the SSRF host validation is the last thing this codebase needs.
    choice = container.resolve_geocoding(settings)
    if choice is None:
        return AsyncNullGeocoding()
    return AsyncHttpGeocoding(
        choice.service,
        choice.base_url,
        api_key=choice.api_key,
        min_interval_seconds=choice.min_interval_seconds,
        user_agent=choice.user_agent,
    )
