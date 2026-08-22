"""Platform substrate: deployment profile, event envelope, and providers.

Public (pure) surface — safe to import anywhere; pulls in no backends:
    - ``DeploymentProfile`` — the deployment shape.
    - ``Event`` / ``new_event`` — the event envelope.
    - ``StateProvider`` / ``StorageProvider`` / ``EventBusProvider`` /
      ``LockProvider`` / ``ClockProvider`` — the capability providers.
    - ``AsyncStateProvider`` / ``AsyncStorageProvider`` / ``AsyncEventBusProvider``
      / ``AsyncLockProvider`` — the async capability providers. ``ClockProvider``
      has no async twin: reading a clock does no I/O.

The composition roots (``Platform`` / ``build_platform`` in ``jasil.container``,
``AsyncPlatform`` / ``build_async_platform`` in ``jasil.container_async``) are
imported explicitly where the platform is built (startup, ``deps``). Keeping them
out of this ``__init__`` means importing the pure profile/capabilities modules
never drags the concrete backends in — and that has to remain true now that some
of those backends would pull in ``redis.asyncio`` or ``httpx``.
"""

from importlib.metadata import PackageNotFoundError as _PackageNotFoundError
from importlib.metadata import version as _version

from jasil.events import Event, new_event
from jasil.profile import DeploymentProfile
from jasil.providers import (
    ClockProvider,
    EventBusProvider,
    LockProvider,
    StateBackendUnavailableError,
    StateProvider,
    StorageProvider,
    TieredFailureOutcome,
)
from jasil.providers_async import (
    AsyncEventBusProvider,
    AsyncGeocodingProvider,
    AsyncLockProvider,
    AsyncStateProvider,
    AsyncStorageProvider,
)

try:
    __version__ = _version("jasil")
except _PackageNotFoundError:  # pragma: no cover - running from an unbuilt source tree
    __version__ = "0.0.0+unknown"

__all__ = [
    # Package metadata
    "__version__",
    "AsyncEventBusProvider",
    "AsyncGeocodingProvider",
    "AsyncLockProvider",
    "AsyncStateProvider",
    "AsyncStorageProvider",
    "ClockProvider",
    "DeploymentProfile",
    "Event",
    "EventBusProvider",
    "LockProvider",
    "StateBackendUnavailableError",
    "StateProvider",
    "StorageProvider",
    "TieredFailureOutcome",
    "new_event",
]
