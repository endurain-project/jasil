"""Platform substrate: deployment profile, event envelope, and providers.

Public (pure) surface — safe to import anywhere; pulls in no backends:
    - ``DeploymentProfile`` / ``JobWorkerMode`` — the deployment shape.
    - ``Event`` / ``new_event`` — the event envelope.
        - Narrow storage protocols plus ``StorageProvider`` as their complete
            aggregate.
        - ``StateProvider`` / ``EventBusProvider`` / ``LockProvider`` /
            ``ClockProvider`` - the other capability providers.
        - ``MailProvider`` / ``MailMessage`` and its ``MailError`` family.
        - ``RealtimeProvider`` - cross-process, best-effort per-user hints.

The composition root (``Platform`` / ``build_platform``) lives in
``jasil.container`` and is imported explicitly where the platform is
built (startup, ``deps``). Keeping it out of this ``__init__`` means importing
the pure profile/capabilities modules never drags the concrete backends in.
"""

from importlib.metadata import PackageNotFoundError as _PackageNotFoundError
from importlib.metadata import version as _version

from jasil.events import Event, new_event
from jasil.profile import DeploymentProfile, JobWorkerMode
from jasil.providers import (
    ClockProvider,
    EventBusProvider,
    LockProvider,
    MailError,
    MailMessage,
    MailNotConfiguredError,
    MailProvider,
    ObjectStat,
    PartRef,
    RealtimeProvider,
    ResumableUploads,
    ServeFile,
    ServePlan,
    ServeRedirect,
    ServeStream,
    StateBackendUnavailableError,
    StateProvider,
    StorageBackendUnavailableError,
    StorageDelivery,
    StorageManagement,
    StorageObjects,
    StorageProvider,
    StorageSizeLimitError,
    StorageStreams,
    StorageUploadSessionError,
    TieredFailureOutcome,
    UploadSession,
)

try:
    __version__ = _version("jasil")
except _PackageNotFoundError:  # pragma: no cover - running from an unbuilt source tree
    __version__ = "0.0.0+unknown"

__all__ = [
    # Package metadata
    "__version__",
    "ClockProvider",
    "DeploymentProfile",
    "Event",
    "EventBusProvider",
    "JobWorkerMode",
    "LockProvider",
    "MailError",
    "MailMessage",
    "MailNotConfiguredError",
    "MailProvider",
    "ObjectStat",
    "PartRef",
    "RealtimeProvider",
    "ResumableUploads",
    "ServeFile",
    "ServePlan",
    "ServeRedirect",
    "ServeStream",
    "StateBackendUnavailableError",
    "StateProvider",
    "StorageBackendUnavailableError",
    "StorageDelivery",
    "StorageManagement",
    "StorageObjects",
    "StorageProvider",
    "StorageSizeLimitError",
    "StorageStreams",
    "StorageUploadSessionError",
    "TieredFailureOutcome",
    "UploadSession",
    "new_event",
]
