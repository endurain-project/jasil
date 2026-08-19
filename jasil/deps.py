"""FastAPI dependencies exposing the platform providers to routes and handlers.

These thin dependencies resolve the assembled ``Platform`` so routes depend on
providers (via ``Depends``) rather than importing backends.

Two places are consulted, in order:

1. ``request.app.state.platform``, when the host attached one. Use this to give a
   single process more than one platform — two apps mounted together, or a test
   client — since it is scoped to the app rather than to the process.
2. The process-wide platform published by ``jasil.runtime.set_active_platform``,
   which is what the quick start does and what every non-request caller (the
   scheduler, the durable-job worker, a background thread) already resolves.

So a host that followed the quick start needs no extra wiring here, and one that
wants per-app isolation gets it by setting ``app.state.platform`` at startup.
"""

from fastapi import Request

import jasil.runtime as platform_runtime
from jasil.container import Platform
from jasil.providers import ClockProvider, EventBusProvider, LockProvider, StateProvider, StorageProvider


def get_platform(request: Request) -> Platform:
    """Return the ``Platform`` for this request.

    Args:
        request: The incoming request, used to reach ``app.state``.

    Returns:
        The app-scoped platform when the host attached one, else the
        process-wide platform.

    Raises:
        RuntimeError: When neither has been published, i.e. startup never ran.
    """
    platform: Platform | None = getattr(request.app.state, "platform", None)
    if platform is not None:
        return platform
    return platform_runtime.get_active_platform()


def get_state(request: Request) -> StateProvider:
    """Return the ephemeral-state provider."""
    return get_platform(request).state


def get_storage(request: Request) -> StorageProvider:
    """Return the blob-storage provider."""
    return get_platform(request).storage


def get_events(request: Request) -> EventBusProvider:
    """Return the event-bus provider."""
    return get_platform(request).events


def get_lock(request: Request) -> LockProvider:
    """Return the coordination-lock provider."""
    return get_platform(request).lock


def get_clock(request: Request) -> ClockProvider:
    """Return the clock provider."""
    return get_platform(request).clock
