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

The ``async_``-prefixed dependencies do the same for the async platform, reading
``request.app.state.async_platform`` and then the process-wide async slot. They
are separate names, and read a separate app-state attribute, so a process running
both faces cannot hand an async provider to a synchronous route or the reverse.
None of them are coroutines: resolving a provider is a dictionary lookup, and it
is the *provider's* methods that are awaitable.
"""

from fastapi import Request

import jasil.runtime as platform_runtime
from jasil.container import Platform
from jasil.container_async import AsyncPlatform
from jasil.providers import ClockProvider, EventBusProvider, LockProvider, StateProvider, StorageProvider
from jasil.providers_async import (
    AsyncEventBusProvider,
    AsyncLockProvider,
    AsyncStateProvider,
    AsyncStorageProvider,
)


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


def get_async_platform(request: Request) -> AsyncPlatform:
    """Return the ``AsyncPlatform`` for this request.

    Args:
        request: The incoming request, used to reach ``app.state``.

    Returns:
        The app-scoped async platform when the host attached one (as
        ``app.state.async_platform``), else the process-wide async platform.

    Raises:
        RuntimeError: When neither has been published, i.e. startup never ran.
    """
    platform: AsyncPlatform | None = getattr(request.app.state, "async_platform", None)
    if platform is not None:
        return platform
    return platform_runtime.get_active_async_platform()


def get_async_state(request: Request) -> AsyncStateProvider:
    """Return the async ephemeral-state provider."""
    return get_async_platform(request).state


def get_async_storage(request: Request) -> AsyncStorageProvider:
    """Return the async blob-storage provider."""
    return get_async_platform(request).storage


def get_async_events(request: Request) -> AsyncEventBusProvider:
    """Return the async event-bus provider."""
    return get_async_platform(request).events


def get_async_lock(request: Request) -> AsyncLockProvider:
    """Return the async coordination-lock provider."""
    return get_async_platform(request).lock


def get_async_clock(request: Request) -> ClockProvider:
    """Return the clock provider.

    Shared with the sync platform unchanged: reading a clock does no I/O, so
    there is no async variant to return.
    """
    return get_async_platform(request).clock
