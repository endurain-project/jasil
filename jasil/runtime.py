"""Process-wide access to the assembled ``Platform``.

The composition root publishes the assembled ``Platform`` here at startup via
:func:`set_active_platform`, and every caller — request handlers, the scheduler,
the durable-job worker, a background thread — resolves the one instance
through :func:`get_active_platform` (or :func:`get_state`). Stores that must work
in any context resolve their provider lazily through :func:`get_state`.

The async platform gets its own slot rather than sharing this one. A single slot
holding "a platform of either kind" would make every mix-up — an async host
calling :func:`get_active_platform`, a sync worker reaching for the async
platform — surface as an ``AttributeError`` or, worse, as a coroutine nobody
awaited. Two slots let each accessor say exactly what is missing and which
builder was supposed to have published it.

A process may legitimately have both published at once: an async API and a
synchronous durable-job worker in the same process is a supported topology.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from jasil.container import Platform
    from jasil.container_async import AsyncPlatform
    from jasil.providers import StateProvider
    from jasil.providers_async import AsyncStateProvider

_active_platform: "Platform | None" = None
_active_async_platform: "AsyncPlatform | None" = None


def set_active_platform(platform: "Platform") -> None:
    """Publish the assembled platform for process-wide access.

    Called once from lifespan startup after ``build_platform``.

    Args:
        platform: The assembled platform substrate.
    """
    global _active_platform
    _active_platform = platform


def get_active_platform() -> "Platform":
    """Return the process-wide platform, or fail if startup has not run.

    Returns:
        The active platform substrate.

    Raises:
        RuntimeError: When no platform has been published yet.
    """
    if _active_platform is None:
        raise RuntimeError("Platform is not initialized; build_platform must run at startup before this is used.")
    return _active_platform


def is_platform_active() -> bool:
    """Return whether a platform has been published.

    Returns:
        True once :func:`set_active_platform` has run and before :func:`reset`.
    """
    return _active_platform is not None


def get_state() -> "StateProvider":
    """Return the process-wide ephemeral-state provider.

    Returns:
        The active ``StateProvider``.

    Raises:
        RuntimeError: When no platform has been published yet.
    """
    return get_active_platform().state


def set_active_async_platform(platform: "AsyncPlatform") -> None:
    """Publish the assembled async platform for process-wide access.

    Called once from lifespan startup after ``build_async_platform``.

    Args:
        platform: The assembled async platform substrate.
    """
    global _active_async_platform
    _active_async_platform = platform


def get_active_async_platform() -> "AsyncPlatform":
    """Return the process-wide async platform, or fail if startup has not run.

    Returns:
        The active async platform substrate.

    Raises:
        RuntimeError: When no async platform has been published yet.
    """
    if _active_async_platform is None:
        raise RuntimeError(
            "Async platform is not initialized; build_async_platform must run at startup before this is used. "
            "If this process builds the synchronous platform, use get_active_platform instead."
        )
    return _active_async_platform


def is_async_platform_active() -> bool:
    """Return whether an async platform has been published.

    Returns:
        True once :func:`set_active_async_platform` has run and before :func:`reset`.
    """
    return _active_async_platform is not None


def get_async_state() -> "AsyncStateProvider":
    """Return the process-wide async ephemeral-state provider.

    Returns:
        The active ``AsyncStateProvider``.

    Raises:
        RuntimeError: When no async platform has been published yet.
    """
    return get_active_async_platform().state


def reset() -> None:
    """Unpublish the active platforms, sync and async alike.

    For tests that build a platform per case; production publishes once at
    startup and never resets. This does not close either platform — call
    :meth:`jasil.container.Platform.close` or
    :meth:`jasil.container_async.AsyncPlatform.aclose` first if they own live
    connections.
    """
    global _active_platform, _active_async_platform
    _active_platform = None
    _active_async_platform = None
