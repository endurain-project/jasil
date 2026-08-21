"""Process-wide access to the assembled ``Platform``.

The composition root publishes the assembled ``Platform`` here at startup via
:func:`set_active_platform`, and every caller — request handlers, the scheduler,
the durable-job worker, a background thread — resolves the one instance
through :func:`get_active_platform` (or :func:`get_state`). Stores that must work
in any context resolve their provider lazily through :func:`get_state`.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from jasil.container import Platform
    from jasil.providers import StateProvider

_active_platform: "Platform | None" = None


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


def reset() -> None:
    """Unpublish the active platform.

    For tests that build a platform per case; production publishes once at
    startup and never resets. This does not close the platform — call
    :meth:`jasil.container.Platform.close` first if it owns live connections.
    """
    global _active_platform
    _active_platform = None
