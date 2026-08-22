"""Test helpers for applications that embed JASIL.

JASIL installs several things process-wide — settings, a correlation provider, a
session factory, the active platform, memoized Redis clients, the durable
subscriber registry — and a test suite has to put every one of them back between
cases or state leaks from one test into the next. This module is the shared
version of the fixture every host would otherwise write.

Nothing here is imported by the library itself, and nothing here needs an
optional extra::

    import jasil.testing as jasil_testing

    @pytest.fixture(autouse=True)
    def _jasil(tmp_path):
        platform = jasil_testing.install_test_platform(tmp_path)
        yield platform
        jasil_testing.reset_all()

A host wiring the async face gets :func:`install_async_test_platform`, which does
the same job for :func:`~jasil.container_async.build_async_platform`. Without it
there would be no supported way to test async wiring, and hosts would end up
reaching into ``jasil.runtime``'s private slots themselves.

:func:`reset_all` clears the slots for *both* faces. It is one function rather
than two because forgetting the second one is exactly the kind of leak this
module exists to prevent, and clearing a slot that was never set costs nothing.
"""

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jasil._core.redis_clients as redis_clients
import jasil.correlation as correlation
import jasil.jobs.registry as jobs_registry
import jasil.runtime as platform_runtime
import jasil.settings as jasil_settings
from jasil.container import Platform, build_platform
from jasil.container_async import AsyncPlatform, build_async_platform

__all__ = ["FixedClock", "install_async_test_platform", "install_test_platform", "reset_all"]

#: An arbitrary but stable instant for a clock nobody bothered to set. Chosen far
#: enough from any real boundary that a test asserting on it reads unambiguously.
DEFAULT_NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


@dataclass
class FixedClock:
    """A ``ClockProvider`` whose time only moves when a test moves it.

    Lease expiry, retry backoff, and retention windows are all measured against
    the platform clock, so injecting this is what lets a suite exercise them
    without sleeping.

    ``monotonic`` advances in lockstep with ``now`` rather than tracking the real
    clock, so code measuring an elapsed duration sees the time the test moved.

    Attributes:
        moment: The instant :meth:`now` returns.
    """

    moment: datetime = DEFAULT_NOW

    def now(self) -> datetime:
        """Return the current instant."""
        return self.moment

    def monotonic(self) -> float:
        """Return a monotonic reading derived from :attr:`moment`."""
        return self.moment.timestamp()

    def advance(self, seconds: float) -> None:
        """Move time forward.

        Args:
            seconds: How far to advance. Negative values move time backwards,
                which is occasionally what a clock-skew test wants.
        """
        self.moment += timedelta(seconds=seconds)


def install_test_platform(
    data_dir: str | Path,
    *,
    clock: FixedClock | None = None,
    settings: jasil_settings.JasilSettings | None = None,
) -> Platform:
    """Build an all-in-process platform, publish it, and return it.

    The ``local`` profile already resolves every capability to a process-local
    backend, so this needs no infrastructure. What it adds is the part that is
    easy to get wrong: rooting local storage inside the test's temporary
    directory instead of the working directory, substituting a controllable
    clock, and *publishing* the result — a platform that is built but never
    passed to ``set_active_platform`` makes every ``publish`` raise.

    Args:
        data_dir: Directory the local storage backend writes under. Pass pytest's
            ``tmp_path`` so nothing survives the test.
        clock: Time source to install. Defaults to a fresh :class:`FixedClock`.
        settings: Base configuration to build from. Defaults to an all-defaults
            :class:`~jasil.settings.JasilSettings`; ``data_dir`` is applied on top
            either way. It is also installed via ``jasil.settings.configure`` so
            code reading the settings directly agrees with the platform.

    Returns:
        The published :class:`~jasil.container.Platform`, carrying ``clock``.
    """
    configured = replace(settings or jasil_settings.JasilSettings(), data_dir=str(data_dir))
    jasil_settings.configure(configured)
    # Substituted after the build rather than wired in: the clock has no URI, so
    # the composition root has no seam for it.
    platform = replace(build_platform(configured), clock=clock or FixedClock())
    platform_runtime.set_active_platform(platform)
    return platform


async def install_async_test_platform(
    data_dir: str | Path,
    *,
    clock: FixedClock | None = None,
    settings: jasil_settings.JasilSettings | None = None,
) -> AsyncPlatform:
    """Build an all-in-process async platform, publish it, and return it.

    The asynchronous counterpart of :func:`install_test_platform`, doing exactly
    the same three things that are easy to get wrong: rooting local storage inside
    the test's temporary directory, substituting a controllable clock, and
    *publishing* the result — an async platform that is built but never passed to
    ``set_active_async_platform`` makes every ``apublish`` raise.

    It is a coroutine because the async composition root is: several async
    backends must connect before they are usable.

    Args:
        data_dir: Directory the local storage backend writes under. Pass pytest's
            ``tmp_path`` so nothing survives the test.
        clock: Time source to install. Defaults to a fresh :class:`FixedClock`.
            The same :class:`FixedClock` serves both faces — reading a clock does
            no I/O, so there is nothing to make async about it.
        settings: Base configuration to build from. Defaults to an all-defaults
            :class:`~jasil.settings.JasilSettings`; ``data_dir`` is applied on top
            either way. It is also installed via ``jasil.settings.configure`` so
            code reading the settings directly agrees with the platform.

    Returns:
        The published :class:`~jasil.container_async.AsyncPlatform`, carrying ``clock``.
    """
    configured = replace(settings or jasil_settings.JasilSettings(), data_dir=str(data_dir))
    jasil_settings.configure(configured)
    # Substituted after the build rather than wired in: the clock has no URI, so
    # the composition root has no seam for it.
    platform = replace(await build_async_platform(configured), clock=clock or FixedClock())
    platform_runtime.set_active_async_platform(platform)
    return platform


def reset_all() -> None:
    """Clear every process-wide slot JASIL installs, except the ORM mapping.

    Covers the settings, the correlation provider, the published platforms (sync
    and async alike), the durable subscriber registry, and the memoized Redis
    clients of both flavours. Call it in fixture teardown; leaving any one of them
    set is how a passing test starts depending on the one before it.

    **The declarative base is deliberately left alone.** JASIL's model modules
    capture it at import time, so clearing it would strand every model already
    imported — and a test module that imports one at module scope cannot re-import
    it. Map once for the whole session and leave it mapped. Call
    :func:`jasil.orm.reset` yourself only if you genuinely need to remap.

    Does not close either platform: call :meth:`jasil.container.Platform.close`
    or :meth:`jasil.container_async.AsyncPlatform.aclose` first if this process
    opened real connections.
    """
    jasil_settings.reset()
    correlation.reset()
    # Clears both platform slots.
    platform_runtime.reset()
    jobs_registry.registry.clear()
    redis_clients.reset_shared_clients()
    # Discarded rather than closed, for the same reason as the sync clients:
    # closing is meaningless for an injected fake and closing here would need an
    # event loop this synchronous teardown may not have.
    redis_clients.reset_shared_async_clients()
