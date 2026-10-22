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
"""

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jasil._core.redis_clients as redis_clients
import jasil.capabilities as capabilities
import jasil.correlation as correlation
import jasil.jobs.registry as jobs_registry
import jasil.runtime as platform_runtime
import jasil.settings as jasil_settings
from jasil.backends.mail_memory import MemoryMail
from jasil.backends.realtime_inprocess import InProcessRealtime
from jasil.container import Platform, build_platform

__all__ = [
    "FixedClock",
    "InProcessRealtime",
    "MemoryMail",
    "capture_hints",
    "install_test_platform",
    "memory_mail",
    "reset_all",
]

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


def _prefer_memory(
    configured: jasil_settings.JasilSettings,
    memory: jasil_settings.JasilSettings,
    refused: object,
) -> jasil_settings.JasilSettings:
    """Pick the ``memory://`` variant, unless the consistency check refuses it under enforcement."""
    return memory if not refused or not memory.enforce_deployment_consistency else configured


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
            code reading the settings directly agrees with the platform. When
            ``mail.uri`` is unset (or empty) it becomes ``memory://``, so sent
            messages land in the outbox :func:`memory_mail` returns — unless the
            settings describe a multi-process deployment that enforces
            consistency, where ``memory://`` would fail the build; mail then
            stays unset and sending raises. An unset ``realtime_uri`` becomes
            ``memory://`` under the same rule, so hints are delivered inline
            and can be captured with :func:`capture_hints`.

    Returns:
        The published :class:`~jasil.container.Platform`, carrying ``clock``.
    """
    configured = replace(settings or jasil_settings.JasilSettings(), data_dir=str(data_dir))
    if not configured.realtime_uri:
        # An unset realtime URI follows the event bus, which may be (fake)
        # Redis; a test wants hints delivered inline and capturable instead.
        inline = replace(configured, realtime_uri="memory://")
        configured = _prefer_memory(
            configured,
            inline,
            capabilities.check_state_consistency(
                topology=capabilities.topology_of(inline),
                state_sources=(capabilities.StateSource(label="realtime_uri", uri="memory://"),),
            ),
        )
    if not configured.mail.uri:
        # Unconfigured mail would raise on every send; a test wants it recorded.
        recording = replace(configured, mail=replace(configured.mail, uri="memory://", secure=True))
        configured = _prefer_memory(
            configured,
            recording,
            capabilities.check_mail_consistency(
                topology=capabilities.topology_of(recording),
                mail_uri=recording.mail.uri,
                mail_label="mail.uri",
            ),
        )
    jasil_settings.configure(configured)
    platform = build_platform(configured, clock=clock or FixedClock())
    platform_runtime.set_active_platform(platform)
    return platform


def memory_mail(platform: Platform) -> MemoryMail:
    """Return the recording mail backend of a test platform, typed.

    ``Platform.mail`` is a :class:`~jasil.providers.MailProvider`, which has no
    ``outbox``; this narrows it for a type checker and fails loudly when the
    platform was built with a real backend.

    Args:
        platform: The platform from :func:`install_test_platform`.

    Returns:
        The ``MemoryMail`` whose ``outbox`` holds every message sent.

    Raises:
        TypeError: When ``platform.mail`` is not the in-memory backend.
    """
    mail = platform.mail
    if not isinstance(mail, MemoryMail):
        raise TypeError("platform.mail is not the in-memory backend; configure mail.uri as memory://")
    return mail


def capture_hints(platform: Platform) -> list[tuple[str, dict]]:
    """Record every realtime hint the platform delivers from now on.

    Registers a handler on the in-process realtime backend and returns the list
    it appends to, as ``(key, hint)`` in publish order. The hint is the
    decoded copy a real subscriber would receive, so a value that does not
    survive JSON (a tuple, an integer key) shows up the way production sends it.

    Args:
        platform: The platform from :func:`install_test_platform`.

    Returns:
        The live list of captured hints.

    Raises:
        TypeError: When ``platform.realtime`` is not the in-process backend.
    """
    realtime = platform.realtime
    if not isinstance(realtime, InProcessRealtime):
        raise TypeError("platform.realtime is not the in-process backend; configure realtime_uri as memory://")
    captured: list[tuple[str, dict]] = []
    realtime.subscribe(lambda key, hint: captured.append((key, hint)))
    return captured


def reset_all() -> None:
    """Clear every process-wide slot JASIL installs, except the ORM mapping.

    Covers the settings, the correlation provider, the published platform (and
    the realtime handlers registered on it), the durable subscriber registry, and
    the memoized Redis clients. Call it in
    fixture teardown; leaving any one of them set is how a passing test starts
    depending on the one before it.

    **The declarative base is deliberately left alone.** JASIL's model modules
    capture it at import time, so clearing it would strand every model already
    imported — and a test module that imports one at module scope cannot re-import
    it. Map once for the whole session and leave it mapped. Call
    :func:`jasil.orm.reset` yourself only if you genuinely need to remap.

    Does not close the platform: call :meth:`jasil.container.Platform.close`
    first if this process opened real connections.
    """
    if platform_runtime.is_platform_active():
        realtime = platform_runtime.get_active_platform().realtime
        if isinstance(realtime, InProcessRealtime):
            # A test that kept a reference to the platform must not keep
            # receiving hints into another test's capture list.
            realtime.clear()
    jasil_settings.reset()
    correlation.reset()
    platform_runtime.reset()
    jobs_registry.registry.clear()
    redis_clients.reset_shared_clients()
