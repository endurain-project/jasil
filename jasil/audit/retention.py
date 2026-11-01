"""The audit retention in force: the host's runtime source when one is installed, otherwise the settings.

A host that lets an administrator change retention while the system runs installs a source with
:func:`configure_retention_source`. Maintenance calls it once per run, under its lock, and uses that one value for
the retention-change check, the checkpoints it seals and the prune, so a run never seals under one retention and
prunes under another. The write path never calls it.
"""

from collections.abc import Callable
from dataclasses import dataclass, replace

from jasil.settings import AuditSettings, get_settings

__all__ = ["AuditRetention", "configure_retention_source", "effective_settings", "reset"]


@dataclass(frozen=True)
class AuditRetention:
    """The retention a runtime source returns.

    Attributes:
        retention_days: Replaces ``audit.retention_days``; the same rules apply.
        legal_hold: Replaces ``audit.legal_hold``.
    """

    retention_days: int
    legal_hold: bool = False


_source: Callable[[], AuditRetention] | None = None


def configure_retention_source(source: Callable[[], AuditRetention] | None) -> None:
    """Install the callable that supplies ``retention_days`` and ``legal_hold`` at run time.

    It is called once per :func:`jasil.audit.run_audit_maintenance` run, outside any transaction, and may raise.
    Making it cheap is the host's job. The database-side retention floor still applies whatever it returns.

    Args:
        source: Returns the current retention; ``None`` reads ``audit.retention_days`` and ``audit.legal_hold``
            from the settings again.
    """
    global _source
    _source = source


def effective_settings() -> AuditSettings:
    """Return the audit settings with the runtime source's retention applied, when a source is installed.

    The result is validated as :class:`~jasil.settings.AuditSettings` is, so a positive ``retention_days`` below
    ``retention_floor_days`` or above the maximum is refused.

    Returns:
        The audit settings in force.

    Raises:
        ValueError: When the source returns a value the settings would refuse.
        Exception: Whatever the source raises.
    """
    settings = get_settings().audit
    if _source is None:
        return settings
    retention = _source()
    return replace(settings, retention_days=retention.retention_days, legal_hold=retention.legal_hold)


def reset() -> None:
    """Remove the runtime source. For tests."""
    global _source
    _source = None
