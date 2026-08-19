"""Startup capability report and deployment-consistency checks.

Renders how each infrastructure capability (state, storage, events, lock, clock)
is wired for a human-readable startup log, and detects fatal inconsistencies — a
deployment that *requires* a shared backend but resolves one to a process- or
node-local implementation.

Three consistency rules are enforced:

- **Cross-process backends** — ephemeral *state* (rate limiting, throttling,
  single-use tokens) and the *event* bus — must not resolve to process-local
  memory when the topology requires shared state (the ``distributed`` profile or
  more than one web worker); the stores and the bus would diverge silently across
  processes. Both share the ``memory://`` / ``redis://`` vocabulary and are
  validated by :func:`check_state_consistency`.
- **Cross-node storage** must not resolve to the local filesystem under the
  ``distributed`` profile, where replicas run on separate nodes with no shared
  disk. A multi-worker *local* deployment shares one host disk, so local storage
  stays valid there. Validated by :func:`check_storage_consistency`.
- **The coordination lock** must not resolve to the in-process ``noop`` lock
  whenever the deployment runs more than one process (the ``distributed`` profile
  or any multi-worker deployment), or every process would run every scheduled
  job. Validated by :func:`check_lock_consistency`.

The *clock* is always the system clock, so its default is never a fatal choice
here.

:func:`check_deployment_consistency` applies all three to a
:class:`~jasil.settings.JasilSettings`, and ``jasil.container.build_platform``
calls it at startup: an inconsistency raises unless the host sets
``enforce_deployment_consistency=False``, which downgrades it to a warning.

Pure module — no infrastructure imports, so every check runs before a single
backend has been constructed.
"""

from collections.abc import Sequence
from dataclasses import dataclass

from jasil.profile import (
    DeploymentProfile,
    DeploymentTopology,
    StateBackendKind,
    classify_state_uri,
    resolve_topology,
)
from jasil.settings import JasilSettings

# Shown in the report when a capability URI was left to the profile.
_PROFILE_DEFAULT = "profile default"


@dataclass(frozen=True)
class StateSource:
    """A configured source of ephemeral state.

    Attributes:
        label: The setting backing this state (e.g. ``state_uri``).
        uri: The effective storage URI.
        applies: Whether this source is active — a capability the host disabled
            cannot be misconfigured.
    """

    label: str
    uri: str
    applies: bool = True

    @property
    def backend(self) -> StateBackendKind:
        """The classified backend for this source's URI."""
        return classify_state_uri(self.uri)


@dataclass(frozen=True)
class CapabilityRow:
    """One line of the capability report.

    Attributes:
        name: Capability name (state / storage / events / lock / clock).
        backend: The resolved backend (e.g. ``memory``, ``local``, ``system``).
        source: Where the value came from (setting name or ``profile default``).
    """

    name: str
    backend: str
    source: str


@dataclass(frozen=True)
class CapabilityReport:
    """A rendered snapshot of how each capability is wired at startup."""

    topology: DeploymentTopology
    rows: tuple[CapabilityRow, ...]

    def render(self) -> str:
        """Render the report as a multi-line, aligned string."""
        header = (
            f"Deployment profile: {self.topology.profile.value} "
            f"(web_workers={self.topology.web_workers}, "
            f"requires_shared_state={self.topology.requires_shared_state})"
        )
        width = max((len(row.name) for row in self.rows), default=0)
        lines = [f"  {row.name.ljust(width)} -> {row.backend}  (source: {row.source})" for row in self.rows]
        return "\n".join([header, *lines])


def _scheme_of(uri: str) -> str:
    """Return a URI's scheme, or the whole value when it carries none."""
    scheme, separator, _ = uri.partition("://")
    return scheme if separator else uri


def _source_of(configured: str | None, label: str) -> str:
    """Name what supplied a capability URI: the host's setting, or the profile."""
    return label if configured else _PROFILE_DEFAULT


def build_capability_report(settings: JasilSettings) -> CapabilityReport:
    """Build the observational capability report for startup logging.

    Args:
        settings: The configuration the platform is being built from.

    Returns:
        A ``CapabilityReport`` reflecting the effective wiring: the backend each
        capability resolved to, and whether that came from an explicit setting or
        from the deployment profile's default.

    Raises:
        ValueError: When a capability URI is unset under a profile that has no
            default for it.
    """
    rows = (
        CapabilityRow("state", _scheme_of(settings.resolved_state_uri), _source_of(settings.state_uri, "state_uri")),
        CapabilityRow(
            "storage", _scheme_of(settings.resolved_storage_uri), _source_of(settings.storage_uri, "storage_uri")
        ),
        CapabilityRow(
            "events", _scheme_of(settings.resolved_events_uri), _source_of(settings.events_uri, "events_uri")
        ),
        CapabilityRow("lock", _scheme_of(settings.resolved_lock_uri), _source_of(settings.lock_uri, "lock_uri")),
        CapabilityRow("clock", "system", "always the system clock"),
    )
    return CapabilityReport(topology=resolve_topology(settings.profile, settings.web_workers), rows=rows)


def check_deployment_consistency(settings: JasilSettings) -> list[str]:
    """Return every fatal wiring inconsistency in ``settings`` (empty when sound).

    The entry point ``build_platform`` uses: it resolves the capability URIs the
    profile would actually build from, then applies all three rules to them.

    Args:
        settings: The configuration the platform is being built from.

    Returns:
        Human-readable issue messages; empty when the wiring is consistent.

    Raises:
        ValueError: When a capability URI is unset under a profile that has no
            default for it.
    """
    profile = settings.profile
    web_workers = settings.web_workers
    return [
        *check_state_consistency(
            profile=profile,
            web_workers=web_workers,
            state_sources=(
                StateSource(label="state_uri", uri=settings.resolved_state_uri),
                StateSource(label="events_uri", uri=settings.resolved_events_uri),
            ),
        ),
        *check_storage_consistency(
            profile=profile,
            storage_uri=settings.resolved_storage_uri,
            storage_label="storage_uri",
        ),
        *check_lock_consistency(
            profile=profile,
            web_workers=web_workers,
            lock_uri=settings.resolved_lock_uri,
            lock_label="lock_uri",
        ),
    ]


def check_state_consistency(
    *,
    profile: DeploymentProfile,
    web_workers: int,
    state_sources: Sequence[StateSource],
) -> list[str]:
    """Return fatal issues for cross-process backends (empty when consistent).

    A deployment that requires shared state (the ``distributed`` profile or more
    than one web worker) but resolves a cross-process backend to process-local
    memory is fatally misconfigured: ephemeral state (rate-limit counters, lockout
    gates, single-use tokens) and the in-process event bus would silently diverge
    across processes. State stores and the event bus share the ``memory://`` /
    ``redis://`` vocabulary, so both are validated here.

    Args:
        profile: The deployment profile.
        web_workers: Configured worker count.
        state_sources: The active cross-process backends to validate (the
            resolved state and event-bus URIs).

    Returns:
        Human-readable issue messages; empty when consistent. The ``custom``
        profile is exempt — it promises no defaults, so nothing can contradict one.
    """
    if profile is DeploymentProfile.CUSTOM:
        return []
    topology = resolve_topology(profile, web_workers)
    if not topology.requires_shared_state:
        return []
    issues: list[str] = []
    for source in state_sources:
        if not source.applies or source.backend is StateBackendKind.REDIS:
            continue
        reason = (
            "process-local memory" if source.backend is StateBackendKind.MEMORY else "an unrecognized storage scheme"
        )
        issues.append(
            f"{source.label} resolves to {reason}, but "
            f"profile={profile.value} with web_workers={topology.web_workers} "
            f"requires a backend shared across processes. Point it at Redis "
            f"(redis://...) or run a single worker under the local profile."
        )
    return issues


def check_storage_consistency(
    *,
    profile: DeploymentProfile,
    storage_uri: str,
    storage_label: str,
) -> list[str]:
    """Return a fatal issue when distributed storage resolves to local disk.

    Under the ``distributed`` profile replicas run on separate nodes that do not
    share a filesystem, so blob storage must be object storage. A multi-worker
    ``local`` deployment shares one host disk, so local storage stays valid there
    and is not flagged.

    Args:
        profile: The deployment profile.
        storage_uri: The resolved blob-storage URI.
        storage_label: The setting backing blob storage (for the message).

    Returns:
        A single-item issue list when misconfigured; empty otherwise.
    """
    if profile is not DeploymentProfile.DISTRIBUTED:
        return []
    if not storage_uri.strip().lower().startswith("local://"):
        return []
    return [
        f"{storage_label} resolves to the local filesystem, but "
        f"profile={profile.value} runs replicas on separate nodes that do "
        f"not share a disk. Point it at object storage (s3://bucket/...)."
    ]


def check_lock_consistency(
    *,
    profile: DeploymentProfile,
    web_workers: int,
    lock_uri: str,
    lock_label: str,
) -> list[str]:
    """Return a fatal issue when a multi-process deployment uses a no-op lock.

    The coordination lock makes scheduled and backfill work single-runner across
    processes. Whenever a deployment runs more than one process — the
    ``distributed`` profile or any multi-worker deployment
    (:attr:`~jasil.profile.DeploymentTopology.requires_shared_state`) — an
    in-process ``noop://`` lock coordinates nothing, so every process would run
    every interval job (the retention prune, a backfill, an upstream sync). The
    profile-aware default already resolves to ``postgres-advisory://`` in that
    case, so this only trips on an explicit ``lock_uri="noop://"`` override. A
    single-process ``local`` deployment has nothing to coordinate, so ``noop``
    stays valid there.

    Args:
        profile: The deployment profile.
        web_workers: Configured worker count.
        lock_uri: The resolved coordination-lock URI.
        lock_label: The setting backing the lock (for the message).

    Returns:
        A single-item issue list when misconfigured; empty otherwise. The
        ``custom`` profile is exempt — it promises no defaults.
    """
    if profile is DeploymentProfile.CUSTOM:
        return []
    topology = resolve_topology(profile, web_workers)
    if not topology.requires_shared_state:
        return []
    if not lock_uri.strip().lower().startswith("noop://"):
        return []
    return [
        f"{lock_label} resolves to an in-process no-op lock, but "
        f"profile={profile.value} with web_workers={topology.web_workers} "
        f"runs multiple processes that would each run scheduled jobs. Point it at "
        f"the shared database lock (postgres-advisory://)."
    ]
