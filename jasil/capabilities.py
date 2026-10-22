"""Startup capability report and deployment-consistency checks.

Renders how each infrastructure capability (state, storage, events, realtime, lock,
mail, clock)
is wired for a human-readable startup log, and detects fatal inconsistencies — a
deployment that *requires* a shared backend but resolves one to a process- or
node-local implementation.

Every rule asks one question of a :class:`~jasil.profile.DeploymentTopology`
rather than re-deriving the deployment's shape from raw settings: can two
processes reach this capability (``multi_process``), or two machines
(``multi_node``)? What makes a topology multi-process — the ``distributed``
profile, more than one web worker, or standalone job workers — is decided in one
place, :func:`~jasil.profile.resolve_topology`, so a new kind of process needs no
change here.

Five consistency rules are enforced:

- **Cross-process backends** — ephemeral *state* (rate limiting, escalating gates,
  single-use tokens), the *event* bus and the *realtime* hint channel — must not
  resolve to process-local memory when the topology is multi-process; the stores
  and the bus would diverge silently across processes, and a hint published by
  one process (a job worker, another replica) would never reach the process
  holding the client's connection. All three share the ``memory://`` /
  ``redis://`` vocabulary and are validated by :func:`check_state_consistency`.
- **Cross-node storage** must not resolve to the local filesystem when the
  topology is multi-node (the ``distributed`` profile), where replicas run on
  separate nodes with no shared disk. Several processes of a *local* deployment
  share one host disk, so local storage stays valid there. Validated by
  :func:`check_storage_consistency`.
- **The coordination lock** must not resolve to the in-process ``noop`` lock
  when the topology is multi-process, or every process would run every scheduled
  job. Validated by :func:`check_lock_consistency`.
- **Mail** must not resolve to the recording ``memory://`` backend when the
  topology is multi-process: every process would keep its own outbox and no
  message would ever be delivered. Validated by :func:`check_mail_consistency`.
- **Job workers** may not be declared standalone while durable jobs are
  disabled: the declaration would make every process stricter for consumers that
  cannot exist. Validated by :func:`check_job_worker_consistency`.

The *clock* is always the system clock, so its default is never a fatal choice
here.

:func:`check_deployment_consistency` applies all five to a
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
    JobWorkerMode,
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
        name: Capability name (state / storage / events / realtime / lock / mail / clock).
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
            f"job_worker_mode={self.topology.job_worker_mode.value}, "
            f"multi_process={self.topology.multi_process}, "
            f"multi_node={self.topology.multi_node})"
        )
        width = max((len(row.name) for row in self.rows), default=0)
        lines = [f"  {row.name.ljust(width)} -> {row.backend}  (source: {row.source})" for row in self.rows]
        return "\n".join([header, *lines])


def _scheme_of(uri: str) -> str:
    """Return a URI's scheme, or the whole value when it carries none."""
    scheme, separator, _ = uri.partition("://")
    return scheme if separator else uri


def _mail_backend_of(uri: str | None, *, secure: bool) -> str:
    """Name the mail backend for the report without ever echoing the value.

    A mail URI is operator input that may carry a mistyped credential, and the
    report is logged before the URI is validated, so a value with no ``://`` is
    reported as ``invalid`` rather than as itself. A cleartext relay says so.
    """
    if not uri:
        return "disabled"
    scheme, separator, _ = uri.partition("://")
    if not (separator and scheme.isalnum()):
        return "invalid"
    scheme = scheme.lower()
    return f"{scheme} (cleartext)" if scheme == "smtp" and not secure else scheme


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
        CapabilityRow(
            "realtime",
            _scheme_of(settings.resolved_realtime_uri),
            "realtime_uri"
            if settings.realtime_uri
            else f"follows events_uri ({_source_of(settings.events_uri, 'events_uri')})",
        ),
        CapabilityRow("lock", _scheme_of(settings.resolved_lock_uri), _source_of(settings.lock_uri, "lock_uri")),
        CapabilityRow(
            "mail",
            _mail_backend_of(settings.mail.uri, secure=settings.mail.secure),
            "mail.uri" if settings.mail.uri else "not configured",
        ),
        CapabilityRow("clock", "system", "always the system clock"),
    )
    return CapabilityReport(topology=topology_of(settings), rows=rows)


def topology_of(settings: JasilSettings) -> DeploymentTopology:
    """Return the deployment topology ``settings`` describe.

    The one place that turns settings into a topology, so the report, every rule
    and the job-worker entry points agree on what shape a deployment has.

    Args:
        settings: The configuration the platform is being built from.

    Returns:
        The resolved topology.
    """
    return resolve_topology(
        profile=settings.profile,
        web_workers=settings.web_workers,
        job_worker_mode=settings.job_worker_mode,
    )


def check_deployment_consistency(settings: JasilSettings) -> list[str]:
    """Return every fatal wiring inconsistency in ``settings`` (empty when sound).

    The entry point ``build_platform`` uses: it resolves the capability URIs the
    profile would actually build from, then applies all five rules to them.

    Args:
        settings: The configuration the platform is being built from.

    Returns:
        Human-readable issue messages; empty when the wiring is consistent.

    Raises:
        ValueError: When a capability URI is unset under a profile that has no
            default for it.
    """
    topology = topology_of(settings)
    return [
        *check_state_consistency(
            topology=topology,
            state_sources=(
                StateSource(label="state_uri", uri=settings.resolved_state_uri),
                StateSource(label="events_uri", uri=settings.resolved_events_uri),
                # Unset, it *is* the events URI, already checked above: flagging
                # it again would name a setting the operator never wrote.
                StateSource(
                    label="realtime_uri", uri=settings.resolved_realtime_uri, applies=bool(settings.realtime_uri)
                ),
            ),
        ),
        *check_storage_consistency(
            topology=topology,
            storage_uri=settings.resolved_storage_uri,
            storage_label="storage_uri",
        ),
        *check_lock_consistency(
            topology=topology,
            lock_uri=settings.resolved_lock_uri,
            lock_label="lock_uri",
        ),
        *check_mail_consistency(
            topology=topology,
            mail_uri=settings.mail.uri,
            mail_label="mail.uri",
        ),
        *check_job_worker_consistency(
            topology=topology,
            jobs_enabled=settings.jobs.enabled,
        ),
    ]


def check_state_consistency(
    *,
    topology: DeploymentTopology,
    state_sources: Sequence[StateSource],
) -> list[str]:
    """Return fatal issues for cross-process backends (empty when consistent).

    A multi-process deployment that resolves a cross-process backend to
    process-local memory is fatally misconfigured: ephemeral state (rate-limit
    counters, escalating gates, single-use tokens) and the in-process event bus
    would silently diverge across processes, and in-process realtime hints would
    never leave the process that published them. State stores, the event bus and
    the realtime channel share the ``memory://`` / ``redis://`` vocabulary, so all
    three are validated here.

    Args:
        topology: The deployment topology.
        state_sources: The active cross-process backends to validate (the
            resolved state, event-bus and realtime URIs).

    Returns:
        Human-readable issue messages; empty when consistent. The ``custom``
        profile is exempt — it promises no defaults, so nothing can contradict one.
    """
    if topology.profile is DeploymentProfile.CUSTOM or not topology.multi_process:
        return []
    issues: list[str] = []
    for source in state_sources:
        if not source.applies or source.backend is StateBackendKind.REDIS:
            continue
        reason = (
            "process-local memory" if source.backend is StateBackendKind.MEMORY else "an unrecognized storage scheme"
        )
        issues.append(
            f"{source.label} resolves to {reason}, but this deployment runs multiple processes "
            f"({topology.multi_process_reason}) that need a backend shared across them. Point it at "
            f"Redis (redis://...) or run a single process under the local profile."
        )
    return issues


def check_storage_consistency(
    *,
    topology: DeploymentTopology,
    storage_uri: str,
    storage_label: str,
) -> list[str]:
    """Return a fatal issue when multi-node storage resolves to local disk.

    Under the ``distributed`` profile replicas run on separate nodes that do not
    share a filesystem, so blob storage must be object storage. Several processes
    of a ``local`` deployment share one host disk, so local storage stays valid
    there and is not flagged.

    Args:
        topology: The deployment topology.
        storage_uri: The resolved blob-storage URI.
        storage_label: The setting backing blob storage (for the message).

    Returns:
        A single-item issue list when misconfigured; empty otherwise.
    """
    if not topology.multi_node:
        return []
    if not storage_uri.strip().lower().startswith("local://"):
        return []
    return [
        f"{storage_label} resolves to the local filesystem, but "
        f"profile={topology.profile.value} runs replicas on separate nodes that do "
        f"not share a disk. Point it at object storage (s3://bucket/...)."
    ]


def check_lock_consistency(
    *,
    topology: DeploymentTopology,
    lock_uri: str,
    lock_label: str,
) -> list[str]:
    """Return a fatal issue when a multi-process deployment uses a no-op lock.

    The coordination lock makes scheduled and backfill work single-runner across
    processes. Whenever the topology is multi-process, an in-process ``noop://``
    lock coordinates nothing, so every process would run every interval job (the
    retention prune, a backfill, an upstream sync). The profile-aware default
    already resolves to ``postgres-advisory://`` under ``distributed``, so this
    trips on an explicit ``lock_uri="noop://"`` override, or on a ``local``
    default paired with several processes. A single-process deployment has
    nothing to coordinate, so ``noop`` stays valid there.

    Args:
        topology: The deployment topology.
        lock_uri: The resolved coordination-lock URI.
        lock_label: The setting backing the lock (for the message).

    Returns:
        A single-item issue list when misconfigured; empty otherwise. The
        ``custom`` profile is exempt — it promises no defaults.
    """
    if topology.profile is DeploymentProfile.CUSTOM or not topology.multi_process:
        return []
    if not lock_uri.strip().lower().startswith("noop://"):
        return []
    return [
        f"{lock_label} resolves to an in-process no-op lock, but this deployment runs multiple "
        f"processes ({topology.multi_process_reason}) that would each run scheduled jobs. Point it at "
        f"the shared database lock (postgres-advisory://)."
    ]


def check_mail_consistency(
    *,
    topology: DeploymentTopology,
    mail_uri: str | None,
    mail_label: str,
) -> list[str]:
    """Return a fatal issue when a multi-process deployment records mail in memory.

    ``memory://`` keeps each message in the sending process's outbox and never
    delivers it. In a single process that is a test fixture; in a multi-process
    deployment — including a standalone job worker, whose handlers are where mail
    is sent — it is production mail silently going nowhere. Unset mail is not
    flagged: it raises on every send, so it cannot drop a message unnoticed.

    Args:
        topology: The deployment topology.
        mail_uri: The configured mail URI, or ``None`` when mail is unset.
        mail_label: The setting backing mail (for the message).

    Returns:
        A single-item issue list when misconfigured; empty otherwise. The
        ``custom`` profile is exempt — it promises no defaults.
    """
    if topology.profile is DeploymentProfile.CUSTOM or not mail_uri or not topology.multi_process:
        return []
    # ``build_platform`` accepts only the literal ``scheme://`` form, so this is
    # the same test the builder applies.
    if mail_uri.partition("://")[0].lower() != "memory":
        return []
    return [
        f"{mail_label} resolves to the in-memory mail recorder, but this deployment runs multiple "
        f"processes ({topology.multi_process_reason}), and no message would ever be delivered. Point it "
        f"at a relay (smtp:// or smtps://), or leave it unset."
    ]


def check_job_worker_consistency(
    *,
    topology: DeploymentTopology,
    jobs_enabled: bool,
) -> list[str]:
    """Return a fatal issue when standalone job workers are declared with jobs off.

    ``job_worker_mode=standalone`` tells every process that other processes
    consume durable jobs, which makes the other rules stricter. With
    ``jobs.enabled`` false the jobs layer is dormant, so no such consumer can
    exist and the declaration would only impose constraints nothing justifies.

    Args:
        topology: The deployment topology.
        jobs_enabled: Whether the durable-job pipeline is enabled.

    Returns:
        A single-item issue list when contradictory; empty otherwise.
    """
    if topology.job_worker_mode is not JobWorkerMode.STANDALONE or jobs_enabled:
        return []
    return [
        "job_worker_mode=standalone declares standalone job workers, but jobs.enabled is false, "
        "so the durable-job pipeline is dormant and no worker can consume anything. Enable "
        "jobs, or leave job_worker_mode at in_process."
    ]
