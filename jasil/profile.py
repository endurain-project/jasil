"""Deployment profile and capability-topology resolution.

Pure module — no I/O and no infrastructure imports. It answers two questions
the rest of the platform substrate builds on:

- **How is this deployment shaped?** ``DeploymentProfile`` (``local`` /
  ``distributed`` / ``custom``), the configured number of web workers, and
  whether durable jobs run inside the API processes or in standalone worker
  processes (``JobWorkerMode``).
- **What does that shape imply?** ``DeploymentTopology.multi_process`` (two
  processes can reach the same capability) and ``DeploymentTopology.multi_node``
  (they may be on different machines). ``resolve_topology`` is the one place that
  knows which inputs imply which fact, so a new kind of process is a single line
  there and every consistency rule picks it up.

It also classifies a storage URI as memory- or Redis-backed so callers can
check the effective wiring against the required shape. Keeping this logic free
of infrastructure imports is what lets :mod:`jasil.settings` and
:mod:`jasil.capabilities` build on it without an import cycle.
"""

from dataclasses import dataclass
from enum import StrEnum


class DeploymentProfile(StrEnum):
    """How a deployment is shaped.

    Attributes:
        LOCAL: Single process/node — in-memory state, local disk, in-process
            events. The default; requires no extra infrastructure.
        DISTRIBUTED: Multi-node — requires shared state (Redis), object storage,
            and cross-process coordination.
        CUSTOM: No profile defaults; every capability must be set explicitly.
    """

    LOCAL = "local"
    DISTRIBUTED = "distributed"
    CUSTOM = "custom"


class JobWorkerMode(StrEnum):
    """Where durable-job consumers run.

    A fact about the deployment's shape, not a count: it does not change when
    workers are scaled, and every process of a deployment loads the same value, so
    the API replicas and the workers agree on the shape without coordinating.

    Attributes:
        IN_PROCESS: Each API process starts its own consumer with
            ``jasil.jobs.service.start_job_worker``. The default.
        STANDALONE: Consumers are dedicated processes started with
            ``jasil.jobs.service.run_job_worker``; API processes only schedule
            the relay and the reaper. Every such process is another process that
            reaches every capability.
    """

    IN_PROCESS = "in_process"
    STANDALONE = "standalone"


class StateBackendKind(StrEnum):
    """Classification of a state storage URI.

    Attributes:
        MEMORY: Process-local memory (``memory://``).
        REDIS: Redis-backed (``redis://`` / ``rediss://`` / ``unix://``).
        UNKNOWN: Unrecognized or unset scheme.
    """

    MEMORY = "memory"
    REDIS = "redis"
    UNKNOWN = "unknown"


_MEMORY_SCHEMES = ("memory://",)
_REDIS_SCHEMES = ("redis://", "rediss://", "unix://")


def parse_profile(value: str | DeploymentProfile | None) -> DeploymentProfile:
    """Parse a raw profile value into a ``DeploymentProfile``.

    Args:
        value: A profile name (case and surrounding space ignored), a profile, or ``None``.

    Returns:
        The parsed profile; ``DeploymentProfile.LOCAL`` when unset or empty.

    Raises:
        ValueError: When the value is a non-empty, unrecognized profile name.
            Raising (rather than defaulting) prevents a typo like
            ``distributd`` from silently running the ``local`` profile and
            disabling the shared-state fail-fast.
    """
    if isinstance(value, DeploymentProfile):
        return value
    if value is None:
        return DeploymentProfile.LOCAL
    normalized = value.strip().lower()
    if not normalized:
        return DeploymentProfile.LOCAL
    try:
        return DeploymentProfile(normalized)
    except ValueError as err:
        valid = ", ".join(profile.value for profile in DeploymentProfile)
        raise ValueError(f"Invalid deployment profile '{value}'. Valid values: {valid}.") from err


def parse_job_worker_mode(value: str | JobWorkerMode | None) -> JobWorkerMode:
    """Parse a raw job-worker-mode value into a ``JobWorkerMode``.

    Args:
        value: A mode name (case and surrounding space ignored), a mode, or ``None``.

    Returns:
        The parsed mode; ``JobWorkerMode.IN_PROCESS`` when unset or empty.

    Raises:
        ValueError: When the value is a non-empty, unrecognized mode name.
            Raising (rather than defaulting) prevents a typo like ``standalon``
            from silently running ``in_process`` and hiding every worker process
            from the consistency checks.
    """
    if isinstance(value, JobWorkerMode):
        return value
    if value is None:
        return JobWorkerMode.IN_PROCESS
    normalized = value.strip().lower()
    if not normalized:
        return JobWorkerMode.IN_PROCESS
    try:
        return JobWorkerMode(normalized)
    except ValueError as err:
        valid = ", ".join(mode.value for mode in JobWorkerMode)
        raise ValueError(f"Invalid job worker mode '{value}'. Valid values: {valid}.") from err


def classify_state_uri(storage_uri: str | None) -> StateBackendKind:
    """Classify a state storage URI as memory / redis / unknown.

    Args:
        storage_uri: A storage URI (e.g. ``memory://`` or ``redis://host:6379/0``).

    Returns:
        The matching ``StateBackendKind``; ``UNKNOWN`` when unset or unrecognized.
    """
    if storage_uri is None:
        return StateBackendKind.UNKNOWN
    normalized = storage_uri.strip().lower()
    if normalized.startswith(_MEMORY_SCHEMES):
        return StateBackendKind.MEMORY
    if normalized.startswith(_REDIS_SCHEMES):
        return StateBackendKind.REDIS
    return StateBackendKind.UNKNOWN


@dataclass(frozen=True)
class DeploymentTopology:
    """Resolved deployment shape.

    Attributes:
        profile: The deployment profile.
        web_workers: Number of web-server worker processes (always >= 1).
        job_worker_mode: Where durable-job consumers run.
    """

    profile: DeploymentProfile
    web_workers: int
    job_worker_mode: JobWorkerMode

    @property
    def multi_process_causes(self) -> tuple[str, ...]:
        """What makes this topology multi-process; empty for a single process.

        Each entry is a short phrase naming the setting behind it, so a refusal
        can tell an operator *why* their deployment counts as multi-process.
        """
        causes: list[str] = []
        if self.profile is DeploymentProfile.DISTRIBUTED:
            causes.append("profile=distributed runs replicas on separate nodes")
        if self.web_workers > 1:
            causes.append(f"web_workers={self.web_workers} runs several API processes")
        if self.job_worker_mode is JobWorkerMode.STANDALONE:
            causes.append("job_worker_mode=standalone runs job workers in their own processes")
        return tuple(causes)

    @property
    def multi_process_reason(self) -> str:
        """:attr:`multi_process_causes` joined into one phrase for a refusal message."""
        return "; ".join(self.multi_process_causes)

    @property
    def multi_process(self) -> bool:
        """Whether two or more processes can reach the same capability.

        True for the ``distributed`` profile, for more than one web worker, and
        whenever durable-job consumers run as standalone processes. Process-local
        memory, an in-process lock or a recording mail backend cannot be shared
        between them.
        """
        return bool(self.multi_process_causes)

    @property
    def multi_node(self) -> bool:
        """Whether those processes may run on different machines.

        Only the ``distributed`` profile promises that. Several processes of a
        ``local`` deployment share one host, and with it one disk.
        """
        return self.profile is DeploymentProfile.DISTRIBUTED


def resolve_topology(
    *,
    profile: DeploymentProfile,
    web_workers: int,
    job_worker_mode: JobWorkerMode,
) -> DeploymentTopology:
    """Resolve the deployment topology, clamping ``web_workers`` to >= 1.

    Args:
        profile: The deployment profile.
        web_workers: Configured web worker count.
        job_worker_mode: Where durable-job consumers run.

    Returns:
        The resolved ``DeploymentTopology``.
    """
    return DeploymentTopology(profile=profile, web_workers=max(1, web_workers), job_worker_mode=job_worker_mode)
