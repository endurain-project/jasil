"""Ordered shutdown of everything JASIL owns in a process.

Two things outlive a request and have to be wound down deliberately: the
durable-job worker thread, and the platform's own resources — the event-bus
consumer thread and the shared Redis clients. **The order matters.** The worker
runs subscribers, and a subscriber that publishes needs the bus still up, so the
worker stops first.

They are two calls underneath because :mod:`jasil.container` must not import
:mod:`jasil.jobs`: the jobs layer imports APScheduler at module scope, and a
deployment that never enables durable jobs has to be able to build a platform
without the ``jobs`` extra installed at all. This module is the seam that
composes them, so the ordering lives in one place instead of in every host::

    import jasil.lifecycle as jasil_lifecycle

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        ...
        yield
        jasil_lifecycle.shutdown()

What it deliberately leaves alone is what the *host* owns: the APScheduler
instance passed to ``schedule_job_maintenance``, and the database engine behind
the session factory. JASIL never created either, so it does not close them.

:func:`ashutdown` does the same for the async face, and — this is the whole point
of the module — in the same order. The ordering rule is not a property of threads;
it is a property of the dependency between the worker and the bus, and it holds
identically when both are tasks. A host that got the order right synchronously and
wrong asynchronously would see subscribers fail at shutdown for reasons that look
like nothing at all.
"""

import logging

import jasil.runtime as platform_runtime

__all__ = ["ashutdown", "shutdown"]

logger = logging.getLogger(__name__)


def shutdown() -> None:
    """Stop the durable-job worker, release the platform, and unpublish it.

    Idempotent, and safe to call when nothing was ever started — a process that
    only ever published events has no worker and may have no platform.

    Never raises. Shutdown runs while something else is often already going
    wrong, and a failure to release a connection must not mask it; each step
    logs and continues instead.

    Returns:
        None.
    """
    _stop_job_worker()
    _close_platform()
    platform_runtime.reset()


def _stop_job_worker() -> None:
    """Stop the in-process durable-job worker, if this deployment has one."""
    try:
        import jasil.jobs.service as jobs_service
    except ImportError:
        # The ``jobs`` extra is not installed, so no worker can be running.
        return
    try:
        jobs_service.stop_job_worker()
    except Exception as error:
        logger.warning("Failed to stop the durable-job worker during shutdown: %r", error)


def _close_platform() -> None:
    """Release the platform's own resources, if one was ever published."""
    if not platform_runtime.is_platform_active():
        return
    platform_runtime.get_active_platform().close()


async def ashutdown() -> None:
    """Stop the async durable-job worker, release the async platform, and unpublish it.

    The asynchronous counterpart of :func:`shutdown`, preserving the same
    ordering: the worker runs subscribers, and a subscriber that publishes needs
    the bus still up, so the worker stops first.

    Idempotent, and safe to call when nothing was ever started. Never raises —
    shutdown runs while something else is often already going wrong, and a failure
    to release a connection must not mask it; each step logs and continues.

    Note this unpublishes *both* platform slots, because
    :func:`jasil.runtime.reset` clears both. A process running an async API beside
    a synchronous worker should call :func:`shutdown` and :func:`ashutdown` in
    turn; the second one's reset is then a no-op.

    Returns:
        None.
    """
    await _stop_async_job_worker()
    await _close_async_platform()
    platform_runtime.reset()


async def _stop_async_job_worker() -> None:
    """Stop the in-process async durable-job worker, if this deployment has one."""
    try:
        import jasil.jobs.service as jobs_service
    except ImportError:
        # The ``jobs`` extra is not installed, so no worker can be running.
        return
    try:
        await jobs_service.stop_async_job_worker()
    except Exception as error:
        logger.warning("Failed to stop the async durable-job worker during shutdown: %r", error)


async def _close_async_platform() -> None:
    """Release the async platform's own resources, if one was ever published."""
    if not platform_runtime.is_async_platform_active():
        return
    await platform_runtime.get_active_async_platform().aclose()
