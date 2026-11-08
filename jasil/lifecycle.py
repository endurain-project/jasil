"""Ordered shutdown of everything JASIL owns in a process.

Two things outlive a request and have to be wound down deliberately: the
durable-job worker thread, and the platform's own resources — the event-bus
consumer thread, the realtime subscriber thread and the shared Redis clients. **The order matters.** The worker
runs subscribers, and a subscriber that publishes needs the bus still up, so the
worker stops first.

They are two calls underneath because :mod:`jasil.container` must not import
:mod:`jasil.jobs`: the jobs layer binds the ORM models, which a deployment that
never enables durable jobs has not mapped, and the platform has to build
without them. This module is the seam that composes them, so the ordering
lives in one place instead of in every host::

    import jasil.lifecycle as jasil_lifecycle

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        ...
        yield
        jasil_lifecycle.shutdown()

What it deliberately leaves alone is what the *host* owns: the APScheduler
instance passed to ``schedule_job_maintenance``, and the database engine behind
the session factory. JASIL never created either, so it does not close them.
"""

import logging

import jasil.runtime as platform_runtime

__all__ = ["shutdown"]

logger = logging.getLogger(__name__)


def shutdown() -> None:
    """Stop the durable-job worker, release the platform, and unpublish it.

    Idempotent, and safe to call when nothing was ever started — a process that
    only ever published events has no worker and may have no platform.

    Never raises. Shutdown runs while something else is often already going
    wrong, and a failure to release a connection must not mask it; each step
    logs and continues instead.

    When the worker's thread does not stop in time because a handler is still
    running, the platform that handler uses is left open and published, and a
    warning is logged. Exiting the process releases it, as does calling this
    again once the handler has returned.

    Returns:
        None.
    """
    if not _stop_job_worker():
        # A handler is still running on the worker thread, and it uses the platform: closing or unpublishing it
        # now would fail that handler and cost its job an attempt. Exiting the process releases it, and so does
        # a later call once the worker has stopped.
        logger.warning("The durable-job worker did not stop in time; the platform stays open for its running handler")
        return
    _close_platform()
    platform_runtime.reset()


def _stop_job_worker() -> bool:
    """Stop the in-process durable-job worker, if this deployment has one.

    Returns:
        False when the worker's thread is still running a handler after its stop
        timed out; True otherwise, including when stopping it raised.
    """
    try:
        import jasil.jobs.service as jobs_service
    except ImportError:
        # The jobs layer cannot be imported at all (a partial install), so no worker can be running.
        return True
    try:
        return jobs_service.stop_job_worker()
    except Exception as error:
        logger.warning("Failed to stop the durable-job worker during shutdown: %r", error)
        return True


def _close_platform() -> None:
    """Release the platform's own resources, if one was ever published."""
    if not platform_runtime.is_platform_active():
        return
    try:
        platform_runtime.get_active_platform().close()
    except Exception as error:
        logger.warning("Failed to close the platform during shutdown: %r", error)
