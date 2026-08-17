"""Correlation-id seam for stamping events with the ambient request id.

Events carry a correlation id so a downstream failure can be traced back to the
request that triggered it. Where that id comes from is the host's business: a
web framework's request middleware, a task-queue header, or nothing at all.

By default the id lives in a module-local :class:`~contextvars.ContextVar` that
the host sets with :func:`set_correlation_id`. A host that already tracks one
(request-id middleware, an OpenTelemetry span) installs a reader instead::

    import jasil.correlation as correlation
    correlation.configure_provider(my_middleware.get_request_id)

Both paths are optional; with neither configured, events simply carry no
correlation id.
"""

from collections.abc import Callable
from contextvars import ContextVar

__all__ = [
    "configure_provider",
    "get_correlation_id",
    "reset",
    "set_correlation_id",
]

_correlation_id: ContextVar[str | None] = ContextVar("jasil_correlation_id", default=None)

_provider: Callable[[], str | None] | None = None


def configure_provider(provider: Callable[[], str | None] | None) -> None:
    """Install a host callable that returns the current correlation id.

    Call once at startup. Passing ``None`` restores the built-in contextvar.

    Args:
        provider: Returns the ambient correlation id, or ``None`` when there is
            none (e.g. outside a request).
    """
    global _provider
    _provider = provider


def set_correlation_id(value: str | None) -> None:
    """Set the correlation id for the current context.

    Only consulted while no provider is installed via :func:`configure_provider`.

    Args:
        value: The id to stamp on events minted in this context.
    """
    _correlation_id.set(value)


def get_correlation_id() -> str | None:
    """Return the ambient correlation id, or ``None``.

    Never raises: a provider that fails is treated as "no id", because a
    correlation id is diagnostic metadata and must not break publishing.
    """
    if _provider is not None:
        try:
            return _provider()
        except Exception:
            return None
    return _correlation_id.get()


def reset() -> None:
    """Clear the installed provider and the current context's id."""
    global _provider
    _provider = None
    _correlation_id.set(None)
