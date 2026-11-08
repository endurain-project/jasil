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
correlation id. Work that runs outside the request it belongs to (a durable job
handler) opens a :func:`correlation_scope`, which wins over both.
"""

import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar

__all__ = [
    "configure_provider",
    "correlation_scope",
    "get_correlation_id",
    "reset",
    "set_correlation_id",
]

logger = logging.getLogger(__name__)

_provider_failing = False

_correlation_id: ContextVar[str | None] = ContextVar("jasil_correlation_id", default=None)

# Set only by correlation_scope(): an explicit scope, which wins over an installed provider. The id is wrapped in a
# tuple so that a scope of ``None`` ("this work has no id") is told apart from no scope at all.
_scope: ContextVar[tuple[str | None] | None] = ContextVar("jasil_correlation_scope", default=None)

_provider: Callable[[], str | None] | None = None


def configure_provider(provider: Callable[[], str | None] | None) -> None:
    """Install a host callable that returns the current correlation id.

    Call once at startup. Passing ``None`` restores the built-in contextvar.

    Args:
        provider: Returns the ambient correlation id as a ``str``, or ``None`` when
            there is none (e.g. outside a request). Any other return type is treated as
            "no id" and logged, see :func:`get_correlation_id`.
    """
    global _provider
    _provider = provider


def set_correlation_id(value: str | None) -> None:
    """Set the correlation id for the current context.

    Only consulted while no provider is installed via :func:`configure_provider` and no
    :func:`correlation_scope` block is open.

    Args:
        value: The id to stamp on events minted in this context.
    """
    _correlation_id.set(value)


def _warn_provider_failing(problem: str, type_name: str) -> None:
    """Log that the installed provider misbehaved, once until it works again.

    Args:
        problem: What the provider did, e.g. ``raised``.
        type_name: The exception or returned type, never a value.
    """
    global _provider_failing
    if _provider_failing:
        return
    _provider_failing = True
    logger.warning("The correlation id provider %s %s; events and audit rows carry no request id", problem, type_name)


def get_correlation_id() -> str | None:
    """Return the ambient correlation id, or ``None``.

    Never raises: a provider that fails is treated as "no id", because a
    correlation id is diagnostic metadata and must not break publishing. So is one
    that returns anything but a ``str`` or ``None``, since an event's request id must
    be a string. The failure is logged once, by exception or returned type and never
    by value, until the provider works again, so a permanently broken one does not
    strip the id from every event unseen.
    """
    global _provider_failing
    scope = _scope.get()
    if scope is not None:
        return scope[0]
    if _provider is not None:
        try:
            correlation_id = _provider()
        except Exception as error:
            _warn_provider_failing("raised", type(error).__name__)
            return None
        if correlation_id is not None and not isinstance(correlation_id, str):
            _warn_provider_failing("returned a non-str", type(correlation_id).__name__)
            return None
        _provider_failing = False
        return correlation_id
    return _correlation_id.get()


@contextmanager
def correlation_scope(value: str | None) -> Generator[None]:
    """Run a block under ``value`` as the correlation id, restoring the previous one afterwards.

    For work that runs outside the request it belongs to, such as a durable job handler: the block's id wins over
    an installed provider and over :func:`set_correlation_id`, neither of which knows about that request. ``None``
    means the block has no id, so it does not pick up one left over from the thread or returned by the provider.

    Args:
        value: The id to stamp on events and audit rows minted inside the block, or ``None`` for none.

    Yields:
        None.
    """
    token = _scope.set((value,))
    try:
        yield
    finally:
        _scope.reset(token)


def reset() -> None:
    """Clear the installed provider, the current context's id and any open :func:`correlation_scope`."""
    global _provider, _provider_failing
    _provider = None
    _provider_failing = False
    _correlation_id.set(None)
    _scope.set(None)
