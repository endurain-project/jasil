"""Helpers for writing event-bus subscribers.

Every domain subscriber in the codebase is written twice: a raising core (the
durable-job handler, so the runner can retry and eventually dead-letter) and a
swallowing wrapper (the bus subscriber, so a derived-work failure never breaks
the producing request). :func:`best_effort` is that wrapper, defined once here
instead of being copy-pasted per subsystem — which previously meant each copy
could drift in what it logged and which exceptions it caught.

:func:`best_effort_async` is the same wrapper for coroutine handlers. It is a
separate function rather than one that inspects its argument, because a decorator
that returns a coroutine function only sometimes is a decorator whose result you
cannot reason about at the call site. The two log identically — the whole point
of having the wrapper in one module is that the log line is the same everywhere,
and that has to survive the second face.
"""

import functools
import logging
from collections.abc import Awaitable, Callable

from jasil.events import Event

__all__ = ["best_effort", "best_effort_async"]

logger = logging.getLogger(__name__)


def _log_subscriber_failure(error: Exception, event: Event, subscriber_name: str) -> None:
    """Log a swallowed subscriber failure.

    Shared by both wrappers so the sync and async faces produce byte-identical
    log records; an operator grepping for a failing subscriber should not have to
    know which face raised it.

    Args:
        error: The exception the handler raised.
        event: The event being processed.
        subscriber_name: The handler's name.
    """
    logger.error(
        "Event subscriber failed",
        exc_info=error,
        extra={
            "event_type": event.event_type,
            "event_id": event.event_id,
            "subscriber": subscriber_name,
            # The whole correlation dict: which keys matter is the host's
            # to decide, so none are singled out here. This is why
            # ``metadata`` must never carry a secret.
            "event_metadata": event.metadata,
        },
    )


def best_effort(handler: Callable[[Event], None]) -> Callable[[Event], None]:
    """Wrap a raising event handler into a swallowing bus subscriber.

    The returned subscriber logs and absorbs **any** exception, so derived work
    can never fail the request or consumer that produced the event. The wrapped
    handler stays available for durable-job registration, where failures must
    propagate to drive retry and dead-lettering.

    Usage::

        def do_work_for_event(event: Event) -> None: ...

        on_event_do_work = best_effort(do_work_for_event)

    Args:
        handler: The raising handler to wrap.

    Returns:
        A subscriber with the same signature that never raises.
    """

    @functools.wraps(handler)
    def _subscriber(event: Event) -> None:
        try:
            handler(event)
        except Exception as err:
            _log_subscriber_failure(err, event, handler.__name__)

    return _subscriber


def best_effort_async(handler: Callable[[Event], Awaitable[None]]) -> Callable[[Event], Awaitable[None]]:
    """Wrap a raising coroutine event handler into a swallowing bus subscriber.

    The asynchronous counterpart of :func:`best_effort`, with identical logging
    and the same guarantee: the returned subscriber absorbs **any** exception, so
    derived work can never fail the request or consumer that produced the event.
    The wrapped handler stays available for durable-job registration, where
    failures must propagate to drive retry and dead-lettering.

    Usage::

        async def do_work_for_event(event: Event) -> None: ...

        on_event_do_work = best_effort_async(do_work_for_event)

    Args:
        handler: The raising coroutine handler to wrap.

    Returns:
        A coroutine subscriber with the same signature that never raises.
    """

    @functools.wraps(handler)
    async def _subscriber(event: Event) -> None:
        try:
            await handler(event)
        except Exception as err:
            _log_subscriber_failure(err, event, handler.__name__)

    return _subscriber
