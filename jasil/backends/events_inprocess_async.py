"""Async in-process ``AsyncEventBusProvider`` backend.

This is the async twin of :mod:`jasil.backends.events_inprocess`, mirroring it
class-for-class and method-for-method. The file exists separately (rather than
adding ``async`` methods to the sync class) because the two faces of every
provider are deliberately disjoint: a single object that sometimes returns a
value and sometimes an awaitable makes both callers worse, and a host may run
the sync and async platforms simultaneously.

**Inline dispatch — why ``publish`` awaits handlers directly:**

``AsyncInProcessEventBus.publish`` awaits each handler's coroutine *inside*
``publish`` itself, in registration order, rather than scheduling them as
:class:`asyncio.Task` objects. This preserves the synchronous bus's ordering
guarantee: when ``publish`` returns (or when its caller awaits it), every
handler has already run. That guarantee matters in two places:

1. The async event recorder uses ``record_processing=False`` for in-process
   dispatch because the intermediate "processing" state is never externally
   observable — dispatch is sequential and completes before ``publish`` returns.
   Task-based dispatch would make the intermediate state real, waste two
   recorder writes per event, and still not give the caller any signal that
   handlers have finished.

2. Any code that reads state written by a handler after ``await publish(...)``
   relies on read-your-writes. Task-based dispatch would make that a data race.

The tradeoff is that a slow handler blocks the event loop for its duration. That
is acceptable for the ``local`` profile this backend targets: the handlers are
fast domain computations, not I/O, and if they were slow the fix is to switch to
the Redis backend rather than to make the in-process path confusingly concurrent.
"""

import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from jasil.events import Event

if TYPE_CHECKING:
    from jasil.providers_async import AsyncEventRecorder

logger = logging.getLogger(__name__)

# Worker label recorded for in-process dispatch (single process, no consumer).
_INPROCESS_WORKER = "inprocess"


class AsyncInProcessEventBus:
    """Async ``AsyncEventBusProvider`` — ``publish`` awaits subscribers inline.

    Correct for the ``local`` profile: dispatch is a sequential series of
    ``await`` calls in the same task, and a handler exception propagates to the
    caller of ``publish``. ``start`` / ``stop`` are no-ops because there is no
    background consumer task.

    When an :class:`~jasil.providers_async.AsyncEventRecorder` is injected, each
    ``publish`` records the event's lifecycle (published -> completed/failed)
    around the inline dispatch in two writes: the intermediate ``processing``
    state is skipped because dispatch is synchronous and single-process, so that
    state is never observed. A handler exception is recorded as a failure and
    then re-raised, preserving the propagate-to-caller contract.
    """

    def __init__(self, recorder: "AsyncEventRecorder | None" = None) -> None:
        self._handlers: dict[str, list[Callable[[Event], Awaitable[None]]]] = defaultdict(list)
        self._recorder = recorder

    async def publish(self, event: Event) -> None:
        """Dispatch ``event`` to all registered handlers, inline and in order.

        Handlers are awaited sequentially, not scheduled as tasks. See the module
        docstring for why this ordering guarantee is intentional.

        Args:
            event: The event to dispatch. Its ``event_type`` selects which
                handlers are called.

        Raises:
            Exception: Re-raised from any handler that raises, after the recorder
                (if present) has marked the event as failed.
        """
        handlers = self._handlers.get(event.event_type, [])
        recorder = self._recorder
        if recorder is None:
            for handler in handlers:
                await handler(event)
            return
        await recorder.record_published(event)
        handler_name = ",".join(handler.__name__ for handler in handlers) or None
        # record_processing=False: the intermediate state is never observed
        # because dispatch completes before publish() returns (see module docstring).
        async with recorder.track(
            event, worker_id=_INPROCESS_WORKER, handler_name=handler_name, record_processing=False
        ):
            for handler in handlers:
                await handler(event)

    def subscribe(self, event_type: str, handler: Callable[[Event], Awaitable[None]]) -> None:
        """Register ``handler`` to be called whenever an event of ``event_type`` is published.

        Args:
            event_type: The ``Event.event_type`` string that triggers ``handler``.
            handler: An async callable accepting an :class:`~jasil.events.Event`.

        Returns:
            None.
        """
        self._handlers[event_type].append(handler)

    async def start(self) -> None:
        """No-op — in-process dispatch needs no consumer loop."""

    async def stop(self) -> None:
        """No-op — in-process dispatch needs no consumer loop."""
