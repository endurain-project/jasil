"""Registry mapping a durable ``subscriber_id`` to its handler and event type.

A durable subscriber declares a stable id (independent of its Python module
path) and the event type it reacts to, and registers a handler here. The relay
uses the event-type mapping to fan an event out into one job per subscriber; the
worker uses the id mapping to resolve a claimed job back to its handler. The same
code therefore runs subscribers in-process or out-of-process in a worker.

There are two registries, one per face, because the handler *types* differ: an
``async def`` handler returns a coroutine, not ``None``, so storing it in the sync
registry would need a cast that moves the type error from startup to the middle of
a worker loop. They are independent singletons and a subscriber belongs to exactly
one of them.

Both live here, in a module that imports no model and no session, because
:mod:`jasil.publisher` consults them to decide whether an event goes to the
durable outbox or on the bus — and it must be able to do that without dragging the
ORM into ``import jasil.publisher``.
"""

from collections import defaultdict
from collections.abc import Awaitable, Callable

from jasil._core.limits import check_length
from jasil.events import MAX_EVENT_TYPE_LENGTH, Event

JobHandler = Callable[[Event], None]

#: The async counterpart: a coroutine function taking one event.
AsyncJobHandler = Callable[[Event], Awaitable[None]]

#: Width of the ``processing_jobs.subscriber_id`` column, imported by the model
#: so the two cannot drift.
MAX_SUBSCRIBER_ID_LENGTH = 200


class JobHandlerRegistry:
    """A process-local registry of durable subscribers."""

    def __init__(self) -> None:
        self._handlers: dict[str, JobHandler] = {}
        self._by_event_type: dict[str, list[str]] = defaultdict(list)

    def register(self, event_type: str, subscriber_id: str, handler: JobHandler) -> None:
        """
        Register (or replace) a durable subscriber.

        Args:
            event_type: The domain-event channel the subscriber reacts to.
            subscriber_id: The stable durable-subscriber identifier.
            handler: The callable that processes an event for this subscriber.

        Returns:
            None.

        Raises:
            ValueError: When ``event_type`` or ``subscriber_id`` is longer than
                the ``processing_jobs`` column it is written to. Checked at
                registration — startup — rather than when the relay first tries
                to enqueue a job, where the failure would be far from its cause.
        """
        check_length(event_type, field="event_type", limit=MAX_EVENT_TYPE_LENGTH)
        check_length(subscriber_id, field="subscriber_id", limit=MAX_SUBSCRIBER_ID_LENGTH)
        self._handlers[subscriber_id] = handler
        if subscriber_id not in self._by_event_type[event_type]:
            self._by_event_type[event_type].append(subscriber_id)

    def get(self, subscriber_id: str) -> JobHandler | None:
        """
        Look up the handler for a subscriber id.

        Args:
            subscriber_id: The durable-subscriber identifier to resolve.

        Returns:
            The registered handler, or ``None`` when none is registered.
        """
        return self._handlers.get(subscriber_id)

    def subscribers_for(self, event_type: str) -> tuple[str, ...]:
        """
        Return the durable subscriber ids registered for an event type.

        Args:
            event_type: The domain-event channel to fan out.

        Returns:
            The subscriber ids, in registration order.
        """
        return tuple(self._by_event_type.get(event_type, ()))

    def subscriber_ids(self) -> frozenset[str]:
        """
        Return every registered subscriber id, across all event types.

        Returns:
            The registered durable-subscriber ids.
        """
        return frozenset(self._handlers)

    def clear(self) -> None:
        """Remove every registration (used to reset state between tests)."""
        self._handlers.clear()
        self._by_event_type.clear()


# Process-wide default registry: durable subscribers register here at startup and
# the worker/relay resolve handlers and fan-out from it.
registry = JobHandlerRegistry()


class AsyncJobHandlerRegistry:
    """A process-local registry of durable async subscribers.

    Structurally identical to :class:`jasil.jobs.registry.JobHandlerRegistry`
    but typed for coroutine handlers rather than synchronous ones.  A separate
    class is necessary because the sync registry stores
    ``Callable[[Event], None]``; an ``async def`` handler returns a coroutine,
    not ``None``, and accepting it would require a silent mypy cast in every
    registration call.  Keeping the two registries independent surfaces the type
    mismatch at ``register`` time — during startup — not at dispatch time, deep
    inside a worker loop.

    Both the relay and the runner receive this registry at construction, so the
    subscriber IDs used for fan-out and the handlers used for dispatch are always
    drawn from the same source of truth.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, AsyncJobHandler] = {}
        self._by_event_type: dict[str, list[str]] = defaultdict(list)

    def register(self, event_type: str, subscriber_id: str, handler: AsyncJobHandler) -> None:
        """
        Register (or replace) a durable async subscriber.

        Args:
            event_type: The domain-event channel the subscriber reacts to.
            subscriber_id: The stable durable-subscriber identifier.
            handler: An async callable accepting an :class:`~jasil.events.Event`.

        Returns:
            None.

        Raises:
            ValueError: When ``event_type`` or ``subscriber_id`` exceeds the
                ``processing_jobs`` column width it is written to.  Checked at
                registration — startup — rather than when the relay first tries
                to enqueue, where the failure would be far from its cause.
        """
        check_length(event_type, field="event_type", limit=MAX_EVENT_TYPE_LENGTH)
        check_length(subscriber_id, field="subscriber_id", limit=MAX_SUBSCRIBER_ID_LENGTH)
        self._handlers[subscriber_id] = handler
        if subscriber_id not in self._by_event_type[event_type]:
            self._by_event_type[event_type].append(subscriber_id)

    def get(self, subscriber_id: str) -> AsyncJobHandler | None:
        """
        Look up the async handler for a subscriber id.

        Args:
            subscriber_id: The durable-subscriber identifier to resolve.

        Returns:
            The registered async handler, or ``None`` when none is registered.
        """
        return self._handlers.get(subscriber_id)

    def subscribers_for(self, event_type: str) -> tuple[str, ...]:
        """
        Return the durable subscriber ids registered for an event type.

        Args:
            event_type: The domain-event channel to fan out.

        Returns:
            The subscriber ids, in registration order.
        """
        return tuple(self._by_event_type.get(event_type, ()))

    def subscriber_ids(self) -> frozenset[str]:
        """
        Return every registered subscriber id, across all event types.

        Returns:
            The registered durable-subscriber ids.
        """
        return frozenset(self._handlers)

    def clear(self) -> None:
        """Remove every registration (used to reset state between tests)."""
        self._handlers.clear()
        self._by_event_type.clear()


#: Process-wide default async registry.  Async durable subscribers register here
#: at startup; the async relay and async runner resolve handlers and fan-out IDs
#: from it.
async_registry = AsyncJobHandlerRegistry()
