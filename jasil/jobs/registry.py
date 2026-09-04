"""Registry mapping a durable ``subscriber_id`` to its handler and event type.

A durable subscriber declares a stable id (independent of its Python module
path) and the event type it reacts to, and registers a handler here. The relay
uses the event-type mapping to fan an event out into one job per subscriber; the
worker uses the id mapping to resolve a claimed job back to its handler. The same
code therefore runs subscribers in-process or out-of-process in a worker.
"""

import re
from collections import defaultdict
from collections.abc import Callable, Iterable

from jasil._core.limits import check_length
from jasil.events import MAX_EVENT_TYPE_LENGTH, Event

JobHandler = Callable[[Event], None]

#: Width of the ``processing_jobs.subscriber_id`` column, imported by the model
#: so the two cannot drift.
MAX_SUBSCRIBER_ID_LENGTH = 200

#: Queue assigned to registrations and jobs that do not select one explicitly.
DEFAULT_QUEUE = "default"

#: Width of ``processing_jobs.queue``. Queue names are configuration identifiers,
#: so they are rejected rather than truncated before they can reach a database.
MAX_QUEUE_NAME_LENGTH = 100

_QUEUE_NAME_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]*\Z")


def validate_queue_name(queue: str) -> str:
    """Validate and return one portable durable-job queue identifier.

    Queue names are deliberately lowercase ASCII so equality and ordering have
    the same meaning under SQLite, PostgreSQL, and MySQL collations.

    Args:
        queue: Queue identifier to validate.

    Returns:
        The unchanged queue name.

    Raises:
        ValueError: When the queue is empty, overlong, or contains unsupported
            characters.
    """
    if not isinstance(queue, str):
        raise ValueError("queue must be a string")
    check_length(queue, field="queue", limit=MAX_QUEUE_NAME_LENGTH)
    if _QUEUE_NAME_PATTERN.fullmatch(queue) is None:
        raise ValueError(
            "queue must start with a lowercase letter or digit and contain only a-z, 0-9, '.', '_', or '-'"
        )
    return queue


def normalize_queue_selector(queues: Iterable[str] | None) -> tuple[str, ...] | None:
    """Validate an optional non-empty queue allowlist and remove duplicates."""
    if queues is None:
        return None
    values = (queues,) if isinstance(queues, str) else tuple(queues)
    if not values:
        raise ValueError("queues must be a non-empty allowlist when provided")
    return tuple(dict.fromkeys(validate_queue_name(queue) for queue in values))


class JobHandlerRegistry:
    """A process-local registry of durable subscribers."""

    def __init__(self) -> None:
        self._handlers: dict[str, JobHandler] = {}
        self._by_event_type: dict[str, list[str]] = defaultdict(list)
        self._queues: dict[str, str] = {}

    def register(
        self,
        event_type: str,
        subscriber_id: str,
        handler: JobHandler,
        *,
        queue: str = DEFAULT_QUEUE,
    ) -> None:
        """
        Register (or replace) a durable subscriber.

        Args:
            event_type: The domain-event channel the subscriber reacts to.
            subscriber_id: The stable durable-subscriber identifier.
            handler: The callable that processes an event for this subscriber.
            queue: Durable-job queue assigned to this subscriber's fan-out.

        Returns:
            None.

        Raises:
            ValueError: When an identifier is invalid for the column it is
                written to. Checked at registration — startup — rather than when
                the relay first tries to enqueue a job, where the failure would
                be far from its cause.
        """
        check_length(event_type, field="event_type", limit=MAX_EVENT_TYPE_LENGTH)
        check_length(subscriber_id, field="subscriber_id", limit=MAX_SUBSCRIBER_ID_LENGTH)
        validate_queue_name(queue)
        self._handlers[subscriber_id] = handler
        self._queues[subscriber_id] = queue
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

    def queue_for(self, subscriber_id: str) -> str:
        """Return the queue assigned to a registered subscriber.

        Raises:
            LookupError: When registry state has no queue for ``subscriber_id``.
                Registration always stores the handler and queue together, so
                this indicates inconsistent process-local state and must not be
                hidden by routing the job to another queue.
        """
        try:
            return self._queues[subscriber_id]
        except KeyError as error:
            raise LookupError(f"no queue registered for subscriber_id {subscriber_id!r}") from error

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

    def _subscriber_ids_for_queues(self, queues: Iterable[str]) -> tuple[str, ...]:
        """Return registered subscriber ids assigned to the selected queues."""
        selected = normalize_queue_selector(queues)
        if selected is None:
            return tuple(self._handlers)
        selected_set = frozenset(selected)
        return tuple(subscriber_id for subscriber_id, queue in self._queues.items() if queue in selected_set)

    def clear(self) -> None:
        """Remove every registration (used to reset state between tests)."""
        self._handlers.clear()
        self._by_event_type.clear()
        self._queues.clear()


# Process-wide default registry: durable subscribers register here at startup and
# the worker/relay resolve handlers and fan-out from it.
registry = JobHandlerRegistry()
