"""Disabled ``EventBusProvider`` backend (``events_uri=none://``)."""

import logging
from collections.abc import Callable

from jasil.events import Event
from jasil.providers import CapabilityDisabledError

logger = logging.getLogger(__name__)


class NullEventBus:
    """``EventBusProvider`` for a deployment with no event bus.

    ``publish`` drops the event, so the best-effort route of
    :func:`jasil.publisher.publish` stays a harmless no-op; durable delivery goes
    through the outbox and never touches the bus. ``subscribe`` raises, because
    a best-effort subscriber registered here would never run.
    """

    def publish(self, event: Event) -> None:
        """Drop ``event`` after a DEBUG log."""
        logger.debug("The event bus is disabled; dropped %s (%s)", event.event_type, event.event_id)

    def subscribe(self, event_type: str, handler: Callable[[Event], None]) -> None:
        """Refuse the subscription: the handler would never run.

        Raises:
            CapabilityDisabledError: Always.
        """
        raise CapabilityDisabledError(
            "The event bus is disabled (events_uri=none://), so a best-effort subscriber would never run; "
            "durable subscribers are unaffected"
        )

    def start(self) -> None:
        """No-op — there is no consumer loop."""

    def stop(self) -> None:
        """No-op — there is no consumer loop."""
