"""In-process (synchronous) ``EventBusProvider`` backend."""

from collections import defaultdict
from collections.abc import Callable
from typing import TYPE_CHECKING

from jasil.events import Event

if TYPE_CHECKING:
    from jasil.providers import EventRecorder

# Worker label recorded for in-process dispatch (single process, no consumer).
_INPROCESS_WORKER = "inprocess"


class InProcessEventBus:
    """Synchronous ``EventBusProvider`` — ``publish`` runs subscribers inline.

    Correct for the ``local`` profile: dispatch is a direct function call in the
    same thread. Every handler runs even when one raises; the failure then
    propagates to the caller (several together as an ``ExceptionGroup``), and the
    scheduler backfill is the safety net. ``start`` / ``stop`` are no-ops because
    there is no background consumer.

    When an :class:`~jasil.providers.EventRecorder` is injected, each
    ``publish`` records the event's lifecycle (published -> completed/failed)
    around the inline dispatch in two writes: the intermediate ``processing``
    state is skipped because dispatch is synchronous and single-process, so that
    state is never observed. A handler exception is recorded as a failure and
    then re-raised, preserving the propagate-to-caller contract.
    """

    def __init__(self, recorder: "EventRecorder | None" = None) -> None:
        self._handlers: dict[str, list[Callable[[Event], None]]] = defaultdict(list)
        self._recorder = recorder

    def publish(self, event: Event) -> None:
        """Run the handlers subscribed to ``event.event_type`` inline, in subscription order.

        Raises:
            Exception: The handler's error when one handler fails, or an ``ExceptionGroup`` when several do, after
                every handler has run.
        """
        handlers = self._handlers.get(event.event_type, [])
        recorder = self._recorder
        if recorder is None:
            _call_each(handlers, event)
            return
        recorder.record_published(event)
        handler_name = ",".join(getattr(handler, "__name__", repr(handler)) for handler in handlers) or None
        with recorder.track(event, worker_id=_INPROCESS_WORKER, handler_name=handler_name, record_processing=False):
            _call_each(handlers, event)

    def subscribe(self, event_type: str, handler: Callable[[Event], None]) -> None:
        """Register ``handler`` for ``event_type``."""
        self._handlers[event_type].append(handler)

    def start(self) -> None:
        """No-op — in-process dispatch needs no consumer loop."""

    def stop(self) -> None:
        """No-op — in-process dispatch needs no consumer loop."""


def _call_each(handlers: list[Callable[[Event], None]], event: Event) -> None:
    """Call every handler, then raise what failed: the error itself, or an ``ExceptionGroup`` of several."""
    errors = []
    for handler in handlers:
        try:
            handler(event)
        except Exception as error:  # one subscriber must not keep the event from the others
            errors.append(error)
    if len(errors) == 1:
        raise errors[0]
    if errors:
        raise ExceptionGroup(f"{len(errors)} of {len(handlers)} handlers failed for {event.event_type}", errors)
