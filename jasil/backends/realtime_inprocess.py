"""In-process (synchronous) ``RealtimeProvider`` backend."""

import logging
from collections.abc import Callable

import jasil._core.realtime_envelope as realtime_envelope

logger = logging.getLogger(__name__)


class InProcessRealtime:
    """``RealtimeProvider`` for a single process — ``notify_user`` calls the handlers inline.

    The ``memory://`` backend and the ``local`` profile default. Each hint is
    validated and round-tripped through the same JSON envelope the Redis backend
    puts on the wire, so a handler sees exactly what it would in production and a
    hint Redis would refuse is refused here too. A handler that raises is logged
    and the next one still runs: a push failing never fails the publisher.

    ``start`` / ``stop`` are no-ops because there is nothing to receive from.

    Args:
        max_bytes: Cap on the serialised envelope.
    """

    def __init__(self, *, max_bytes: int = realtime_envelope.DEFAULT_MAX_ENVELOPE_BYTES) -> None:
        self._max_bytes = max_bytes
        self._handlers = realtime_envelope.HintHandlers()

    def notify_user(self, user_id: int, hint: dict) -> None:
        """Validate the hint, then deliver it to every handler on this thread.

        Raises:
            ValueError: When the hint is invalid (see
                :class:`~jasil.providers.RealtimeProvider`).
        """
        decoded = realtime_envelope.decode(realtime_envelope.encode(user_id, hint, max_bytes=self._max_bytes))
        if decoded is not None:
            self._handlers.deliver(*decoded)

    def subscribe(self, handler: Callable[[int, dict], None]) -> None:
        self._handlers.add(handler)

    def clear(self) -> None:
        """Drop every registered handler (for test isolation)."""
        self._handlers.clear()

    def start(self) -> None:
        """No-op — in-process delivery needs no subscriber."""

    def stop(self) -> None:
        """No-op — in-process delivery needs no subscriber."""
