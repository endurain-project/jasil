"""In-process (synchronous) ``RealtimeProvider`` backend."""

import json
import logging
import threading
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
        self._lock = threading.Lock()
        self._handlers: tuple[Callable[[int, dict], None], ...] = ()

    def notify_user(self, user_id: int, hint: dict) -> None:
        """Validate the hint, then deliver it to every handler on this thread.

        Raises:
            ValueError: When the hint is invalid (see
                :class:`~jasil.providers.RealtimeProvider`).
        """
        delivered = json.loads(realtime_envelope.encode(user_id, hint, max_bytes=self._max_bytes))["hint"]
        for handler in self._handlers:
            try:
                handler(user_id, delivered)
            except Exception as error:  # one handler must not stop the rest, nor the publisher
                logger.error("Realtime hint handler %r failed", handler, exc_info=error)

    def subscribe(self, handler: Callable[[int, dict], None]) -> None:
        with self._lock:
            self._handlers = (*self._handlers, handler)

    def clear(self) -> None:
        """Drop every registered handler (for test isolation)."""
        with self._lock:
            self._handlers = ()

    def start(self) -> None:
        """No-op — in-process delivery needs no subscriber."""

    def stop(self) -> None:
        """No-op — in-process delivery needs no subscriber."""
