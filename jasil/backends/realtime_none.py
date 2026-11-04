"""Disabled ``RealtimeProvider`` backend (``realtime_uri=none://``)."""

import logging
from collections.abc import Callable

import jasil._core.realtime_envelope as realtime_envelope
from jasil.providers import CapabilityDisabledError

logger = logging.getLogger(__name__)


class NullRealtime:
    """``RealtimeProvider`` for a deployment with no realtime channel.

    ``notify`` validates the hint exactly as the other backends do, so a
    programmer error surfaces in every deployment, then drops it. ``subscribe``
    raises, because a handler registered here would never be called.

    Args:
        max_bytes: Cap on the serialised envelope.
    """

    def __init__(self, *, max_bytes: int = realtime_envelope.DEFAULT_MAX_ENVELOPE_BYTES) -> None:
        self._max_bytes = max_bytes

    def notify(self, key: str, hint: dict) -> None:
        """Validate the hint, then drop it.

        Raises:
            ValueError: When the key or hint is invalid, or the encoded envelope
                exceeds the configured byte cap (see
                :class:`~jasil.providers.RealtimeProvider`).
        """
        realtime_envelope.encode(key, hint, max_bytes=self._max_bytes)
        logger.debug("Realtime is disabled; dropped a hint")

    def subscribe(self, handler: Callable[[str, dict], None]) -> None:
        """Refuse the subscription: the handler would never be called.

        Raises:
            CapabilityDisabledError: Always.
        """
        raise CapabilityDisabledError(
            "Realtime is disabled (realtime_uri, or events_uri when it is unset, is none://), "
            "so a subscriber would never be called"
        )

    def start(self) -> None:
        """No-op — there is nothing to receive from."""

    def stop(self) -> None:
        """No-op — there is nothing to receive from."""
