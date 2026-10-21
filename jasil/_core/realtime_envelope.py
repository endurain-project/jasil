"""The wire envelope shared by every ``RealtimeProvider`` backend.

A hint travels as compact JSON ``{"v": 1, "user_id": <int>, "hint": {...}}``.
Both backends encode through :func:`encode` — the in-process one too, although
nothing leaves the process — so a hint that is not JSON, or is too large, fails
in a local test exactly as it would against Redis, and the handler receives the
same decoded value (tuples as lists, keys as strings) either way.

Validation failures are programmer errors and raise ``ValueError``;
:func:`decode` never raises, because what arrives on a shared channel is not
under this process's control.
"""

import json
import logging
import threading
from collections.abc import Callable
from typing import Any, TypeGuard

__all__ = ["DEFAULT_MAX_ENVELOPE_BYTES", "ENVELOPE_VERSION", "HintHandlers", "decode", "encode"]

logger = logging.getLogger(__name__)

#: The only envelope version this build reads or writes.
ENVELOPE_VERSION = 1

#: Default cap on the serialised envelope, in bytes. A hint says *what* changed,
#: not the data itself; anything near this size belongs in a refetch.
DEFAULT_MAX_ENVELOPE_BYTES = 4096


def _is_user_id(value: object) -> TypeGuard[int]:
    # ``bool`` is an ``int`` subclass; ``True`` is never a user id.
    return isinstance(value, int) and not isinstance(value, bool)


def encode(user_id: int, hint: dict, *, max_bytes: int = DEFAULT_MAX_ENVELOPE_BYTES) -> bytes:
    """Validate and serialise one hint.

    Args:
        user_id: The user the hint is for.
        hint: A JSON-serialisable mapping.
        max_bytes: Cap on the serialised envelope.

    Returns:
        The UTF-8 JSON envelope.

    Raises:
        ValueError: When ``user_id`` is not an ``int``, ``hint`` is not a
            ``dict`` or not JSON-serialisable (including NaN / infinity), or the
            envelope exceeds ``max_bytes``.
    """
    if not _is_user_id(user_id):
        raise ValueError(f"user_id must be an int, not {type(user_id).__name__}")
    if not isinstance(hint, dict):
        raise ValueError(f"hint must be a dict, not {type(hint).__name__}")
    envelope = {"v": ENVELOPE_VERSION, "user_id": user_id, "hint": hint}
    try:
        encoded = json.dumps(envelope, separators=(",", ":"), allow_nan=False, ensure_ascii=False).encode()
    except (TypeError, ValueError) as error:
        raise ValueError(f"hint is not JSON-serialisable: {error}") from None
    if len(encoded) > max_bytes:
        raise ValueError(f"realtime hint envelope is {len(encoded)} bytes, over the {max_bytes}-byte cap")
    return encoded


def decode(raw: str | bytes) -> tuple[int, dict[str, Any]] | None:
    """Parse one envelope, or log and return ``None`` when it is unusable.

    Args:
        raw: The payload as received.

    Returns:
        ``(user_id, hint)``, or ``None`` for a malformed payload or an unknown
        envelope version.
    """
    try:
        envelope = json.loads(raw)
    except (TypeError, ValueError):
        logger.warning("Ignoring a realtime hint that is not valid JSON")
        return None
    if not isinstance(envelope, dict):
        logger.warning("Ignoring a realtime hint whose envelope is not an object")
        return None
    version = envelope.get("v")
    if version != ENVELOPE_VERSION:
        logger.warning("Ignoring a realtime hint with unsupported envelope version %r", version)
        return None
    user_id, hint = envelope.get("user_id"), envelope.get("hint")
    if not _is_user_id(user_id) or not isinstance(hint, dict):
        logger.warning("Ignoring a malformed realtime hint envelope")
        return None
    return user_id, hint


class HintHandlers:
    """The handler registry and fan-out both backends share.

    Registration is copy-on-write under a lock, so :meth:`deliver` iterates a
    stable snapshot without holding it.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._handlers: tuple[Callable[[int, dict], None], ...] = ()

    def add(self, handler: Callable[[int, dict], None]) -> None:
        """Register ``handler``."""
        with self._lock:
            self._handlers = (*self._handlers, handler)

    def clear(self) -> None:
        """Drop every handler."""
        with self._lock:
            self._handlers = ()

    def deliver(self, user_id: int, hint: dict) -> None:
        """Call every handler; one that raises is logged and the rest still run."""
        for handler in self._handlers:
            try:
                handler(user_id, hint)
            except Exception as error:  # one handler must not stop the rest, nor the caller
                logger.error("Realtime hint handler %r failed", handler, exc_info=error)
