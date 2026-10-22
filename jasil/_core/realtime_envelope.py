"""The wire envelope shared by every ``RealtimeProvider`` backend.

A hint travels as compact JSON ``{"v": 2, "key": "<str>", "hint": {...}}``. The
``key`` names the audience and is opaque to JASIL — a non-empty string of at most
:data:`MAX_KEY_LENGTH` characters whose meaning belongs to the host.
Both backends encode through :func:`encode` — the in-process one too, although
nothing leaves the process — so a hint that is not JSON, or is too large, fails
in a local test exactly as it would against Redis, and the handler receives the
same decoded value (tuples as lists, keys as strings) either way.

Only version 2 is written. Version 1 (``{"v": 1, "user_id": <int>, "hint":
{...}}``, JASIL 0.7) is still read, its ``user_id`` arriving as the key
``str(user_id)``, so hints from 0.7 replicas still arrive during a rolling
upgrade (0.7 ignores version 2, so the reverse direction is lost until the
rollout finishes). Any other version is logged and ignored.

Validation failures are programmer errors and raise ``ValueError``;
:func:`decode` never raises, because what arrives on a shared channel is not
under this process's control.
"""

import json
import logging
import threading
from collections.abc import Callable
from typing import Any

__all__ = [
    "DEFAULT_MAX_ENVELOPE_BYTES",
    "ENVELOPE_VERSION",
    "LEGACY_ENVELOPE_VERSION",
    "MAX_KEY_LENGTH",
    "HintHandlers",
    "decode",
    "encode",
]

logger = logging.getLogger(__name__)

#: The envelope version this build writes.
ENVELOPE_VERSION = 2

#: The previous envelope version, still read so a rolling upgrade from 0.7 loses
#: no hints. Drop it once no supported release writes it.
LEGACY_ENVELOPE_VERSION = 1

#: Cap on the length of a hint key, in characters (code points, not bytes).
#: Re-exported publicly as ``jasil.providers.MAX_REALTIME_KEY_LENGTH``.
MAX_KEY_LENGTH = 256

#: Default cap on the serialised envelope, in bytes. A hint says *what* changed,
#: not the data itself; anything near this size belongs in a refetch.
DEFAULT_MAX_ENVELOPE_BYTES = 4096


def _key_problem(key: object) -> str | None:
    """Why ``key`` is not a valid hint key, or ``None`` when it is."""
    if not isinstance(key, str):
        return f"key must be a str, not {type(key).__name__}"
    if not key:
        return "key must not be empty"
    if len(key) > MAX_KEY_LENGTH:
        return f"key is {len(key)} characters, over the {MAX_KEY_LENGTH}-character cap"
    try:
        key.encode("utf-8")
    except UnicodeEncodeError:
        return "key contains an unpaired Unicode surrogate"
    return None


def encode(key: str, hint: dict, *, max_bytes: int = DEFAULT_MAX_ENVELOPE_BYTES) -> bytes:
    """Validate and serialise one hint.

    Args:
        key: The audience of the hint; opaque to JASIL.
        hint: A JSON-serialisable mapping.
        max_bytes: Cap on the serialised envelope.

    Returns:
        The UTF-8 JSON envelope.

    Raises:
        ValueError: When ``key`` is not a non-empty ``str`` of at most
            :data:`MAX_KEY_LENGTH` characters or holds an unpaired Unicode
            surrogate, ``hint`` is not a ``dict`` or not JSON-serialisable as
            UTF-8 (including NaN / infinity and unpaired surrogates), or the
            envelope exceeds ``max_bytes``.
    """
    problem = _key_problem(key)
    if problem is not None:
        raise ValueError(problem)
    if not isinstance(hint, dict):
        raise ValueError(f"hint must be a dict, not {type(hint).__name__}")
    envelope = {"v": ENVELOPE_VERSION, "key": key, "hint": hint}
    try:
        encoded = json.dumps(envelope, separators=(",", ":"), allow_nan=False, ensure_ascii=False).encode()
    except (TypeError, ValueError) as error:
        raise ValueError(f"hint is not JSON-serialisable: {error}") from None
    if len(encoded) > max_bytes:
        raise ValueError(f"realtime hint envelope is {len(encoded)} bytes, over the {max_bytes}-byte cap")
    return encoded


def decode(raw: str | bytes) -> tuple[str, dict[str, Any]] | None:
    """Parse one envelope, or log and return ``None`` when it is unusable.

    Args:
        raw: The payload as received.

    Returns:
        ``(key, hint)``, or ``None`` for a malformed payload or an unknown
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
    if type(version) is int and version == ENVELOPE_VERSION:
        key = envelope.get("key")
        key_ok = isinstance(key, str) and _key_problem(key) is None
    elif type(version) is int and version == LEGACY_ENVELOPE_VERSION:
        user_id = envelope.get("user_id")
        # ``bool`` is an ``int`` subclass; ``True`` was never a user id.
        key = str(user_id) if isinstance(user_id, int) and not isinstance(user_id, bool) else None
        # A v1 id is already a validated int; the v2 key cap does not apply to it.
        key_ok = key is not None
    else:
        logger.warning("Ignoring a realtime hint with unsupported envelope version %r", version)
        return None
    hint = envelope.get("hint")
    if not key_ok or not isinstance(hint, dict) or not _encodes(hint):
        logger.warning("Ignoring a malformed realtime hint envelope")
        return None
    return key, hint


def _encodes(hint: dict) -> bool:
    # JSON can carry an escaped unpaired surrogate (``"\ud800"``) that ``encode``
    # would refuse, and ``NaN`` / ``Infinity`` parse but ``encode`` rejects them;
    # drop both here so no handler sees a hint no backend could send.
    try:
        json.dumps(hint, ensure_ascii=False, allow_nan=False).encode()
    except (UnicodeEncodeError, ValueError):
        return False
    return True


class HintHandlers:
    """The handler registry and fan-out both backends share.

    Registration is copy-on-write under a lock, so :meth:`deliver` iterates a
    stable snapshot without holding it.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._handlers: tuple[Callable[[str, dict], None], ...] = ()

    def add(self, handler: Callable[[str, dict], None]) -> None:
        """Register ``handler``."""
        with self._lock:
            self._handlers = (*self._handlers, handler)

    def clear(self) -> None:
        """Drop every handler."""
        with self._lock:
            self._handlers = ()

    def deliver(self, key: str, hint: dict) -> None:
        """Call every handler; one that raises is logged and the rest still run."""
        for handler in self._handlers:
            try:
                handler(key, hint)
            except Exception as error:  # one handler must not stop the rest, nor the caller
                logger.error("Realtime hint handler %r failed", handler, exc_info=error)
