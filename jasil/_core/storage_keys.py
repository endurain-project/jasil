"""Validation for the ``(area, key)`` pair every storage backend is addressed by.

Both backends take the same two caller-supplied segments, so both have to reject
the same values — otherwise a host that validated its inputs against local disk
in development finds a different contract in production. Local paths normalize
dot components and separators that object-storage keys preserve literally, so
portable addresses must already be canonical before either backend sees them.

Kept here rather than on either backend: neither should have to import the other
to share a rule they both enforce.
"""

OBJECT_STORAGE_AREA = ".jasil-objects"
UPLOAD_STAGING_AREA = ".jasil-upload-sessions"
# Every area under this prefix is JASIL's, compared case-insensitively: on a case-insensitive filesystem
# ``.JASIL-OBJECTS`` is the same directory as ``.jasil-objects``.
_RESERVED_AREA_PREFIX = ".jasil-"
#: The longest object key S3 accepts, in UTF-8 bytes; local disk is held to it too so both refuse the same keys.
MAX_KEY_BYTES = 1024

__all__ = [
    "MAX_KEY_BYTES",
    "OBJECT_STORAGE_AREA",
    "UPLOAD_STAGING_AREA",
    "check_area",
    "check_key_length",
    "check_listing_prefix",
    "check_segment",
]


def check_area(area: str) -> None:
    """Validate an area and reject JASIL's private storage roots."""
    check_segment(area, "area")
    if "/" in area:
        raise ValueError(f"Storage area must be a single path component: {area!r}")
    if area.casefold().startswith(_RESERVED_AREA_PREFIX):
        raise ValueError(f"Storage area is reserved: {area!r}")


def check_listing_prefix(prefix: str) -> None:
    """Validate a lexical key filter, allowing one meaningful trailing slash."""
    candidate = prefix[:-1] if prefix.endswith("/") and prefix != "/" else prefix
    check_segment(candidate, "prefix")


def check_segment(value: str, label: str) -> None:
    """Reject an empty, non-canonical, absolute, or traversing segment.

    Pure — touches no filesystem and no client, so it runs identically on every
    backend and before anything is dialled.

    Args:
        value: The area, key, or key prefix supplied by the caller.
        label: What ``value`` is, for the error message.

    Raises:
        ValueError: When the segment is empty, absolute, contains a ``..``
            component or a control character, is not a canonical slash-delimited
            path, or is longer than :data:`MAX_KEY_BYTES` in UTF-8.
    """
    if not value:
        # An empty segment collapses the address onto the container itself, so
        # ``save("", "")`` would target the storage root.
        raise ValueError(f"Storage {label} must not be empty")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        # S3 cannot list or batch-delete such a key (XML 1.0 has no way to carry most of them), and a local
        # path cannot hold a NUL.
        raise ValueError(f"Storage {label} must not contain a control character: {value!r}")
    check_key_length(value, label)
    normalized_parts = value.replace("\\", "/").split("/")
    if value.startswith(("/", "\\")) or ".." in normalized_parts:
        raise ValueError(f"Storage {label} escapes base directory: {value!r}")
    if "\\" in value or any(part in {"", "."} for part in normalized_parts):
        raise ValueError(f"Storage {label} must be a canonical slash-delimited path: {value!r}")


def check_key_length(value: str, label: str) -> None:
    """Reject a value longer than :data:`MAX_KEY_BYTES` in UTF-8.

    Raises:
        ValueError: When ``value`` is too long.
    """
    if len(value.encode("utf-8")) > MAX_KEY_BYTES:
        raise ValueError(f"Storage {label} must be at most {MAX_KEY_BYTES} bytes in UTF-8")
