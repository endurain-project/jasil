"""Validation for the ``(area, key)`` pair every storage backend is addressed by.

Both backends take the same two caller-supplied segments, so both have to reject
the same values — otherwise a host that validated its inputs against local disk
in development finds a different contract in production. Local paths normalize
dot components and separators that object-storage keys preserve literally, so
portable addresses must already be canonical before either backend sees them.

Kept here rather than on either backend: neither should have to import the other
to share a rule they both enforce.
"""

UPLOAD_STAGING_AREA = ".jasil-upload-sessions"

__all__ = ["UPLOAD_STAGING_AREA", "check_area", "check_listing_prefix", "check_segment"]


def check_area(area: str) -> None:
    """Validate an area and reject JASIL's private upload staging name."""
    check_segment(area, "area")
    if area.split("/", 1)[0] == UPLOAD_STAGING_AREA:
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
            component, or is not a canonical slash-delimited path.
    """
    if not value:
        # An empty segment collapses the address onto the container itself, so
        # ``save("", "")`` would target the storage root.
        raise ValueError(f"Storage {label} must not be empty")
    normalized_parts = value.replace("\\", "/").split("/")
    if value.startswith(("/", "\\")) or ".." in normalized_parts:
        raise ValueError(f"Storage {label} escapes base directory: {value!r}")
    if "\\" in value or any(part in {"", "."} for part in normalized_parts):
        raise ValueError(f"Storage {label} must be a canonical slash-delimited path: {value!r}")
