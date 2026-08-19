"""Validation for the ``(area, key)`` pair every storage backend is addressed by.

Both backends take the same two caller-supplied segments, so both have to reject
the same values — otherwise a host that validated its inputs against local disk
in development finds a different contract in production. The local backend needs
this because a segment escapes the base directory; the object-storage backend
needs it because ``..`` is a literal character in an S3 key, so a traversing value
is silently stored under a nonsense key rather than refused.

Kept here rather than on either backend: neither should have to import the other
to share a rule they both enforce.
"""

from pathlib import PurePosixPath

__all__ = ["check_segment"]


def check_segment(value: str, label: str) -> None:
    """Reject an empty, absolute, or parent-traversing storage segment.

    Pure — touches no filesystem and no client, so it runs identically on every
    backend and before anything is dialled.

    Args:
        value: The area, key, or key prefix supplied by the caller.
        label: What ``value`` is, for the error message.

    Raises:
        ValueError: When the segment is empty, absolute, or contains a ``..``
            component.
    """
    if not value:
        # An empty segment collapses the address onto the container itself, so
        # ``save("", "")`` would target the storage root.
        raise ValueError(f"Storage {label} must not be empty")
    if value.startswith(("/", "\\")) or ".." in PurePosixPath(value.replace("\\", "/")).parts:
        raise ValueError(f"Storage {label} escapes base directory: {value!r}")
