"""The key namespace JASIL reserves in a state store, and the ``delete_prefix`` rule both state backends enforce.

Kept here rather than on either backend: neither should have to import the other to share a rule they both
enforce, and the memory backend must refuse the same prefixes Redis does.
"""

#: Where the Redis backend keeps an index's member deadlines: this prefix plus the index key.
INDEX_EXPIRY_PREFIX = "jasil:state:index-expiry:"

__all__ = ["INDEX_EXPIRY_PREFIX", "check_delete_prefix"]


def check_delete_prefix(prefix: str) -> None:
    """Refuse a ``delete_prefix`` prefix that is empty or reaches the reserved index namespace.

    A prefix that starts the reserved namespace (``"jasil:"``) or lies inside it would delete index deadlines
    while keeping their indexes, whose members would then never expire on their own.

    Args:
        prefix: The prefix the caller asked to delete.

    Raises:
        ValueError: When ``prefix`` is empty or overlaps :data:`INDEX_EXPIRY_PREFIX`.
    """
    if not prefix:
        raise ValueError("delete_prefix needs a non-empty prefix")
    if INDEX_EXPIRY_PREFIX.startswith(prefix) or prefix.startswith(INDEX_EXPIRY_PREFIX):
        raise ValueError(f"delete_prefix cannot reach the reserved index namespace {INDEX_EXPIRY_PREFIX!r}")
