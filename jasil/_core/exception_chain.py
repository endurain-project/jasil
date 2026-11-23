"""Walking the exceptions that led to an error, as a traceback shows them."""

from collections.abc import Iterator

__all__ = ["exception_chain"]


def exception_chain(error: BaseException) -> Iterator[BaseException]:
    """Yield ``error`` and then each exception that led to it, nearest first.

    Each step follows ``__cause__`` (``raise ... from``), or else ``__context__`` (raised while handling another)
    unless ``raise ... from None`` suppressed it, which is the chain a traceback prints. A cycle ends the walk.

    Args:
        error: The exception to start from.

    Yields:
        ``error``, then its cause or context, then theirs.
    """
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        if current.__cause__ is not None:
            current = current.__cause__
        elif current.__suppress_context__:
            current = None
        else:
            current = current.__context__
