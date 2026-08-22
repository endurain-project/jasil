"""Session helpers shared by the write paths.

Kept out of the CRUD modules because the choice they encode is a contract, not a
detail: a write that takes ``commit=False`` must land in the *caller's* open
transaction, so a domain change and the events it produces commit together or
not at all. Three call sites make that choice, and they have to make it the same
way.

The async twin exists for the same reason and makes the same choice; it is a
separate function rather than a mode flag because a helper that returns
"maybe a coroutine" would push the ambiguity into every caller.
"""

from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

__all__ = ["commit_or_flush", "commit_or_flush_async"]


def commit_or_flush(db: Session, commit: bool) -> None:
    """End a write by committing it, or by leaving it in the caller's transaction.

    Args:
        db: The session the write was made on.
        commit: True to commit now. False to flush only, so the write becomes
            visible to the rest of the caller's transaction but is not durable
            until the caller commits.
    """
    if commit:
        db.commit()
    else:
        db.flush()


async def commit_or_flush_async(db: "AsyncSession", commit: bool) -> None:
    """End an async write by committing it, or leaving it in the caller's transaction.

    The asynchronous counterpart of :func:`commit_or_flush`, with identical
    semantics; only the execution differs.

    Args:
        db: The async session the write was made on.
        commit: True to commit now. False to flush only, so the write becomes
            visible to the rest of the caller's transaction but is not durable
            until the caller commits.
    """
    if commit:
        await db.commit()
    else:
        await db.flush()
