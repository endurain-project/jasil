"""Session helpers shared by the write paths.

Kept out of the CRUD modules because the choice they encode is a contract, not a
detail: a write that takes ``commit=False`` must land in the *caller's* open
transaction, so a domain change and the events it produces commit together or
not at all. Three call sites make that choice, and they have to make it the same
way.
"""

from sqlalchemy.orm import Session

__all__ = ["commit_or_flush"]


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
