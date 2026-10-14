"""Run callbacks after a session's OUTERMOST transaction commits.

SQLAlchemy's ``after_commit`` also fires when a SAVEPOINT is released, and
``after_rollback`` never fires for a SAVEPOINT rollback, so neither alone says
"the work is durable". This tracks each pending callback against the
(sub)transaction that was current when it was registered and settles it when
that transaction ends:

* a released savepoint hands its callbacks to its parent;
* a rolled-back (or closed) transaction discards its callbacks;
* the root transaction's commit runs whatever is left, in registration order.

State lives in ``Session.info``, so concurrent sessions never see each other's
callbacks. Listeners are attached to the session *instance* on first use, so a
session that never registers a callback pays nothing.
"""

import logging
from collections.abc import Callable
from typing import Any

from sqlalchemy import event
from sqlalchemy.orm import Session, SessionTransaction

logger = logging.getLogger(__name__)

__all__ = ["run_after_commit"]

_PENDING = "jasil.after_commit.pending"
_COMMITTED = "jasil.after_commit.committed"


def run_after_commit(db: Session, callback: Callable[[], None]) -> None:
    """Call ``callback`` once ``db``'s outermost transaction commits.

    Starts the session's transaction if none is active, so the callback has a
    transaction to belong to. ``callback`` runs after the commit has succeeded,
    with the session between transactions; it must not raise (a raised error is
    logged and swallowed so it cannot reach the committer).

    Args:
        db: The session whose commit gates the callback.
        callback: Zero-arg callable to run after the outermost commit.
    """
    if _PENDING not in db.info:
        db.info[_PENDING] = []
        event.listen(db, "after_commit", _on_commit)
        event.listen(db, "after_transaction_end", _on_transaction_end)
    if not db.in_transaction():
        db.begin()
    transaction = db.get_nested_transaction() or db.get_transaction()
    db.info[_PENDING].append((transaction, callback))


def _on_commit(session: Session) -> None:
    # Fires for savepoint releases too, always immediately before the matching
    # after_transaction_end, which is where the outcome is acted on.
    session.info[_COMMITTED] = True


def _on_transaction_end(session: Session, transaction: SessionTransaction) -> None:
    committed = session.info.pop(_COMMITTED, False)
    pending: list[tuple[SessionTransaction, Callable[[], None]]] = session.info[_PENDING]
    if not pending:
        return

    if transaction.parent is not None:
        # Savepoint: keep (re-parent) on release, drop on rollback. Anything
        # registered deeper has already been re-parented onto this one.
        if committed:
            pending[:] = [(transaction.parent if tx is transaction else tx, cb) for tx, cb in pending]
        else:
            pending[:] = [(tx, cb) for tx, cb in pending if tx is not transaction]
        return

    ready: list[Callable[[], None]] = [cb for _, cb in pending] if committed else []
    # Cleared before running so a callback that registers more work (or a later
    # commit) can never replay this batch.
    pending.clear()
    for callback in ready:
        _run(callback)


def _run(callback: Callable[[], Any]) -> None:
    try:
        callback()
    except Exception as err:
        logger.error("After-commit callback failed: %s", err, exc_info=err)
