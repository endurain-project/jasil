"""0.19: every timestamp column keeps microseconds on MySQL and MariaDB, as it already does elsewhere.

On these servers ``DateTime(timezone=True)`` was a plain ``DATETIME``, which keeps whole seconds and rounds the
fraction away, so a stored timestamp could not be compared for equality with the one written: an audit row's
``occurred_at`` (which ``get_entry`` and the keyset cursors look up) or a cursor's position. Each JASIL table's
timestamp columns become ``DATETIME(6)`` in one ``ALTER TABLE`` per table, restating each column's nullability,
comment and default, which becomes ``CURRENT_TIMESTAMP(6)``. Rows keep their values: widening adds a zero fraction.
Nothing changes on other databases: PostgreSQL's ``timestamptz`` already keeps microseconds and SQLite stores text.

Changing a column's type rebuilds the table, and on these servers that blocks writes to it until the rebuild
finishes; ``jasil_audit_log`` keeps its daily ``RANGE COLUMNS`` partitions on ``occurred_at``, its append-only
triggers (a rebuild fires none) and the procedures, none of which depends on the column's precision. DDL commits
on these servers, so a run that failed part-way has converted some tables; running it again converts them all.
Processes of the previous release keep working against the converted columns during a rolling deploy.

Downgrading makes the columns ``DATETIME`` again and rounds every stored fraction to the nearest second (truncates
it under the ``TIME_TRUNCATE_FRACTIONAL`` SQL mode). That changes the rows a checkpoint hashed, so a window sealed
over rows with a fraction then reports ``row_mismatch``, and an audit row in the last half second of a day moves to
the next day's partition.

Revision ID: rev0012
Revises: rev0011
Create Date: 2026-11-08
"""

from collections.abc import Sequence

from alembic import op

from jasil.migrations._names import PREFIX

revision: str = "rev0012"
down_revision: str | None = "rev0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Per table, every timestamp column as (name, nullable, has the current time as its default, comment).
_TIMESTAMPS: dict[str, tuple[tuple[str, bool, bool, str], ...]] = {
    f"{PREFIX}event_log": (
        ("created_at", False, True, "When the event was published"),
        ("processed_at", True, False, "When a consumer picked the event up"),
        ("completed_at", True, False, "When processing finished (success or failure)"),
    ),
    f"{PREFIX}event_outbox": (
        ("created_at", False, True, "When the event was written to the outbox"),
        ("relayed_at", True, False, "When the relay fanned the event out; NULL while pending"),
    ),
    f"{PREFIX}processing_jobs": (
        ("available_at", False, True, "Earliest instant the job may be claimed (backoff gate)"),
        ("locked_at", True, False, "When the current lease was taken"),
        ("lease_expires_at", True, False, "When the current lease expires (drives reaping)"),
        ("created_at", False, True, "When the job was enqueued"),
        ("updated_at", False, True, "When the job last changed state"),
        ("completed_at", True, False, "When the job reached a terminal state (completed/dead_letter)"),
        (
            "dead_letter_notified_at",
            True,
            False,
            "When the on_dead_letter callback last succeeded; NULL while one is owed",
        ),
        (
            "dead_letter_notify_after",
            True,
            False,
            "Earliest instant an on_dead_letter delivery may be claimed (lease or backoff)",
        ),
    ),
    f"{PREFIX}job_workers": (
        ("started_at", False, False, "When this worker instance started"),
        ("last_heartbeat_at", False, False, "Most recent successful worker heartbeat"),
        ("stopped_at", True, False, "When this worker stopped gracefully; NULL after a crash"),
    ),
    f"{PREFIX}job_attempts": (
        ("claimed_at", False, False, "When the worker took the lease"),
        ("started_at", True, False, "When the handler began; NULL if it never ran"),
        ("finished_at", True, False, "When the attempt ended; NULL while open"),
    ),
    f"{PREFIX}audit_log": (("occurred_at", False, True, "When the action was staged; partition key"),),
    f"{PREFIX}audit_actions": (
        ("first_seen_at", False, True, "First synchronization"),
        ("last_seen_at", False, True, "Last synchronization that saw it"),
    ),
    f"{PREFIX}audit_checkpoints": (
        ("window_start", False, False, "Window start, inclusive"),
        ("window_end", False, False, "Window end, exclusive"),
        ("created_at", False, True, "When the checkpoint was written"),
    ),
    f"{PREFIX}instance": (
        ("created_at", False, True, "When the id was generated"),
        ("last_checkpoint_at", True, False, "End of the latest sealed window"),
    ),
}


def _mysql() -> bool:
    """Whether the target is MySQL or MariaDB, which a ``mariadb+`` URL names differently."""
    return op.get_bind().dialect.name in ("mysql", "mariadb")


def _alter_statements(precision: str) -> list[str]:
    """Return one ``ALTER TABLE`` per table giving its timestamp columns the type ``DATETIME{precision}``.

    ``MODIFY COLUMN`` replaces the whole column definition, so each restates the column's nullability, default
    and comment.
    """
    statements = []
    for table, columns in _TIMESTAMPS.items():
        clauses = []
        for name, nullable, defaulted, comment in columns:
            default = f" DEFAULT CURRENT_TIMESTAMP{precision}" if defaulted else ""
            nullability = "NULL" if nullable else "NOT NULL"
            clauses.append(f"MODIFY COLUMN {name} DATETIME{precision} {nullability}{default} COMMENT '{comment}'")
        statements.append(f"ALTER TABLE {table} " + ", ".join(clauses))
    return statements


def upgrade() -> None:
    if _mysql():
        for statement in _alter_statements("(6)"):
            op.execute(statement)


def downgrade() -> None:
    if _mysql():
        for statement in _alter_statements(""):
            op.execute(statement)
