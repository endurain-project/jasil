"""0.19: an opaque, host-defined ``scope`` on audit rows, and the format each checkpoint was sealed in.

``jasil_audit_log`` gains a nullable ``scope`` and an index on ``(scope, occurred_at)``; on PostgreSQL both go on
the partitioned parent, so every partition has them and those created later get them too. Adding the column
rewrites no row, so the append-only triggers are not involved. ``jasil_audit_checkpoints`` gains
``format_version``, which defaults to 1: every checkpoint sealed before this revision hashed its rows without
``scope``, and so does one written by a previous-release process during a rolling deploy, which neither writes
the column nor sees ``scope``.

On PostgreSQL the index is built after the columns commit: the parent's ``ON ONLY``, then each partition's
``CONCURRENTLY``, attached as it is built, so audit writes continue throughout. A run that failed during the
build resumes there. Rendered with ``--sql`` the index is one plain ``CREATE INDEX``, which blocks writes to the
log while it runs.

Downgrading drops ``scope``, its values and ``format_version``: the previous release then verifies every
checkpoint as format 1, so a window sealed in format 2 reports ``row_mismatch``.

Revision ID: rev0011
Revises: rev0010
Create Date: 2026-11-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from jasil._core.limits import MAX_AUDIT_ID_LENGTH
from jasil.migrations import _online as online
from jasil.migrations._names import PREFIX

revision: str = "rev0011"
down_revision: str | None = "rev0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LOG = f"{PREFIX}audit_log"
_CHECKPOINTS = f"{PREFIX}audit_checkpoints"
_SCOPE_INDEX = f"idx_{_LOG}_scope"


def upgrade() -> None:
    if not online.resumed(_LOG, "scope"):
        op.add_column(
            _LOG,
            sa.Column(
                "scope",
                sa.String(MAX_AUDIT_ID_LENGTH),
                nullable=True,
                comment="Host-defined partition key; opaque to JASIL",
            ),
        )
        op.add_column(
            _CHECKPOINTS,
            sa.Column(
                "format_version", sa.Integer, nullable=False, server_default="1", comment="Row format the hash covers"
            ),
        )
    # Last: on PostgreSQL the build first commits the columns, so audit writes continue while it runs.
    online.create_partitioned_index(_SCOPE_INDEX, _LOG, ["scope", "occurred_at"])


def downgrade() -> None:
    op.drop_index(_SCOPE_INDEX, table_name=_LOG)
    op.drop_column(_CHECKPOINTS, "format_version")
    op.drop_column(_LOG, "scope")
