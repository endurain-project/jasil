"""Create the ``jasil_instance`` row, so the first audit write never has to.

Until now the row was created by the first audited write, on a session of its own. On SQLite that session
waits for the write lock the caller's transaction already holds and fails with ``database is locked``. A
database migrated to this revision holds the row from the start. A row that already exists is kept, so the
deployment's id does not change. A script rendered with ``--sql`` seeds nothing: its id would be one literal that
every database the script runs on shares, so those databases create the row on their first audited write. The downgrade leaves the row: previous-release processes read it and create
it only when it is missing, and removing it would give the deployment a new id on the next upgrade.

Revision ID: rev0010
Revises: rev0009
Create Date: 2026-11-04
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from jasil._core.uuid7 import uuid7
from jasil.migrations._names import PREFIX

revision: str = "rev0010"
down_revision: str | None = "rev0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INSTANCE = f"{PREFIX}instance"


def upgrade() -> None:
    if op.get_context().as_sql:
        return
    instance = sa.table(_INSTANCE, sa.column("id", sa.Integer), sa.column("instance_id", sa.String))
    op.execute(
        sa.insert(instance).from_select(
            ["id", "instance_id"],
            sa.select(sa.literal(1), sa.literal(str(uuid7()))).where(~sa.exists().select_from(instance)),
        )
    )


def downgrade() -> None:
    """Leave the row in place; see the module docstring."""
