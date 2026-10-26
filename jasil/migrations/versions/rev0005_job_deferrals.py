"""Add ``processing_jobs.deferrals``, the claims a handler deferred with ``DeferJob``.

``attempts`` keeps counting every claim, because it is the token that fences a
claim, while a deferred claim must not count against ``max_attempts``. The budget
spent is ``attempts - deferrals``. Existing rows start at 0, which is what they
have always meant. Previous-release processes neither read nor write the column.

Revision ID: rev0005
Revises: rev0004
Create Date: 2026-10-26
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from jasil.migrations._names import PREFIX

revision: str = "rev0005"
down_revision: str | None = "rev0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_JOBS = f"{PREFIX}processing_jobs"


def upgrade() -> None:
    op.add_column(
        _JOBS,
        sa.Column(
            "deferrals",
            sa.Integer,
            nullable=False,
            server_default="0",
            comment="Claims the handler deferred with DeferJob; they do not count against max_attempts",
        ),
    )


def downgrade() -> None:
    op.drop_column(_JOBS, "deferrals")
