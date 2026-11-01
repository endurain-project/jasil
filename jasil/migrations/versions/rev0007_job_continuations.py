"""Add ``processing_jobs.continuations`` and ``budget_offset``, for handlers that end a pass with ``ContinueJob``.

``continuations`` counts the claims a handler ended with ``ContinueJob``, which bounds them by
``jobs.max_continuations``. ``budget_offset`` holds ``attempts - deferrals`` as of the last continuation, so
the failure budget, ``attempts - deferrals - budget_offset``, counts only the claims made since then;
``attempts`` itself keeps counting every claim, because it fences the claim. Existing rows start at 0,
which is what they have always meant. Previous-release processes neither read nor write the columns, so
they count a job's claims since its last continuation as if it had never continued.

Revision ID: rev0007
Revises: rev0006
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from jasil.migrations._names import PREFIX

revision: str = "rev0007"
down_revision: str | None = "rev0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_JOBS = f"{PREFIX}processing_jobs"


def upgrade() -> None:
    op.add_column(
        _JOBS,
        sa.Column(
            "continuations",
            sa.Integer,
            nullable=False,
            server_default="0",
            comment="Claims the handler ended with ContinueJob",
        ),
    )
    op.add_column(
        _JOBS,
        sa.Column(
            "budget_offset",
            sa.Integer,
            nullable=False,
            server_default="0",
            comment="attempts - deferrals at the last ContinueJob; the failure budget counts claims after it",
        ),
    )


def downgrade() -> None:
    op.drop_column(_JOBS, "budget_offset")
    op.drop_column(_JOBS, "continuations")
