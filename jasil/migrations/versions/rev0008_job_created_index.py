"""Index ``processing_jobs.created_at`` for the jobs dashboard's time window.

``jasil.admin.get_jobs_summary`` counts the jobs created within its window, which without an index reads every
retained job. On PostgreSQL the index is built ``CONCURRENTLY``, so the previous release's workers keep claiming
and finishing jobs while it builds.

Revision ID: rev0008
Revises: rev0007
Create Date: 2026-10-29
"""

from collections.abc import Sequence

from alembic import op

from jasil.migrations import _online as online
from jasil.migrations._names import PREFIX

revision: str = "rev0008"
down_revision: str | None = "rev0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_JOBS = f"{PREFIX}processing_jobs"
_INDEX = "idx_jasil_processing_jobs_created"


def upgrade() -> None:
    online.create_index(_INDEX, _JOBS, ["created_at"])


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_JOBS)
