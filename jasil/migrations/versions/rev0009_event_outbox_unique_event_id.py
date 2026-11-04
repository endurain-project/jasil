"""Index ``event_outbox.event_id`` uniquely, so publishing an ``event_id`` that is already stored is a no-op.

A producer may publish with an ``event_id`` of its own. The unique index is what makes a reused one a true
no-op under concurrent publishers: the second insert is refused by the database instead of racing a check.
Building it fails if the table already holds two rows with the same ``event_id``, which only a host that wrote
rows with ``jasil.jobs.outbox.add_to_outbox`` directly can have; remove the duplicates and run the revision
again.

On PostgreSQL the index is built concurrently, so every producer keeps writing to the outbox during the build.
The revision changes nothing else, so there is no earlier work to resume: a run that failed during the build
leaves an invalid index, which the next run drops and builds again. Previous-release processes always mint a
fresh UUIDv4 ``event_id``, so they never collide with the index and keep working while the upgrade rolls out.

Revision ID: rev0009
Revises: rev0008
Create Date: 2026-11-03
"""

from collections.abc import Sequence

from alembic import op

from jasil.migrations import _online as online
from jasil.migrations._names import PREFIX

revision: str = "rev0009"
down_revision: str | None = "rev0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OUTBOX = f"{PREFIX}event_outbox"
_INDEX = f"uq_{PREFIX}event_outbox_event_id"


def upgrade() -> None:
    online.create_index(_INDEX, _OUTBOX, ["event_id"], unique=True)


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_OUTBOX)
