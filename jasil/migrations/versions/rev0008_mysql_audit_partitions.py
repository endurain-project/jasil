"""0.16: the audit log on MySQL and MariaDB is partitioned, append-only and guarded, as on PostgreSQL.

``jasil_audit_log`` becomes ``RANGE COLUMNS`` partitioned by ``occurred_at``, one partition per UTC day:
one for every day that already has rows, and from yesterday to ``AuditSettings.partition_days_ahead`` days
ahead. ``BEFORE UPDATE`` and ``BEFORE DELETE`` triggers reject changing a row, and two ``SQL SECURITY
DEFINER`` procedures add the next partitions and drop expired ones without any row-by-row delete.
``jasil_audit_drop_partitions`` refuses a cutoff younger than the retention floor, which is baked into the
procedure from ``AuditSettings.retention_floor_days`` as it is when this revision runs; change it afterwards
with ``jasil.audit.retention_floor_sql(..., dialect="mysql")``. Nothing changes on other databases.

Partitioning rebuilds the table, which blocks writes to the audit log while the rows are copied. Run it as
the account that should own the procedures. With binary logging on (the MySQL 8 default) creating a trigger
needs ``SUPER`` or ``log_bin_trust_function_creators=1``. DDL commits on these servers, so a run that fails
part-way is not rolled back; running it again replaces what the failed run created. The partitions are chosen
from the rows in the table, so this revision cannot be rendered as SQL for MySQL; it renders as nothing for
other databases.
Upgrade every process that runs audit maintenance with it. The maintenance of JASIL 0.15 and earlier treats this
log as a plain table: the trigger refuses its row ``DELETE`` (after it has committed a prune record each time), and it
never creates partitions, so inserts fail once the days created here run out.

Revision ID: rev0008
Revises: rev0007
Create Date: 2026-11-03
"""

from collections.abc import Sequence
from datetime import timedelta

import sqlalchemy as sa
from alembic import op

from jasil.audit import _mysql as audit_mysql
from jasil.settings import get_settings

revision: str = "rev0008"
down_revision: str | None = "rev0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _mysql() -> bool:
    """Whether the target is MySQL or MariaDB, which a ``mariadb+`` URL names differently."""
    if op.get_bind().dialect.name not in ("mysql", "mariadb"):
        return False
    if op.get_context().as_sql:
        raise RuntimeError(
            "rev0008 cannot be rendered as SQL for MySQL or MariaDB: it partitions jasil_audit_log by the days its "
            "rows fall on. Run it against the database with jasil.migrations.upgrade."
        )
    return True


def upgrade() -> None:
    if _mysql():  # pragma: no cover - exercised on MySQL and MariaDB (tests/test_audit_mysql.py), not on SQLite
        bind = op.get_bind()
        schema = bind.execute(sa.text("SELECT DATABASE()")).scalar_one()
        today = bind.execute(sa.text("SELECT UTC_DATE()")).scalar_one()
        days = set(bind.execute(sa.text("SELECT DISTINCT DATE(occurred_at) FROM jasil_audit_log")).scalars())
        audit = get_settings().audit
        # The whole horizon at once: a rebuild that runs past midnight must not leave a day without a partition
        # for the writes queued behind it.
        days |= {today + timedelta(days=offset) for offset in range(-1, audit.partition_days_ahead + 1)}
        for statement in audit_mysql.install_statements(
            schema, audit.retention_floor_days, audit.partition_days_ahead, sorted(days)
        ):
            op.execute(sa.text(statement))


def downgrade() -> None:
    if _mysql():  # pragma: no cover - exercised on MySQL and MariaDB (tests/test_audit_mysql.py), not on SQLite
        schema = op.get_bind().execute(sa.text("SELECT DATABASE()")).scalar_one()
        for statement in audit_mysql.uninstall_statements(schema):
            op.execute(statement)
        op.execute("ALTER TABLE jasil_audit_log REMOVE PARTITIONING")
