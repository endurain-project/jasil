"""MySQL and MariaDB DDL for the audit log: partition management, append-only triggers, the retention guard.

Used by migration ``rev0008`` and by :func:`jasil.audit.retention_floor_sql` with ``dialect="mysql"``. It
needs MySQL 8.0.1+ or MariaDB 10.6+; nothing here is applied on another dialect.

The log is ``RANGE COLUMNS`` partitioned by ``occurred_at``, one partition per UTC day named
``p<YYYYMMDD>`` that holds the rows before the end of that day. ``BEFORE UPDATE`` and ``BEFORE DELETE``
triggers reject every change to a row. ``TRUNCATE`` skips triggers on these servers, so only privileges keep
it out: :func:`jasil.audit.grants_sql` never grants ``DROP``.

The two procedures are ``SQL SECURITY DEFINER`` so an account holding no DDL rights can still add the next
day's partition and drop an expired one; ``ALTER TABLE ... DROP PARTITION`` removes rows without firing the
triggers, as dropping a partition does on PostgreSQL. Inside a routine the default database is the routine's
own, so their unqualified table names cannot be pointed at another database by the caller. Both take a named
lock for the database (``GET_LOCK``, waiting at most 10 seconds) and wait at most 10 seconds for a table's
metadata lock (``lock_wait_timeout``, restored before they return or fail). Both end with a ``SELECT`` of
their result, which ``CALL`` returns as a one-row result set.

DDL commits implicitly on these servers, so neither procedure runs inside the caller's transaction: call
them on a connection with no uncommitted work, which they would commit.
"""

import re
from collections.abc import Sequence
from datetime import date, timedelta

from jasil._core.limits import MAX_AUDIT_RETENTION_DAYS, MAX_PARTITION_DAYS_AHEAD

__all__ = [
    "drop_partitions_procedure",
    "ensure_partitions_procedure",
    "install_statements",
    "partition_clause",
    "quote_ident",
    "trigger_ddl",
    "uninstall_statements",
]

LOG = "jasil_audit_log"
INSTANCE = "jasil_instance"
ENSURE_PROCEDURE = "jasil_audit_ensure_partitions"
DROP_PROCEDURE = "jasil_audit_drop_partitions"
TRIGGERS = (f"{LOG}_no_update", f"{LOG}_no_delete")
#: Maintenance's statements: whether the routines are installed, create partitions, drop them (``prune_at`` may be
#: NULL, which stamps the prune row with the server's clock), and the UTC clock the retention floor is measured on.
INSTALLED_SQL = (
    "SELECT 1 FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() "
    "AND ROUTINE_NAME = 'jasil_audit_drop_partitions' AND ROUTINE_TYPE = 'PROCEDURE'"
)
ENSURE_CALL = f"CALL {ENSURE_PROCEDURE}(:days)"
DROP_CALL = f"CALL {DROP_PROCEDURE}(:cutoff, :prune_at)"
NOW_SQL = "SELECT UTC_TIMESTAMP(6)"
_PLACEHOLDER = re.compile(r"@([A-Z_]+)@")


def quote_ident(name: str) -> str:
    """Quote ``name`` as a MySQL identifier (or account name)."""
    return "`" + name.replace("`", "``") + "`"


def _render(template: str, **extra: str) -> str:
    """Fill the ``@NAME@`` placeholders of an SQL template in one pass, so a value is never substituted again."""
    values = {
        "LOG": LOG,
        "INSTANCE": INSTANCE,
        "ENSURE": ENSURE_PROCEDURE,
        "DROP": DROP_PROCEDURE,
        "MAX_DAYS_AHEAD": str(MAX_PARTITION_DAYS_AHEAD),
        "PARTITIONS": _PARTITIONS,
        **extra,
    }
    return _PLACEHOLDER.sub(lambda match: values[match.group(1)], template)


# The partitions this module creates and reads: ``p`` and the eight digits of the day they end with.
_PARTITIONS = """
            SELECT STR_TO_DATE(SUBSTRING(PARTITION_NAME, 2), '%Y%m%d') AS day, PARTITION_NAME AS name
            FROM information_schema.PARTITIONS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'jasil_audit_log'
                AND PARTITION_NAME REGEXP '^p[0-9]{8}$'"""

_ENSURE_TEMPLATE = """
CREATE PROCEDURE @QUALIFIED@(IN days_ahead INT)
SQL SECURITY DEFINER
MODIFIES SQL DATA
BEGIN
    DECLARE saved_wait BIGINT DEFAULT @@SESSION.lock_wait_timeout;
    DECLARE lock_name VARCHAR(64) DEFAULT CONCAT('@ENSURE@:', MD5(DATABASE()));
    DECLARE today DATE DEFAULT UTC_DATE();
    DECLARE last_day DATE;
    DECLARE next_day DATE;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET SESSION lock_wait_timeout = saved_wait;
        DO RELEASE_LOCK(lock_name);
        RESIGNAL;
    END;
    IF days_ahead IS NULL OR days_ahead < 0 OR days_ahead > @MAX_DAYS_AHEAD@ THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '@ENSURE@: days_ahead must be between 0 and @MAX_DAYS_AHEAD@';
    END IF;
    IF COALESCE(GET_LOCK(lock_name, 10), 0) <> 1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '@ENSURE@: another session holds the lock';
    END IF;
    SET SESSION lock_wait_timeout = 10;
    SELECT MAX(day) INTO last_day FROM (@PARTITIONS@) AS parts;
    IF last_day IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '@ENSURE@: @LOG@ is not partitioned by day';
    END IF;
    -- A partition can only be added after the last one, which already holds every earlier day.
    SET next_day = GREATEST(today - INTERVAL 1 DAY, last_day + INTERVAL 1 DAY);
    WHILE next_day <= today + INTERVAL days_ahead DAY DO
        SET @jasil_audit_ddl = CONCAT(
            'ALTER TABLE @LOG@ ADD PARTITION (PARTITION p', DATE_FORMAT(next_day, '%Y%m%d'),
            ' VALUES LESS THAN (''', DATE_FORMAT(next_day + INTERVAL 1 DAY, '%Y-%m-%d'), ' 00:00:00''))');
        PREPARE add_partition FROM @jasil_audit_ddl;
        EXECUTE add_partition;
        DEALLOCATE PREPARE add_partition;
        SET next_day = next_day + INTERVAL 1 DAY;
    END WHILE;
    SET @jasil_audit_ddl = NULL;
    SET SESSION lock_wait_timeout = saved_wait;
    DO RELEASE_LOCK(lock_name);
    SELECT MAX(day) AS furthest FROM (@PARTITIONS@) AS parts;
END"""

_DROP_TEMPLATE = """
CREATE PROCEDURE @QUALIFIED@(IN cutoff DATETIME(6), IN prune_at DATETIME(6))
SQL SECURITY DEFINER
MODIFIES SQL DATA
BEGIN
    DECLARE floor_days INT DEFAULT @FLOOR_DAYS@;
    DECLARE saved_wait BIGINT DEFAULT @@SESSION.lock_wait_timeout;
    DECLARE lock_name VARCHAR(64) DEFAULT CONCAT('@DROP@:', MD5(DATABASE()));
    DECLARE refusal VARCHAR(128);
    DECLARE dropped INT DEFAULT 0;
    DECLARE done INT DEFAULT 0;
    DECLARE first_day DATE;
    DECLARE last_day DATE;
    DECLARE part VARCHAR(64);
    DECLARE stamp CHAR(12);
    DECLARE noise CHAR(64);
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET SESSION lock_wait_timeout = saved_wait;
        DO RELEASE_LOCK(lock_name);
        RESIGNAL;
    END;
    -- Whole 24-hour days on the server's UTC clock, whatever the session's time zone.
    IF cutoff IS NULL OR cutoff > UTC_TIMESTAMP(6) - INTERVAL floor_days DAY THEN
        SET refusal = CONCAT('@DROP@: cutoff ', COALESCE(cutoff, 'NULL'),
                             ' is inside the retention floor of ', floor_days, ' days');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = refusal;
    END IF;
    IF COALESCE(GET_LOCK(lock_name, 10), 0) <> 1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '@DROP@: another session holds the lock';
    END IF;
    SET SESSION lock_wait_timeout = 10;
    SELECT COUNT(*), MIN(day), MAX(day) INTO dropped, first_day, last_day FROM (@PARTITIONS@) AS parts
    WHERE day + INTERVAL 1 DAY <= cutoff;
    IF dropped > 0 THEN
        -- A UUIDv7: 48 bits of Unix milliseconds, then version 7, the variant and hashed randomness.
        SET stamp = LPAD(HEX(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', UTC_TIMESTAMP(6)) DIV 1000), 12, '0');
        SET noise = SHA2(UUID(), 256);
        INSERT INTO @LOG@ (
            id, occurred_at, action_id, category, severity, outcome, actor_type, actor_id, source,
            target_type, details, details_version, instance_id
        ) VALUES (
            LOWER(CONCAT(SUBSTRING(stamp, 1, 8), '-', SUBSTRING(stamp, 9, 4), '-7', SUBSTRING(noise, 1, 3), '-',
                         SUBSTRING('89ab', 1 + CONV(SUBSTRING(noise, 4, 1), 16, 10) % 4, 1),
                         SUBSTRING(noise, 5, 3), '-', SUBSTRING(noise, 8, 12))),
            COALESCE(prune_at, UTC_TIMESTAMP(6)),
            'audit.retention.prune', 'system', 'medium', 'success', 'system', LEFT(USER(), 64), 'worker',
            '@LOG@',
            JSON_OBJECT('cutoff', DATE_FORMAT(cutoff, '%Y-%m-%dT%H:%i:%s.%f+00:00'),
                        -- MariaDB renders a bare INT variable as a JSON string.
                        'partitions_dropped', CAST(dropped AS SIGNED),
                        'first_day', DATE_FORMAT(first_day, '%Y-%m-%d'), 'last_day', DATE_FORMAT(last_day, '%Y-%m-%d')),
            1,
            (SELECT instance_id FROM @INSTANCE@ WHERE id = 1)
        );
        -- The record is committed before any partition goes, so a failure in between leaves a record of a
        -- prune the next run finishes, never a prune with no record.
        COMMIT;
        WHILE done < dropped DO
            SELECT name INTO part FROM (@PARTITIONS@) AS parts
            WHERE day + INTERVAL 1 DAY <= cutoff ORDER BY day LIMIT 1;
            SET @jasil_audit_ddl = CONCAT('ALTER TABLE @LOG@ DROP PARTITION ', part);
            PREPARE drop_partition FROM @jasil_audit_ddl;
            EXECUTE drop_partition;
            DEALLOCATE PREPARE drop_partition;
            SET done = done + 1;
        END WHILE;
        SET @jasil_audit_ddl = NULL;
    END IF;
    SET SESSION lock_wait_timeout = saved_wait;
    DO RELEASE_LOCK(lock_name);
    SELECT dropped;
END"""


def _qualified(schema: str, name: str) -> str:
    return f"{quote_ident(schema)}.{name}"


def trigger_ddl() -> list[str]:
    """Return the statements installing the append-only triggers on the log, in the connection's database."""
    return [
        f"CREATE TRIGGER {LOG}_no_{verb.lower()} BEFORE {verb} ON {LOG} FOR EACH ROW "
        f"SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '{LOG} is append-only: {verb} is not permitted'"
        for verb in ("UPDATE", "DELETE")
    ]


def ensure_partitions_procedure(schema: str) -> str:
    """Return ``jasil_audit_ensure_partitions(days_ahead)``, created in database ``schema``.

    It adds the daily partitions up to ``days_ahead`` days from today (UTC), after the last one there is, and
    returns the last day that has a partition.
    """
    return _render(_ENSURE_TEMPLATE, QUALIFIED=_qualified(schema, ENSURE_PROCEDURE))


def drop_partitions_procedure(schema: str, floor_days: int) -> str:
    """Return ``jasil_audit_drop_partitions(cutoff, prune_at)``, with ``floor_days`` baked in as the retention floor.

    It refuses a cutoff younger than ``UTC_TIMESTAMP()`` minus ``floor_days`` days, drops only partitions that
    end at or before the cutoff, records the drop as an ``audit.retention.prune`` row stamped ``prune_at`` (the
    server's UTC clock when NULL) and returns how many it dropped. It never deletes rows.

    Raises:
        ValueError: When ``floor_days`` is below 1 or above 36500.
    """
    if floor_days < 1:
        raise ValueError("the retention floor must be at least 1 day")
    if floor_days > MAX_AUDIT_RETENTION_DAYS:
        raise ValueError(f"the retention floor must be at most {MAX_AUDIT_RETENTION_DAYS} days")
    return _render(_DROP_TEMPLATE, QUALIFIED=_qualified(schema, DROP_PROCEDURE), FLOOR_DAYS=str(int(floor_days)))


def partition_clause(days: Sequence[date]) -> str:
    """Return the ``PARTITION BY`` clause giving each of ``days`` (dates, ascending, distinct) its own partition."""
    partitions = ", ".join(
        f"PARTITION p{day:%Y%m%d} VALUES LESS THAN ('{day + timedelta(days=1):%Y-%m-%d} 00:00:00')" for day in days
    )
    return f"PARTITION BY RANGE COLUMNS(occurred_at) ({partitions})"


def install_statements(schema: str, floor_days: int, days_ahead: int, days: Sequence[date]) -> list[str]:
    """Return, in order, everything migration ``rev0008`` runs to partition and protect the log.

    DDL commits on these servers, so a run that failed part-way has left some of it behind: the list first
    removes what an earlier run may have created, and creates the triggers before the table is rebuilt, so a
    missing privilege fails before the slow part.

    Args:
        schema: Database holding the audit tables.
        floor_days: The retention floor to bake into ``jasil_audit_drop_partitions``.
        days_ahead: How many days of partitions to create beyond today.
        days: The days to give a partition when the table is partitioned (ascending, distinct).
    """
    return [
        *uninstall_statements(schema),
        *trigger_ddl(),
        ensure_partitions_procedure(schema),
        drop_partitions_procedure(schema, floor_days),
        f"ALTER TABLE {LOG} {partition_clause(days)}",
        f"CALL {_qualified(schema, ENSURE_PROCEDURE)}({int(days_ahead)})",
    ]


def uninstall_statements(schema: str) -> list[str]:
    """Return the statements that remove the procedures and triggers, leaving the table and its rows."""
    return [
        f"DROP PROCEDURE IF EXISTS {_qualified(schema, DROP_PROCEDURE)}",
        f"DROP PROCEDURE IF EXISTS {_qualified(schema, ENSURE_PROCEDURE)}",
        *(f"DROP TRIGGER IF EXISTS {_qualified(schema, name)}" for name in TRIGGERS),
    ]
