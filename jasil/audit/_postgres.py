"""PostgreSQL DDL for the audit log: partition management, append-only triggers, the retention guard.

Used by migration ``rev0006`` and by :func:`jasil.audit.retention_floor_sql`. Everything here is
PostgreSQL 13 or newer (BEFORE row triggers on partitioned tables); nothing is applied on another
dialect, where the audit log is a plain table.

The two functions are ``SECURITY DEFINER`` so a runtime role holding no DDL rights can still create the
next day's partition and drop an expired one. Both pin ``search_path`` to ``pg_catalog, pg_temp`` (an
unlisted ``pg_temp`` would be searched first) and qualify every object, so a caller cannot redirect them
by shadowing a name with a temporary table. Both pin ``TimeZone`` and ``DateStyle`` too, so the partition
bounds they write as text and read back mean the same instant whatever the caller's session uses. Both wait at most ``lock_timeout`` (10 seconds) for a lock:
their advisory lock is one any role that can connect may take, and the table locks they need queue the
application's inserts behind them while they wait.
"""

import re

from jasil._core.limits import MAX_AUDIT_RETENTION_DAYS, MAX_PARTITION_DAYS_AHEAD

__all__ = [
    "drop_partitions_function",
    "ensure_partitions_function",
    "forbid_mutation_function",
    "install_statements",
    "partition_name_pattern",
    "quote_ident",
    "quote_literal",
    "trigger_ddl",
    "uninstall_statements",
]

LOG = "jasil_audit_log"
INSTANCE = "jasil_instance"
FORBID_FUNCTION = "jasil_audit_forbid_mutation"
ENSURE_FUNCTION = "jasil_audit_ensure_partitions"
DROP_FUNCTION = "jasil_audit_drop_partitions"
#: The argument types of the drop function: the cutoff, and the instant the prune row is stamped with.
DROP_ARGUMENTS = "timestamptz, timestamptz"
#: The day a partition covers is the last eight digits of its name.
partition_name_pattern = "_p([0-9]{8})$"
_PLACEHOLDER = re.compile(r"@([A-Z_]+)@")


def quote_ident(name: str) -> str:
    """Quote ``name`` as an SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """Quote ``value`` as an SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _render(template: str, schema: str, **extra: str) -> str:
    """Fill the ``@NAME@`` placeholders of an SQL template.

    Plain substitution rather than string formatting, because the templates are PL/pgSQL full of
    ``%`` and braces, and every substituted value is a quoted identifier, a quoted literal or an integer.
    Every placeholder is replaced in one pass, so a value that itself reads ``@NAME@`` is never substituted
    again.

    Raises:
        ValueError: When ``schema`` contains ``$``: quoting does not escape it, and inside a dollar-quoted
            body it could end the body.
    """
    if "$" in schema:
        raise ValueError(f"a schema name holding the audit functions must not contain '$', not {schema!r}")
    values = {
        "SCHEMA": quote_ident(schema),
        "SCHEMA_LIT": quote_literal(schema),
        "LOG": LOG,
        "INSTANCE": INSTANCE,
        "FORBID": FORBID_FUNCTION,
        "ENSURE": ENSURE_FUNCTION,
        "DROP": DROP_FUNCTION,
        "PART_PATTERN": partition_name_pattern,
        "MAX_DAYS_AHEAD": str(MAX_PARTITION_DAYS_AHEAD),
        **extra,
    }
    return _PLACEHOLDER.sub(lambda match: values[match.group(1)], template)


_FORBID_TEMPLATE = """
CREATE OR REPLACE FUNCTION @SCHEMA@.@FORBID@() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $body$
BEGIN
    RAISE EXCEPTION '@LOG@ is append-only: % is not permitted', TG_OP
        USING ERRCODE = 'restrict_violation';
END
$body$"""

_ENSURE_TEMPLATE = """
CREATE OR REPLACE FUNCTION @SCHEMA@.@ENSURE@(days_ahead integer) RETURNS date
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET lock_timeout = '10s'
SET TimeZone = 'UTC'
SET DateStyle = 'ISO, YMD'
SET standard_conforming_strings = on
AS $body$
DECLARE
    today date := (now() AT TIME ZONE 'UTC')::date;
    day date;
    part text;
    existing regclass;
BEGIN
    IF days_ahead < 0 OR days_ahead > @MAX_DAYS_AHEAD@ THEN
        RAISE EXCEPTION '@ENSURE@: days_ahead must be between 0 and @MAX_DAYS_AHEAD@, not %', days_ahead;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtext('@ENSURE@'));
    FOR day IN SELECT d::date FROM generate_series(today - 1, today + days_ahead, interval '1 day') AS d LOOP
        part := '@LOG@_p' || to_char(day, 'YYYYMMDD');
        existing := to_regclass(format('%I.%I', @SCHEMA_LIT@, part));
        IF existing IS NULL THEN
            EXECUTE format(
                'CREATE TABLE %I.%I PARTITION OF %I.@LOG@ FOR VALUES FROM (%L) TO (%L)',
                @SCHEMA_LIT@, part, @SCHEMA_LIT@,
                (day::timestamp AT TIME ZONE 'UTC'), ((day + 1)::timestamp AT TIME ZONE 'UTC'));
            EXECUTE format(
                'CREATE TRIGGER @LOG@_no_truncate BEFORE TRUNCATE ON %I.%I '
                'FOR EACH STATEMENT EXECUTE FUNCTION %I.@FORBID@()',
                @SCHEMA_LIT@, part, @SCHEMA_LIT@);
        ELSIF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_inherits i
            WHERE i.inhrelid = existing AND i.inhparent = to_regclass(format('%I.%I', @SCHEMA_LIT@, '@LOG@'))
        ) THEN
            -- Another table holds the name, so the day would have no partition and every row for it would fail.
            RAISE EXCEPTION '@ENSURE@: %.% exists but is not a partition of @LOG@', @SCHEMA_LIT@, part;
        END IF;
    END LOOP;
    RETURN (
        SELECT max(to_date(substring(c.relname FROM '@PART_PATTERN@'), 'YYYYMMDD'))
        FROM pg_catalog.pg_inherits i JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = to_regclass(format('%I.%I', @SCHEMA_LIT@, '@LOG@'))
    );
END
$body$"""

_DROP_TEMPLATE = """
CREATE OR REPLACE FUNCTION @SCHEMA@.@DROP@(cutoff timestamptz, prune_at timestamptz DEFAULT now()) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET lock_timeout = '10s'
SET TimeZone = 'UTC'
SET DateStyle = 'ISO, YMD'
SET standard_conforming_strings = on
AS $body$
DECLARE
    floor_days constant integer := @FLOOR_DAYS@;
    part record;
    day date;
    dropped integer := 0;
    first_day date;
    last_day date;
BEGIN
    -- Whole 24-hour days: a days interval on a timestamptz follows the session's calendar, so across a
    -- clock change it would move the floor by an hour against the application's own clamp.
    IF cutoff > now() - make_interval(hours => 24 * floor_days) THEN
        RAISE EXCEPTION '@DROP@: cutoff % is inside the retention floor of % days', cutoff, floor_days
            USING ERRCODE = 'restrict_violation';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtext('@DROP@'));
    FOR part IN
        SELECT c.relname
        FROM pg_catalog.pg_inherits i JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = to_regclass(format('%I.%I', @SCHEMA_LIT@, '@LOG@'))
        ORDER BY c.relname
    LOOP
        day := to_date(substring(part.relname FROM '@PART_PATTERN@'), 'YYYYMMDD');
        CONTINUE WHEN day IS NULL;
        IF ((day + 1)::timestamp AT TIME ZONE 'UTC') <= cutoff THEN
            EXECUTE format('DROP TABLE %I.%I', @SCHEMA_LIT@, part.relname);
            dropped := dropped + 1;
            first_day := coalesce(first_day, day);
            last_day := day;
        END IF;
    END LOOP;
    IF dropped > 0 THEN
        INSERT INTO @SCHEMA@.@LOG@ (
            id, occurred_at, action_id, category, severity, outcome, actor_type, actor_id, source,
            target_type, details, details_version, instance_id
        ) VALUES (
            encode(
                set_bit(set_bit(
                    overlay(
                        uuid_send(gen_random_uuid())
                        placing substring(int8send(floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint) FROM 3)
                        FROM 1 FOR 6),
                    52, 1), 53, 1),
                'hex')::uuid::text,
            prune_at,
            'audit.retention.prune', 'system', 'medium', 'success', 'system', left(session_user::text, 64), 'worker',
            '@LOG@',
            jsonb_build_object('cutoff', cutoff, 'partitions_dropped', dropped,
                               'first_day', first_day, 'last_day', last_day),
            1,
            (SELECT instance_id FROM @SCHEMA@.@INSTANCE@ WHERE id = 1)
        );
    END IF;
    RETURN dropped;
END
$body$"""


def forbid_mutation_function(schema: str) -> str:
    """Return the trigger function that rejects ``UPDATE``, ``DELETE`` and ``TRUNCATE``."""
    return _render(_FORBID_TEMPLATE, schema)


def trigger_ddl(schema: str) -> list[str]:
    """Return the statements installing the append-only triggers on the parent table."""
    table = f"{quote_ident(schema)}.{LOG}"
    function = f"{quote_ident(schema)}.{FORBID_FUNCTION}()"
    return [
        f"CREATE TRIGGER {LOG}_no_update_delete BEFORE UPDATE OR DELETE ON {table} "
        f"FOR EACH ROW EXECUTE FUNCTION {function}",
        f"CREATE TRIGGER {LOG}_no_truncate BEFORE TRUNCATE ON {table} FOR EACH STATEMENT EXECUTE FUNCTION {function}",
    ]


def ensure_partitions_function(schema: str) -> str:
    """Return ``jasil_audit_ensure_partitions(days_ahead)``.

    It creates the daily partitions from yesterday to ``days_ahead`` days from today (UTC), gives each a
    ``BEFORE TRUNCATE`` trigger (statement triggers are not inherited by partitions, and ``TRUNCATE`` on
    a partition would bypass the parent's), and returns the last day that has a partition.
    """
    return _render(_ENSURE_TEMPLATE, schema)


def drop_partitions_function(schema: str, floor_days: int) -> str:
    """Return ``jasil_audit_drop_partitions(cutoff, prune_at)``, with ``floor_days`` baked in as the retention floor.

    It refuses a cutoff younger than ``now()`` minus ``floor_days`` times 24 hours, whatever the session's
    ``TimeZone``, drops only partitions that end at or before the cutoff, and records the drop as an
    ``audit.retention.prune`` row stamped ``prune_at`` (the database's ``now()`` when omitted; maintenance passes
    the platform clock's instant, as for every other row). It never deletes rows.

    Raises:
        ValueError: When ``floor_days`` is below 1 or above 36500.
    """
    if floor_days < 1:
        raise ValueError("the retention floor must be at least 1 day")
    if floor_days > MAX_AUDIT_RETENTION_DAYS:
        raise ValueError(f"the retention floor must be at most {MAX_AUDIT_RETENTION_DAYS} days")
    return _render(_DROP_TEMPLATE, schema, FLOOR_DAYS=str(int(floor_days)))


def install_statements(schema: str, floor_days: int, days_ahead: int) -> list[str]:
    """Return, in order, everything migration ``rev0006`` runs on PostgreSQL after creating the tables.

    Args:
        schema: Schema holding the audit tables.
        floor_days: The retention floor to bake into ``jasil_audit_drop_partitions``.
        days_ahead: How many days of partitions to create beyond today.
    """
    s = quote_ident(schema)
    statements = [forbid_mutation_function(schema), *trigger_ddl(schema)]
    statements += [ensure_partitions_function(schema), drop_partitions_function(schema, floor_days)]
    # Functions are executable by PUBLIC unless revoked, and these run with the owner's rights.
    statements += [
        f"REVOKE ALL ON FUNCTION {s}.{ENSURE_FUNCTION}(integer) FROM PUBLIC",
        f"REVOKE ALL ON FUNCTION {s}.{DROP_FUNCTION}({DROP_ARGUMENTS}) FROM PUBLIC",
        f"SELECT {s}.{ENSURE_FUNCTION}({int(days_ahead)})",
    ]
    return statements


def uninstall_statements(schema: str) -> list[str]:
    """Return the statements that remove the functions, once the tables (and their triggers) are gone."""
    s = quote_ident(schema)
    return [
        f"DROP FUNCTION IF EXISTS {s}.{DROP_FUNCTION}({DROP_ARGUMENTS})",
        f"DROP FUNCTION IF EXISTS {s}.{ENSURE_FUNCTION}(integer)",
        f"DROP FUNCTION IF EXISTS {s}.{FORBID_FUNCTION}()",
    ]
