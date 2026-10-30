# API stability

JASIL follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html). This
page defines what that covers — and, just as importantly, what it does not.

!!! warning "0.x releases"
    The guarantees below take effect at `1.0.0`. While the version is `0.x` the
    API is still settling and a minor release may break it. The changelog will
    say so.

## Covered by SemVer

A breaking change to any of these requires a major version bump.

### The public namespace

Everything exported from `jasil/__init__.py`, plus the public names in these
modules:

| Module | Surface |
|---|---|
| `jasil.providers` | The capability protocols and their method signatures, the value types they exchange (`GeocodedPlace`, `MailMessage`, `UploadSession`, ...), `AuditSink` and `AuditSinkConflictError`, and `MAX_REALTIME_KEY_LENGTH`. |
| `jasil.events` | `Event`, `new_event`, `META_REQUEST_ID`, `INITIAL_SCHEMA_VERSION`, `MAX_EVENT_ID_LENGTH`, `MAX_EVENT_TYPE_LENGTH`, `MAX_SOURCE_LENGTH`. |
| `jasil.event_versioning` | `VersionedPayload`, `parse_payload`, `UnsupportedEventVersionError`. |
| `jasil.profile` | `DeploymentProfile`, `JobWorkerMode`, `parse_profile`, `parse_job_worker_mode`, `classify_state_uri` with its `StateBackendKind`, `DeploymentTopology` (`multi_process`, `multi_node`, `multi_process_causes`) and `resolve_topology`. |
| `jasil.settings` | The settings dataclasses and `configure` / `get_settings` / `is_configured`, `RuntimeSources`, and `mail_problems` with `MailProblem` and `MailProblemCode` (the set of codes may grow in a minor release; a code is never renamed or reused). |
| `jasil.orm` | `map_models`, `configure_sessionmaker`, `get_sessionmaker`, `get_engine`, `is_models_mapped`, `is_sessionmaker_configured`, `Base`, `jasil_table_names`, `legacy_table_names`. |
| `jasil.correlation` | `configure_provider`, `get_correlation_id`, `set_correlation_id`. |
| `jasil.container` | `Platform`, `build_platform` (including its `sources` parameter). |
| `jasil.capabilities` | `check_deployment_consistency`, `topology_of`, the individual `check_*` rules and the `StateSource` that `check_state_consistency` takes, and `build_capability_report` with the `CapabilityReport` (`topology`, `rows`, `render()`) and `CapabilityRow` (`name`, `backend`, `source`) it returns. The *rendered* report is not covered — see below. |
| `jasil.runtime` | `set_active_platform`, `get_active_platform`, `is_platform_active`, `get_state`, `reset`. |
| `jasil.lifecycle` | `shutdown`, and the order in which it releases what JASIL owns. |
| `jasil.testing` | `FixedClock`, `MemoryMail` (`outbox`, `built`, `clear`), `InProcessRealtime` (`clear`), `capture_hints`, `capture_audit` with `AuditCapture` (`entries`, `matching`, `assert_audited`, `assert_not_audited`), `install_test_platform`, `memory_mail`, `reset_all`. Covered because a host's test suite depends on it as much as its production code does. |
| `jasil.admin` | `get_jobs_summary`, `get_event_log_summary`, `get_workers_summary`, `get_job`, `get_event_jobs`, `get_worker_attempts`, `replay_dead_letter_job`, and the response schemas re-exported alongside them. |
| `jasil.publisher` | `publish`, `publish_committing`, `publish_many_committing`, `publish_on_commit`, `publish_many_on_commit`. |
| `jasil.subscribers` | `best_effort`. |
| `jasil.deps` | `get_platform`, `get_state`, `get_storage`, `get_events`, `get_lock`, `get_clock`, `get_mail`, `get_realtime`, `get_geocoding`, and the order they resolve a platform in. |
| `jasil.jobs.service` | `start_job_worker`, `run_job_worker`, `stop_job_worker`, `schedule_job_maintenance`, `build_runner`. |
| `jasil.retention` | `prune_expired_records`, `schedule_retention_maintenance`. |
| `jasil.audit` | Every name in its `__all__` except `reset`: the catalog (`AuditAction`, `AuditCatalog`, `Category`, `Severity`, `Outcome`, `get_catalog`, the built-in actions and their details models), the context (`AuditContext`, `ActorType`, `Source`, `set_context`, `get_context`, `audit_context`, `configure_provider`, `reset_context`), the write paths (`stage`, `stage_many`, `record_detached`, `AuditRecord`, `get_audit_health`), the query surface (`list_entries`, `get_entry`, `summary`, `list_catalog`, `verify_checkpoints` and the schemas they return), maintenance (`run_audit_maintenance`, `schedule_audit_maintenance`, `sync_catalog`) and the SQL helpers (`grants_sql`, `retention_floor_sql`). The underscore modules (`jasil.audit._postgres`) are private. A host's architecture test should treat `jasil.audit` as public surface. |
| `jasil.jobs` | `lease_lost`, `DeferJob`. |
| `jasil.jobs.registry` | `JobHandlerRegistry` (including its queue registration contract and the `on_dead_letter` callback contract: at-least-once within 10 deliveries and a 7-day window, delivered by the scheduled sweep, idempotent), `DeadLetterCallback`, `DEFAULT_QUEUE`, `MAX_QUEUE_NAME_LENGTH`, `MAX_SUBSCRIBER_ID_LENGTH`, `validate_queue_name`, and the process-wide `registry`. |
| `jasil.jobs.reconciliation` | `DurableSubscriberNet`, `undeclared_subscribers`, `assert_nets_complete`. |
| `jasil.migrations` | `upgrade`, `downgrade`, `adopt_existing_schema`, `SchemaCompatibilityError`, `head_revision`, `db_revision`, `verify_schema_current`, `jasil_include_object`, and `VERSION_TABLE`, the name of JASIL's Alembic version table. |

### Provider protocols

`MailProvider.send` returns the `Message-ID` as a `str`, and `MailError` /
`MailNotConfiguredError` are covered. Adding a field to `MailMessage` with a
default is additive.

`RealtimeProvider`'s four methods and its contract are covered: best-effort,
at-most-once, unordered delivery; transport failures never raise from
`notify`; an invalid hint raises `ValueError`; every started process
receives every hint. The realtime wire envelope (`{"v": 2, "key", "hint"}`)
is covered like the event envelope: a new version is added alongside, never in
place of, the one a running replica reads. 0.8 writes version 2 and still reads
version 1 (`{"v": 1, "user_id", "hint"}`, its `user_id` delivered as the key
`str(user_id)`). The default 4096-byte cap may grow
but will not shrink within a minor series.

Adding a method to a protocol is **breaking** — every host-supplied backend would
have to implement it. Adding an optional keyword argument with a default is not.

`StorageObjects`, `StorageStreams`, `StorageDelivery`, `StorageManagement`, and
`ResumableUploads` are composable slices. `StorageProvider` is their complete
aggregate and remains the type of `Platform.storage`; URI-selected built-in
backends implement every slice.

### Capability URI schemes

An existing scheme continuing to resolve to the same backend. Adding a new scheme
is additive.

### The event envelope

`Event`'s field names and types, and the wire form used by the Redis Streams bus
(the fields `serialize_event` / `deserialize_event` in `jasil.backends.events_redis`
write and read; the functions themselves are not public). A replica running the old version must
be able to read what a new one writes, or a rolling deploy breaks.

### Database schema

The columns, indexes and constraints of `jasil_event_log`, `jasil_event_outbox`,
`jasil_processing_jobs`, `jasil_job_workers`, `jasil_job_attempts`, `jasil_audit_log`,
`jasil_audit_actions`, `jasil_audit_checkpoints`, and `jasil_instance`; and, on PostgreSQL, the names and
signatures of `jasil_audit_ensure_partitions(integer)` and `jasil_audit_drop_partitions(timestamptz, timestamptz)`, the
append-only behaviour of `jasil_audit_log`, and that retention drops whole partitions. The partition names
(`jasil_audit_log_pYYYYMMDD`) are covered too. Schema
changes ship as Alembic revisions, and a revision must be forward-compatible with
the previous release: during a rolling deploy the old code runs against the new
schema. The one exception is the `jasil_` prefix rename (`rev0003`, 0.9.0), which
needs every older process stopped first; see
[Upgrading from 0.8](observability.md#upgrading-from-08-or-earlier-the-jasil_-prefix).

Removing a column is a major change. Adding a nullable one is not.

### Exception types

`StateBackendUnavailableError`, `StorageBackendUnavailableError`,
`StorageKeyConflictError`, `StorageSizeLimitError`, `StorageUploadSessionError`,
`UnsupportedEventVersionError`, `UnknownAuditActionError`, `AuditDetailsError`, and `AuditSinkConflictError` — what raises them and what they inherit from.

## Not covered

These may change in any release, including a patch.

### Private modules

`jasil._core` is internal, and the leading underscore is the notice. It holds
vendored helpers (the SSRF guard, the config slot) whose signatures serve JASIL's
needs, not yours. The reloading providers behind `RuntimeSources` are in
`jasil._reloading`, private for the same reason.

Anything else prefixed with `_`, at any level.

### Log messages

The text, level, and structured fields of any log record. Do not parse them or
alert on their exact wording.

### Error message text

The `str()` of an exception. The **type** is stable; the wording is not.

### Defaults

Tuning defaults — `lease_seconds`, `max_runtime_seconds`, `batch_size`, backoff
parameters, retention windows — may be adjusted in a minor release if a better
value is found. Pin any you depend on by setting them explicitly.

### Names exported for JASIL's own use

These are importable, some from a module's `__all__`, but they serve JASIL's
wiring and tests:

- `jasil.orm.get_active_base` and `jasil.orm.mapper_registry`.
- The `reset()` functions of `jasil.orm`, `jasil.settings`, `jasil.correlation` and
  `jasil.audit`. A host's tests call `jasil.testing.reset_all`, which is covered.
- The limit constants and helpers a public module imports from `jasil._core`:
  `MAX_AUDIT_*`, `MAX_JOB_BATCH_SIZE`, `MAX_PARTITION_DAYS_AHEAD` and
  `MAX_RETENTION_DAYS` in `jasil.settings`, `MAX_DASHBOARD_*` and `check_limit` in
  `jasil.admin`, `MAX_KEY_LENGTH` in `jasil.providers`, and `SMTP_PORTS`,
  `SMTP_SEND_DEADLINE_SECONDS` and `resolve_reverse_endpoint` in `jasil.container`.
  Their values are described in the configuration page; the names are not covered.
- `jasil.testing.DEFAULT_NOW`, `jasil.jobs.registry.normalize_queue_selector` and
  `JobHandler`, and the `profile`, `web_workers` and `job_worker_mode` fields of
  `DeploymentTopology`, which is read through `multi_process` and `multi_node`.
- `jasil.jobs.service.relay_outbox_scheduled`, `reap_expired_jobs_scheduled` and
  `deliver_dead_letters_scheduled`. Register them with `schedule_job_maintenance`,
  which is covered, rather than by name.

### Backend classes

The classes in the `jasil.backends` modules (`MemoryState`, `RedisState`,
`LocalStorage`, `S3Storage`, `SmtpMail` and the rest; none is exported from the
package itself), with their constructors and any method
beyond the protocol they implement. A backend is chosen by its URI scheme, which
is covered, and used through its provider protocol, which is covered too. The two
test doubles `jasil.testing` re-exports, `MemoryMail` and `InProcessRealtime`, are
covered there.

### Internal helpers

Functions in `jasil.jobs.crud` and `jasil.event_log.crud`. They are importable,
but they exist to serve the layers above and their signatures follow those needs.
Use `jasil.admin` for the dashboard aggregates and dead-letter replay.

### The capability report

The rendered format of `jasil.capabilities` output is for humans, not parsers.

## Deprecation

Where a change can be staged, it will be:

1. The new API ships alongside the old.
2. The old one emits a `DeprecationWarning` naming its replacement.
3. It is removed no earlier than the next major version.

Security fixes are the exception — those may break an API in a minor release, and
the changelog will say so explicitly.

## Python versions

`requires-python = ">=3.12,<4.0"`. Dropping support for a Python version is a
major change. Adding support for a new one is not.

Support tracks upstream: a Python version is dropped no earlier than its
end-of-life.

## Optional dependencies

Which extra provides which backend is stable. The **version ranges** within an
extra are not — they may be widened or narrowed in any release to track upstream
security fixes.

Moving a dependency from core to an extra is breaking. Moving one from an extra
to core is not.
