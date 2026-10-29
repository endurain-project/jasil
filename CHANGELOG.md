# Changelog

All notable changes to JASIL are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

What counts as a breaking change — and what does not — is defined in [API stability](docs/api-stability.md). In short: the `jasil` namespace, the capability provider protocols, the event envelope, the capability URI schemes, and the database schema of JASIL's own tables are covered by SemVer; anything under `jasil._core`, log message text, and the wording of error messages are not.

## [0.14.0] - 2026-10-30

This release adds the audit trail and hardens the substrate around it: durable jobs, publishing, migrations, storage, the event buses and the outbound calls. The audit trail writes nothing until the host calls its API, and every addition is optional. The breaking changes are listed under **Upgrading**.

### Upgrading

1. **State.** Stop passing `ttl_seconds=0` (or a negative value) to `StateProvider.set`, `set_if_absent` or `incr`: pass `None` for no expiry. `delete_prefix("")` is refused too.
2. **Redis event bus.** If anything recovers stuck entries from the stream with `XPENDING` / `XAUTOCLAIM`, drop it: a failed entry is now acked, and durable jobs are the retrying path.
3. **Job settings.** Check your `JobSettings`: every field is validated when the object is built, so a value the pipeline cannot honour raises where you configure JASIL instead of when a worker starts, or not at all.
4. **Dead-letter callbacks.** Register `on_dead_letter` callbacks in the process that runs `schedule_job_maintenance`: the scheduled sweep is now the only thing that calls them.
5. **Migrations.** If you run JASIL's revisions from your own Alembic history, set `transaction_per_migration=True` in your `env.py`: on PostgreSQL `rev0002`, `rev0004` and `rev0006` commit before building their indexes concurrently. If your declarative base's `MetaData` names a `schema`, drop it: `upgrade`, `downgrade` and `adopt_existing_schema` refuse such a base. `adopt_existing_schema` also refuses schemas it used to accept (see **Changed**).
6. **Storage.** Rename any storage area that starts with `.jasil-` in any letter case, such as `.JASIL-exports`. Areas, keys and prefixes with a control character or longer than 1024 UTF-8 bytes are refused, and an S3 `url` / `serve` with `expires_in` outside 1 second to 7 days raises `ValueError`.
7. **Outbound hosts.** If a reverse-geocoding host resolves to the shared address space `100.64.0.0/10` or an IPv6 site-local address, add it to the SSRF allowlist. Use `nominatim://` or `photon://`, not the `+http` forms, for a geocoding host on the internet; an `https://` S3 `endpoint_url` unless the store is on your private network; and `smtps://` or a private address rather than cleartext mail to a 6to4 (`2002::/16`) address.
8. **Admin dashboards.** Publish a platform before calling `jasil.admin.get_jobs_summary` or `get_event_log_summary`, which now measure their window from its clock. All three summaries take `hours` from 1 to 8784, and the two list limits from 0 to 500.

### Added

- **Audit trail (`jasil.audit`).** A complete, append-only record of who did what, written synchronously on the caller's own session so the audit row and the change commit or roll back together, identically with or without durable jobs. `stage(db, action, ...)` / `stage_many` add the row to the caller's session and never commit; they **fail closed** (validation and storage errors propagate). `record_detached(action, outcome=..., ...)` writes on a session of its own for denials, failed logins and CLI commands and **fails open** (logged without the row's values, counted, never raised); `get_audit_health()` reports `write_failures`, `last_failure_at`, `furthest_partition_date` and `last_checkpoint_at`. Every timestamp comes from the platform clock. See [Audit trail](docs/audit.md).
  - **Catalog.** `AuditAction`, `AuditCatalog`, `get_catalog()`, `Category`, `Severity`, `Outcome`. Ids are `<module>.<resource>.<verb>`, stable forever; `details` are validated against a versioned `details_model` and capped at `audit.max_details_bytes`. The substrate registers `audit.retention.prune`, `audit.retention.change`, `audit.integrity.violation` and `audit.catalog.sync`. `sync_catalog()` publishes the catalog to `jasil_audit_actions` for readers that cannot import the host's code; the application that registers every action calls it at startup.
  - **Context.** `AuditContext`, `set_context`, `get_context`, `audit_context(...)`, `configure_provider`: a `ContextVar` modelled on `jasil.correlation`. `request_id` is always read from `jasil.correlation`. Ids and `credential_type` are checked against their columns; the client address and user agent are clipped. A provider that raises is logged by exception type and the row is written without its context.
  - **Query surface.** `list_entries` (keyset-paginated, always time-bounded to at most 31 days, fourteen filters), `get_entry`, `summary`, `list_catalog` and `verify_checkpoints`, with Pydantic schemas. `verify_checkpoints` covers at most 366 days per call and takes one public key or a list of them, so a trail signed across a key rotation, pruned windows included, verifies in one call. It reports a gap between checkpoints even when every checkpoint inside the range was deleted, and rows dated before the first checkpoint.
  - **Maintenance.** `run_audit_maintenance()` and `schedule_audit_maintenance(scheduler, run_at_startup=True)`: retention-change record, partitions, checkpoints, retention, each step isolated, under its own lock. Scheduled by the host, like `jasil.retention`. It never syncs the catalog, so it can run in a process that registers only JASIL's own actions.
  - **Tamper evidence.** Signed (Ed25519), hash-chained checkpoints over fixed windows with a grace period, stored in `jasil_audit_checkpoints` and optionally copied to a write-once external sink: `AuditSink` (`write_once`, `read`) / `AuditSinkConflictError` in `jasil.providers`, `Platform.audit_sink` (default `None`), and the `file://` and `s3://` backends selected by `audit.external_sink_uri`. The `s3://` sink writes each checkpoint with a conditional `PutObject` (`If-None-Match: *`), so a bucket policy can deny every overwrite; a store without conditional writes fails every seal. The `distributed` profile refuses a `file://` sink, whose copies would scatter across nodes' disks (`jasil.capabilities.check_audit_sink_consistency`). `audit.legal_hold` binds JASIL's maintenance; the database function enforces only the retention floor (see [Retention](docs/audit.md#retention) for a hold the database enforces). Each checkpoint signs the `retention_days` and `legal_hold` in force when it was sealed: verification excuses an emptied window only where a checkpoint's signed retention covers it, and retention never prunes past what the newest sealed checkpoint allows, so never a row that was not sealed. A seal retried after a crash between the sink and the database write stores the copy the sink already holds. New optional extra `jasil[audit]` (`cryptography`); unsigned, hash-chained checkpoints need nothing.
  - **Settings.** `AuditSettings` (`JasilSettings.audit`): `enabled`, `retention_days` (90), `retention_floor_days` (30), `legal_hold`, `partition_days_ahead` (7), `checkpoint_window_minutes` (15), `checkpoint_grace_minutes` (5), `signing_key`, `external_sink_uri`, `max_details_bytes` (4096), `app_name`, `app_version`, all validated when the object is built.
  - **SQL for the host.** `grants_sql(runtime_role, reader_role, *, maintenance_role=None, schema=None)` and `retention_floor_sql(floor_days, schema=...)` (the schema is required).
  - **Testing.** `jasil.testing.capture_audit()` / `AuditCapture` (with `assert_audited`), working with `audit.enabled = False` and in both `JOBS_ENABLED` modes; `reset_all()` also resets the audit state.
- **Migration `rev0006`.** Adds `jasil_audit_log`, `jasil_audit_actions`, `jasil_audit_checkpoints` and `jasil_instance`. On PostgreSQL (13 or newer) the log is range-partitioned by day, `UPDATE`, `DELETE` and `TRUNCATE` are rejected by triggers, and two `SECURITY DEFINER` functions (`jasil_audit_ensure_partitions`, `jasil_audit_drop_partitions`) manage the partitions whatever the caller's `TimeZone` or `DateStyle`. The latter refuses a cutoff inside a retention floor baked in from `audit.retention_floor_days` when the migration runs, and drops whole partitions only; both wait at most 10 seconds for a lock. Other dialects get a plain table. The revision also adds the nullable `jasil_processing_jobs.event_timestamp`, so a durable subscriber's (and an `on_dead_letter` callback's) envelope carries the event's publish `timestamp`, as documented, rather than the time its job was enqueued, and indexes `jasil_processing_jobs.created_at` for the jobs dashboard, `CONCURRENTLY` on PostgreSQL. Run it as the role that should own the audit table and functions; to render it as SQL, pass `-x schema=<name>`. Processes of the previous release keep running while it applies. Downgrading drops the audit trail.
- `new_event` takes `now`, the instant to stamp; the publish functions pass the platform clock's.

### Changed

- **BREAKING: `StateProvider` backends refuse the arguments they used to read differently.** A `ttl_seconds` of zero or less raises `ValueError` (memory took it as already expired, Redis as no expiry, so a computed TTL of 0 made a permanent key), and so does `delete_prefix("")` (on Redis it deleted every key in the database, the event stream included). On Redis, a command refused for the key's value (`WRONGTYPE`, `incr` of a non-integer) raises `ValueError`, as on memory, instead of `StateBackendUnavailableError`; a server condition (a read-only replica, out of memory) is still `StateBackendUnavailableError`.
- **BREAKING: the event buses run every handler, and the Redis bus acks a failed entry.** A handler that raises no longer keeps the event from the handlers after it. In process, the failure then reaches the publisher as before (several together as an `ExceptionGroup`). On Redis it is logged and recorded, and the entry is acked: left pending it was never read again and only grew the pending-entries list. The Redis bus is documented as what it is, best-effort and at most once per event.
- **BREAKING: `JobSettings` validates every field when it is built.** `batch_size` is 1 to 1000, `poll_interval_seconds` and `heartbeat_interval_seconds` are greater than zero, `max_attempts` is at least 1, `backoff_base_seconds`, `defer_max_seconds` and `max_deferrals` are at least 0, and `backoff_max_seconds` is not below `backoff_base_seconds`; the lease and runtime rules that `build_runner` checked are checked here too. A zero poll interval made an idle worker poll the database in a hot loop.
- **BREAKING: `on_dead_letter` callbacks run only in the scheduled sweep**, every 60 seconds, never in the worker that dead-lettered the job: a slow or hung callback held the worker and the leases of the jobs behind it in its batch. The delivery limits (10 deliveries, jobs dead-lettered in the last 7 days) are documented.
- **BREAKING: storage addresses are stricter.** Every area whose name starts with `.jasil-`, in any letter case, is reserved; a case-insensitive filesystem reached the private directories under another spelling. A control character in an area, key or prefix, or one longer than 1024 UTF-8 bytes, is refused on both backends (on S3 the bucket prefix and area count toward the limit): S3 refused them as an outage, and local disk stored keys S3 could not.
- **BREAKING: migrations refuse what they cannot honour.** `upgrade`, `downgrade` and `adopt_existing_schema` refuse a base whose `MetaData` names a `schema`, since the migrations create JASIL's tables in the connection's default schema. `adopt_existing_schema` also refuses a `NOT NULL` column missing its database default, a missing named `CHECK` constraint, an unexpected unique index, and, on PostgreSQL, an audit log without its partitions, append-only triggers or functions: each breaks a guarantee `upgrade()` would not restore.
- **BREAKING: outbound calls reach fewer addresses.** The reverse-geocoding address check refuses every address that is not globally reachable, including the shared address space `100.64.0.0/10` (where one cloud serves instance metadata) and IPv6 site-local. `nominatim+http://` and `photon+http://` are refused for a host that resolves to a public address, and an S3 `endpoint_url` must be `https://`, or `http://` to a host that resolves only to private addresses. Cleartext mail no longer counts a 6to4 (`2002::/16`) address as private.
- **BREAKING: the admin dashboards are bounded and follow the platform clock.** `jasil.admin.get_jobs_summary`, `get_event_log_summary` and `get_workers_summary` take `hours` from 1 to 8784 (366 days), and the two list limits from 0 to 500: an unbounded window read every retained row, and a huge one overflowed. The jobs and event-log summaries measure their window from the platform clock and raise `RuntimeError` when no platform is published.
- **Migrations run safely beside a running release.** `jasil.migrations.upgrade` commits each revision on its own and holds a database lock for the run (PostgreSQL and MySQL), so replicas starting together no longer collide. On PostgreSQL `rev0002`, `rev0004` and `rev0006` build their `jasil_processing_jobs` indexes `CONCURRENTLY`, so the previous release's workers keep claiming jobs, and a run that fails resumes from the last committed revision, including one interrupted in an index build. `verify_schema_current` accepts, with a warning, a revision only a newer JASIL records, so a previous-release replica restarting during a rolling deploy starts.
- **Publishing.** `publish_committing`, `publish_on_commit` and their batch forms raise `ValueError` for an `event_type` or `source` longer than its column before committing or staging anything, on the best-effort route as on the durable one, which used to commit, then log the failure and drop the event. Events are stamped by the platform clock. A failed publish is logged by exception type, and the driver error it wraps, without the message or traceback: a SQLAlchemy error's text carries the statement parameters, which are the event's payload and metadata.
- **Durable jobs.** A job's stored failure reason (`last_error`, the attempt history and the `error` an `on_dead_letter` callback receives) is `"<ExceptionType>: <message>"`, so an exception with an empty message still names the failure. The outbox relay logs a warning when an event has no durable subscriber registered in its process, which marks it relayed with no jobs.
- **Event log.** `jasil_event_log` timestamps (`created_at`, `processed_at`, `completed_at`) come from the platform clock in UTC instead of the database's `now()`, which on MySQL is the session time zone's local time read back as UTC. Rows written by a previous release keep the database's value.
- **Storage.** Local storage flushes each write to disk before the rename that publishes it and syncs the directory after, so a write that returned survives a crash; writes are slower for it. The S3 client waits at most 10 seconds to connect and 60 seconds for each read.
- `jasil.orm.map_models` refuses, after `reset()`, a different base from the one JASIL's models were first imported against, instead of mapping no tables into it.
- [API stability](docs/api-stability.md) classifies every exported name. It now lists `jasil.deps.get_geocoding`, `jasil.orm.jasil_table_names` / `legacy_table_names` / `is_models_mapped` / `is_sessionmaker_configured`, `jasil.settings.is_configured`, `jasil.migrations.jasil_include_object` / `VERSION_TABLE`, `jasil.capabilities.build_capability_report` with its `CapabilityReport` / `CapabilityRow` and `StateSource`, and `jasil.profile.StateBackendKind`, and states that the other exported helpers and the backend classes are not covered.

### Removed

- `JobRunner.reap_once`, which no scheduled job called; the reaper is `schedule_job_maintenance`'s job. Also the internal `jasil.jobs.crud.get_job` and `reclaim_expired_leases_detailed`, and `jasil.backends.geocoding_http.build_reverse_endpoint`.

### Fixed

- **Durable jobs.** A durable handler that raises `SystemExit`, `KeyboardInterrupt` or a cancellation has its failure recorded and the rest of its batch returned to `pending` before the exception ends the worker, which logs it at `ERROR`; the job stayed claimed until its lease expired. The scheduled reaper reclaims expired leases in batches of 100 until none are left, up to 10 000 a run, instead of 100 a minute. A dead-letter callback delivery claimed before a replay can no longer settle the delivery of the job's next dead letter, and the reaper's dead-letter path clears `locked_at` like every other terminal path. `JobHandlerRegistry` is safe to use from several threads, and registering a subscriber again under another event type moves it instead of leaving it listed under both. `supports_skip_locked` and the durable-job enqueue recognise the `mariadb` dialect name, so a `mariadb+...` URL gets `SKIP LOCKED` and no longer fails every enqueue.
- **Publishing.** `publish_on_commit` dispatches the payload as it was when published: a dict changed before the commit no longer changes the event. `publish_many_committing` no longer raises after the domain commit when `metadata_for` fails on the best-effort path; that event is logged and skipped.
- **Redis event bus.** The consumer recreates its consumer group when Redis has lost it (a restart without persistence, or a flush) instead of failing every poll. A failed ack is logged and the rest of the batch still handled. A restart after a `stop()` that timed out no longer revives the old consumer thread. An envelope is serialized before it is recorded `published`, so one that cannot be sent leaves no row behind.
- **Redis clients.** `Platform.close()`, and a failed `build_platform()`, give back only the clients that platform took; a client is closed when no platform in the process uses it. Another platform's clients were closed under it.
- **Realtime.** A hint nested past the JSON parser's depth is ignored instead of tearing down every subscriber's connection, and one sent that deep raises `ValueError` as documented.
- **State and locks.** `MemoryState` sweeps expired keys on a write at most once a minute; a key that expired without being read again stayed in memory for the life of the process. `PgAdvisoryLock` holds its lock on an autocommit connection, so `idle_in_transaction_session_timeout` cannot end the session and release the lock under the guarded work, and logs at `ERROR` and discards the connection when the lock was lost before release.
- **Storage.** `LocalStorage.save` writes through a temporary file and an atomic rename, as `save_stream` does, so a failed overwrite leaves the previous object instead of a torn one. S3 presigned URLs are signed with Signature Version 4, which SSE-KMS objects and SigV4-only stores require. S3 `upload_part` works against a plain-`http://` endpoint (it raised `AttributeError` there), botocore can resend a part after a transient failure, and a source shorter or longer than `size` raises `ValueError` before any request is sent; the part is staged in memory up to 8 MiB and in a temporary file beyond that, so allow temporary disk for the largest part in flight. S3 `cleanup_uploads` logs an invalid session manifest and goes on with the rest instead of stopping every pass at it.
- **Reverse geocoding** has an overall 30-second deadline on top of the per-read timeout: a watchdog shuts the request's sockets down when it passes, headers included, so an upstream dripping its reply cannot hold the caller for hours.
- **Secrets.** A malformed capability URI is no longer echoed, password included, by the "Unsupported scheme" errors, the startup report or a Redis client error. `state_uri`, `events_uri`, `realtime_uri`, `storage_uri` and `audit.external_sink_uri` are left out of the settings' `repr`. A `MailError` raised because the mail settings source failed no longer chains the source's exception, whose text may carry a secret from the host's store.
- **Settings.** `JasilSettings` takes `profile` and `job_worker_mode` as their names too (`"distributed"`), and refuses a misspelt one; a plain string passed every deployment check unmatched.

## [0.13.0] - 2026-10-27

### Added

- `LocalStorage(..., plain_areas=...)` and `StorageSettings.local_plain_areas` opt selected local-storage areas into readable `{data_dir}/{area}/{key}` paths. Other areas retain the private hashed layout, and S3 ignores the setting. Plain areas read and list older hashed-layout objects and migrate them to the plain path on overwrite; delete removes both copies. Plain writes are atomic and do not create identity sidecars.
- `StorageKeyConflictError` reports a file/directory key conflict in plain local areas. Unlike S3, plain local storage cannot store both `a` and `a/b` in the same area.

No breaking changes: plain areas are opt-in and the default local layout is unchanged.

## [0.12.0] - 2026-10-26

### Added

- **`DeferJob`.** A durable handler raises `jasil.jobs.DeferJob(retry_at, reason=None)` to run again later without failing. The job returns to `pending` with `available_at = retry_at` and its lease cleared, `last_error` set to the reason, and the claim is **not** counted against `max_attempts`; it never dead-letters and never runs `on_dead_letter`. `retry_at` is clamped to between now and the new `jobs.defer_max_seconds` (default `86400`) with up to 10% jitter, and after `jobs.max_deferrals` (default `100`) deferrals the next `DeferJob` is an ordinary failure. Applied only while the worker still owns the claim. Recorded as a `deferred` attempt outcome and logged at `INFO`.
- `Event.deferral_count`, and `deferrals` on `JobDetail` and the admin `DeadLetterJob`.
- `jasil.jobs.crud.mark_job_deferred`.

### Changed

- Migration `rev0005` adds `jasil_processing_jobs.deferrals` (default 0). `attempts` still counts every claim; the budget against `max_attempts` is `attempts - deferrals`, used by failures, the reaper and `Event.retry_count`. Without deferrals nothing changes. Run the migration before deploying; processes of the previous release neither read nor write the column, and would count a deferred claim as an attempt.

No breaking changes: `DeferJob` is opt-in, and a job that is never deferred behaves exactly as before.

## [0.11.0] - 2026-10-24

### Added

- **Dead-letter hook.** `JobHandlerRegistry.register(..., on_dead_letter=callback)` registers `callback(event, error)`, called once a subscriber's job has become `dead_letter`: the handler raised on its last attempt, the lease expired, or the reaper recovered a crashed worker. It runs in the worker or reaper process after the transition is committed; an exception is logged and swallowed. Delivery is **at least once** (callbacks must be idempotent), tracked by the new `jasil_processing_jobs.dead_letter_notified_at` column (plus `dead_letter_notify_after`, `dead_letter_attempts` and `dead_letter_callback_error` for the claim lease, retry backoff and diagnosis) and a sweep run by its own scheduled job (`jasil_dead_letter_delivery`, so slow callbacks never hold up the lease reaper). Each delivery is claimed with its own lease when its callback starts, its outcome is written only while that claim is still live, a callback that keeps failing is given up on after 10 deliveries, and the sweep ignores jobs dead-lettered more than 7 days ago. `replay_dead_letter_job` clears it. Also `JobHandlerRegistry.dead_letter_callback` / `dead_letter_subscriber_ids`, and `DeadLetterCallback`.
- **Authoritative attempt counts.** `Event.max_attempts`, and the `Event.attempt` and `Event.is_last_attempt` properties, are filled for durable handlers from the job row, the same values that decide dead-lettering. `attempt` is 1-based and equals `retry_count` inside a durable handler. Handlers no longer need to compare `retry_count` with a setting, which is wrong for jobs enqueued under a different `max_attempts`.
- `jasil.jobs.crud.reclaim_expired_leases_detailed`, which also returns the ids it dead-lettered.

### Changed

- Migration `rev0004` adds an `idx_jasil_processing_jobs_dead_letter` index and the `dead_letter_notified_at`, `dead_letter_notify_after`, `dead_letter_attempts` and `dead_letter_callback_error` columns and stamps existing dead letters so they are not called back. Run it before deploying; processes of the previous release neither read nor write the column.

No breaking changes: `on_dead_letter` is optional, and a handler registered without it behaves as before.

## [0.10.1] - 2026-10-22

### Fixed

- A subscriber without a `__name__` (a `functools.partial` or a callable object) no longer raises `AttributeError` from the event-recording or the subscriber-failure logging path; its `repr` is used instead.

### Changed

- `@contextmanager` functions are annotated `Generator[...]` rather than `Iterator[...]`, as typeshed now prefers. No runtime change.
- Development: the Astral `ty` type checker runs beside mypy as an advisory CI job (`jasil (ty, advisory)`); mypy remains the gate.

No breaking changes.

## [0.10.0] - 2026-10-22

### Added

- **Runtime-configured mail and geocoding.** `RuntimeSources(mail=..., geocoding=...)` holds callables that return the current `MailSettings` / `GeocodingSettings`; pass it as `build_platform(settings, sources=...)`. `Platform.mail` / `Platform.geocoding` then follow the source without a restart: the backend is rebuilt only when the returned settings change, the outcome (backend or rejection) is cached so a bad configuration is logged once per change, and a source that raises falls back to the backend the latest settings built, if it works (mail raises a retryable `MailError` and geocoding returns `None` when there is none). Invalid mail settings raise `MailNotConfiguredError` and never reuse earlier credentials. A host that does not resolve (`secure=False` mail, geocoding) is retried after 30 seconds instead of being remembered as rejected; mail raises a plain retryable `MailError` meanwhile. The multi-process `memory://` rule is applied on every rebuild. `build_platform` never calls a source; the first call is the first use. State, storage, events, lock and realtime stay static. See [Runtime sources](docs/configuration.md#runtime-sources).
- `mail_problems(mail_settings)`, `MailProblem` and `MailProblemCode` in `jasil.settings`: validate mail settings without building a backend, for a host that checks operator input on save. The builder now raises the first reported problem, so there is one copy of the rules.
- The startup capability report gains a `geocoding` row, and reports a sourced capability as `runtime` / `runtime source`.
- `jasil.testing.install_test_platform(..., sources=)`; `memory_mail` follows a sourced `memory://` backend.

No breaking changes: a host that passes no sources sees no difference, apart from the extra `geocoding` line in the startup log (anything parsing that output should expect it).

## [0.9.0] - 2026-10-22

A release about not assuming the application JASIL was extracted from. Several defaults, names and value shapes were Endurain's; they are now neutral. Most of it is breaking; read **Upgrading** first.

### Upgrading

1. **Stop every process running 0.8 or earlier**, then run `migrations.upgrade(engine)` (or `adopt_existing_schema` then `upgrade`). Revision `rev0003` renames JASIL's tables, and the old code looks the old names up, so a rolling deployment across it is not possible.
2. Update anything of yours that names the tables (reports, dashboards, raw SQL): see the new names below.
3. Replace the `GeocodingSettings` fields and read the new `GeocodedPlace` fields (below).
4. If you have objects in the flat `{data_dir}/{area}/{key}` layout, set `StorageSettings(local_legacy_layout=True)`. It is now off by default.

### Changed

- **BREAKING: every JASIL table, index and constraint is prefixed `jasil_`.** `event_log`, `event_outbox`, `processing_jobs`, `job_workers` and `job_attempts` become `jasil_event_log`, `jasil_event_outbox`, `jasil_processing_jobs`, `jasil_job_workers` and `jasil_job_attempts`; indexes read `idx_jasil_...` and the unique constraint `uq_jasil_processing_jobs_event_subscriber`. The tables live in the host's database and registry, and a generic name could collide with the host's own. No column or row changes.
  - Migration `rev0003` renames each table with its indexes, unique constraint and, on PostgreSQL, primary-key index. PostgreSQL renames in place (instant); SQLite and MySQL recreate the indexes under the new names. Downgrading restores the old names and fails if a table of the host's has since taken one.
  - A **fresh** database is created with the final names directly, so a host that already owns a table called `event_log` can install JASIL, and the rename never touches an unprefixed table when a prefixed one exists. This holds for `jasil.migrations.upgrade()` only: a host that runs JASIL's revisions from its own Alembic history gets the unprefixed tables from `rev0001` first, which `rev0003` then renames, so a host table of the same name still collides there.
  - `adopt_existing_schema` recognises an unversioned pre-0.9 schema (at least two of `event_log`, `event_outbox`, `processing_jobs`) and adopts it at its old revision for `upgrade()` to rename; a lone unprefixed `event_log` is taken to be the host's and left alone. PostgreSQL adoption checks the GIN index under whichever name its schema has.
  - `jasil.orm.jasil_table_names()` returns the new names; `jasil.orm.legacy_table_names()` returns the old ones, for migration tooling. `jasil.migrations.jasil_include_object` no longer includes the unprefixed names, so a host's own `event_log` is never diffed or dropped.
- **BREAKING: `GeocodedPlace` carries administrative levels, not one application's vocabulary.** `city` / `town` / `country` become `locality`, `sublocality`, `region`, `country`, `country_code` (ISO 3166-1 alpha-2, upper-case) and `display_name`. Every service is mapped onto the same levels: for Nominatim and geocode.maps.co, `locality` is the first of city, town, village, municipality or hamlet and `sublocality` the first of suburb, city_district, borough, neighbourhood or quarter; for Photon, `locality` is the first of city, town or village and `sublocality` the first of district or suburb. Previously Photon's `district` was reported as `city` and its `city` as `town`, and Nominatim's `town` as "the district". Map the levels onto your own terms in your own code.
- **BREAKING: `GeocodingSettings` is configured by a URI like every other capability.** `provider`, `nominatim_host`, `nominatim_use_https`, `photon_host` and `photon_use_https` are replaced by `uri`: `nominatim://host[:port]` or `photon://host[:port]` (HTTPS), `nominatim+http://...` or `photon+http://...` (cleartext), or `geocode://` (geocode.maps.co; needs `api_key`). An unusable value still disables geocoding and is logged rather than failing startup. `api_key` is now excluded from the settings' `repr`.
- **BREAKING: the flat `{data_dir}/{area}/{key}` storage layout is off by default.** `LocalStorage` used to fall back to it for every read, listing and delete, so any other file under the data directory could be served as an object. It is now opt-in with `LocalStorage(..., legacy_layout=True)` or `StorageSettings(local_legacy_layout=True)`; with it off, an overwrite or delete leaves a flat-layout copy of the key in place. Hosts holding objects written by JASIL 0.3 or earlier, or by the application JASIL was extracted from, need it.
- `StateProvider.record_tiered_failure` and `TieredFailureOutcome` are documented as the general escalating-gate primitive they are, with the argument semantics written out, instead of as a login lockout. No signature or behaviour changes.

### Added

- `JasilSettings.events_stream` (default `jasil:events`) and `events_group` (default `jasil`) name the Redis Stream and consumer group of the event bus, so two deployments sharing a Redis database no longer consume each other's events. Empty values fail the build. Defaults are unchanged.
- `StorageSettings` (`JasilSettings.storage`) with `local_url_prefix`, which sets the prefix of `local://` URLs (the backend always accepted one; nothing could set it), and `local_legacy_layout`.
- `jasil.deps.get_geocoding`, beside the dependency for every other provider.

## [0.8.0] - 2026-10-22

### Added

- `jasil.providers.MAX_REALTIME_KEY_LENGTH` (256), the cap `notify` enforces on a key.

### Changed

- **BREAKING: the realtime hint channel is addressed by an opaque string key, not a user id.** `RealtimeProvider.notify_user(user_id: int, hint)` is now `notify(key: str, hint)`, and `subscribe` handlers are called as `handler(key: str, hint)`. The `key` names the audience (a user id, device id, organisation, cache name, ...); its meaning belongs to the host and JASIL does no per-key routing. There is no deprecated `notify_user` alias. A host that used integer user ids passes `str(user_id)` and parses it back in its handler.
- `notify` raises `ValueError` for a `key` that is not a `str`, is empty, is over 256 characters (`MAX_REALTIME_KEY_LENGTH`, counted in code points), or holds an unpaired Unicode surrogate, in addition to the existing hint and envelope-size checks. Both backends validate identically.
- A received envelope whose `key` or `hint` holds an unpaired Unicode surrogate (sent JSON-escaped), or whose `hint` holds `NaN` / infinity, is logged and dropped instead of reaching the handlers, so a subscriber never sees a hint that `notify` would have refused.
- The `key` appears in the warning logged when a publish fails; do not put a secret in it.
- **BREAKING: the realtime wire envelope is now version 2**, `{"v": 2, "key": "<str>", "hint": {...}}`. Version 1 envelopes from 0.7.0 replicas are still read, with `str(user_id)` as the key, so a rolling upgrade loses no hints in that direction; 0.7.0 replicas ignore version 2, so hints published by already-upgraded processes do not reach not-yet-upgraded ones until the rollout finishes.
- `jasil.testing.capture_hints` returns `list[tuple[str, dict]]` of `(key, hint)`.
- Development tooling moves to uv 0.13: `[tool.uv] required-version` is now `>=0.13.5,<0.14`, the locked `uv` is 0.13.5, and the release build pins the same version. uv is development-only and does not affect installed packages.
- CI now also runs the default SQLite test matrix on Python 3.15, in addition to 3.12, 3.13 and 3.14. The supported Python versions are unchanged.

## [0.7.0] - 2026-10-21

### Added

- **Realtime hint capability.** `RealtimeProvider` (`notify_user(user_id, hint)`, `subscribe(handler)`, `start()`, `stop()`) carries a small per-user "something changed, refetch" hint to every process of the deployment, so a host can push to a browser connection held by any replica, including from a standalone job worker that holds no connections. The host keeps the WebSocket/SSE endpoint, auth and connection registry. Delivery is best-effort, at-most-once and unordered: a hint is a latency optimisation, never the record. Transport failures never raise from `notify_user`; a hint that is not a JSON-serialisable `dict`, a non-`int` `user_id`, or an envelope over 4096 bytes raises `ValueError`. Exported from `jasil`.
- **Realtime backends.** `InProcessRealtime` (`memory://`) delivers inline on the caller's thread. `RedisRealtime` (`redis://`, `rediss://`, `unix://`, the `redis` extra) uses Pub/Sub on one channel (`jasil:realtime`), not the event bus's consumer group, so every started process receives every hint. Its subscriber runs on a daemon thread with a dedicated connection, reconnects with capped exponential backoff and jitter, and PINGs an idle subscription to replace a silently dead one. The wire envelope is `{"v": 1, "user_id": ..., "hint": {...}}`; unknown versions and malformed payloads are logged and ignored. The Redis module is imported only when `realtime_uri` selects Redis.
- **`JasilSettings.realtime_channel`** (default `jasil:realtime`) names the Redis Pub/Sub channel, so deployments sharing one Redis server can isolate their hints.
- **`JasilSettings.realtime_uri`** and `resolved_realtime_uri`. Unset, it follows the resolved `events_uri` in every profile (`memory://` under the `local` defaults), so no existing deployment needs a new setting.
- **`Platform.realtime`**, defaulting to `InProcessRealtime()` so a hand-built `Platform` need not supply it. `Platform.close()` (and so `jasil.lifecycle.shutdown()`) stops it and never raises. `jasil.deps.get_realtime` exposes it to routes, and the startup capability report gains a `realtime` row.
- **Testing helpers.** `jasil.testing.capture_hints(platform)` returns a list collecting every hint published afterwards. `install_test_platform` defaults an unset `realtime_uri` to `memory://` (unless that would fail an enforced multi-process build), and `reset_all` clears the handlers of the active in-process provider.

### Changed

- Refreshed the Python dependency minimums and the locked dependency graph to the newest releases allowed by the supply-chain cooldown. Runtime floors rise to `sqlalchemy>=2.0.54`, `boto3>=1.43.98` (the `s3` extra) and `psycopg[binary]>=3.3.6` (the `postgres` extra).
- The cross-process consistency rule now also covers an explicit `realtime_uri`: `memory://` in a multi-process deployment (`profile=distributed`, `web_workers > 1`, or `job_worker_mode=standalone`) fails the build, or warns with `enforce_deployment_consistency=False`. An unset `realtime_uri` is judged as `events_uri`, which it follows.

### Security

- `certifi` is locked at 2026.10.19, which fixes PYSEC-2026-4320. It is a runtime transitive dependency of `requests` and `botocore`, so the fix is exempt from the cooldown until it ages out of the window. The cooldown exemptions for `cryptography`, `urllib3` and `virtualenv` are removed now that their fixes are older than the window.

## [0.6.1] - 2026-10-18

### Changed

- **`adopt_existing_schema` adopts an older-baseline schema.** Unversioned JASIL tables are matched against the head schema first, as before, then against each earlier revision's schema, newest first. The first exact match is recorded as the database's revision and `True` is returned, so the `upgrade()` that follows applies the later revisions. A database created at `rev0001` (no `queue` column, no `job_workers` or `job_attempts`) is now adopted at `rev0001` instead of being rejected as a partial schema. No match still raises `SchemaCompatibilityError`, now naming the closest revision and listing its differences. An empty database or one that already records a revision still returns `False`. Each older schema is derived from that migration's own `downgrade`, never maintained separately, and a migration using an operation the derivation does not understand fails loudly. There is no schema change.

### Fixed

- **The `queued` event-log row now commits with the outbox row.** `publish_on_commit` and `publish_many_on_commit` used to write the `queued` row on a separate session, so it could be committed when the caller's transaction rolled back, or be missing when it committed. The row is now staged on the caller's session, inside a savepoint, so it commits or rolls back with the outbox row beside it; a storage error is logged and swallowed without poisoning the caller's transaction. `EventLogRecorder.record_queued_in(event, db)` is the new session-aware method, and `event_log.crud.record_queued` takes a keyword-only `commit` (default `True`, unchanged). `EventRecorder` gains no required method: the publisher uses `record_queued_in` when a recorder offers it and otherwise falls back to `record_queued`, so third-party recorders keep working.

## [0.6.0] - 2026-09-15

### Upgrade notes

- **Run the `rev0002` migration before deploying.** It adds the `queue` column (backfilled, with a database-side `default` so writers from the previous release keep working during a rolling deployment), the `job_workers` registry and the `job_attempts` table. Claiming a job writes its attempt row, and with `jobs.record_attempts` off the reaper and the retention prune still touch the table.
- **Standalone workers must declare themselves.** Set `job_worker_mode=standalone` in every process of the deployment (API and workers alike) and call `run_job_worker()`. `start_job_worker()` now requires `in_process` (the default).
- **Review `jobs.lease_seconds`.** It now means how long a worker may go silent, not the longest a handler may run. Startup refuses `lease_seconds` below three `heartbeat_interval_seconds`, and `max_runtime_seconds` below `lease_seconds`.

### Added

- **Named durable-job queues.** Subscriber registrations default to `default` and can select a validated queue; the relay persists it, all-queue workers rotate fairly, and selective workers claim only an explicit non-empty allowlist. Selective workers adopt only their subscribers' legacy `default` rows, and selective `default` workers exclude subscribers assigned to named queues. A worker retags pending legacy `default` rows to their subscribers' queues once at its first claim, and re-reads the registry on every claim. `run_job_worker()` is the supported blocking standalone-worker API.
- **Standalone worker mode.** `JasilSettings.job_worker_mode` (`JobWorkerMode.IN_PROCESS` by default, or `STANDALONE`), with `jasil.profile.parse_job_worker_mode` for an environment value, declares where durable-job consumers run. A standalone worker is one more process reaching state, the event bus, the lock and mail, so declaring it makes every consistency rule see it. `start_job_worker()` requires `in_process` and `run_job_worker()` requires `standalone`; starting the wrong one raises `RuntimeError` even with `enforce_deployment_consistency=False`.
- **Worker registry.** A portable `job_workers` table with restart-unique instance ids (the same UUID keys leases and telemetry), bounded heartbeats that stay live during handlers, graceful-stop timestamps, queue selection, optional host metadata (16 KiB bound), active-claim counts, derived running/stale/stopped status, bounded retention, and cursor-paginated public reads through `jasil.admin`.
- **Lease renewal and runtime cap.** One heartbeat per worker (`jobs.heartbeat_interval_seconds`) renews the lease of the running job and of the jobs still waiting in its batch, and refreshes the registry row. A slow but healthy handler is no longer requeued and run twice, and a `stale` worker is one whose leases are about to expire. `JobSettings.max_runtime_seconds` (default 1800) is the longest a single handler is kept leased: past it the job is abandoned to the reaper, the handler is signalled, an `ERROR` is logged, and the rest of the batch is released back to the queue without spending an attempt. Completion and failure are guarded by worker id plus attempt generation, so a handler that outlives its lease cannot overwrite a replacement worker's claim.
- **`jasil.jobs.lease_lost()`**: a handler's cooperative signal that its lease is gone (stolen, abandoned past the runtime cap, or fenced after failed renewals). `False` outside a durable job. `SmtpMail` and `MemoryMail` check it themselves and raise `MailError` instead of sending.
- **Per-attempt job history.** The `job_attempts` table holds one row per attempt with its job, event, subscriber, queue, attempt number, worker instance, claimed/started/finished times, handler duration (monotonic clock), outcome and error, written in the same transaction as the state change it describes. Outcomes: `completed`, `failed`, `dead_letter`, `lease_expired`, `superseded` (a handler finished after its job was taken: it ran twice), `released` and `abandoned`. `jobs.record_attempts` (on by default) switches the writes off; attempt-derived fields then read `None`, never `0`. `jobs.retention_days` also prunes attempts older than the window, once their job is pruned; the history of `pending`, `claimed` and `dead_letter` jobs is kept.
- **Admin reads.** `jasil.admin.get_job`, `get_event_jobs` and `get_worker_attempts` answer "what happened to this job?", "which worker processed this event, for each subscriber?" and "what has this worker been doing?". `get_jobs_summary` gains per-subscriber and per-queue `avg_duration_ms`, `max_duration_ms`, fixed-boundary `duration_buckets` and `superseded`, and each dead letter's `last_worker_id`. `get_workers_summary` gains `hours`, per-worker outcome counts and `last_finished_at`, and `claimed_by_stale_workers`.
- **Observability.** Job-outcome log records carry `worker_id`, `queue`, `attempts` and `duration_ms`. The reaper's expired-lease warning carries a `workers` breakdown, so a crash names the worker that went silent.
- **Mail capability.** `MailProvider.send(MailMessage) -> str` returns the `Message-ID`. Delivery failures raise `MailError` (`MailNotConfiguredError` when unconfigured), whose `status` and `permanent` expose the SMTP status so a handler can stop retrying a 5xx; an invalid message raises `ValueError`. `Platform.mail` defaults to a `NullMail`, and `jasil.deps.get_mail` and a `mail` row in the startup capability report are added.
- **Mail backends.** `MailSettings` and `JasilSettings.mail`, with URI-selected backends: unset (disabled), `memory://` (records to `outbox`), `smtp://` (STARTTLS) and `smtps://` (implicit TLS). TLS is verified by default; `MailSettings.secure = False` permits cleartext `smtp://` only to a private/loopback relay and never with credentials. `MailSettings.ehlo_hostname` overrides the name announced in `EHLO`. Standard library only, no new dependency.
- **Testing helpers.** `jasil.testing.MemoryMail` and `memory_mail(platform)` for a typed outbox, with `MemoryMail.clear()`. `build_platform(..., clock=...)` lets a test helper supply the clock the backends stamp time with. `install_test_platform` defaults an unset or empty `mail.uri` to `memory://`, except in a multi-process deployment that enforces consistency. `Platform.geocoding` and `Platform.recorder` default to `NullGeocoding()` and `None`, so a hand-built `Platform` need not supply them.
- **Transactional publishing.** `jasil.publisher.publish_on_commit` and `publish_many_on_commit` publish a domain event without owning the commit, so a function that publishes can be composed with other writes in one atomic unit of work. `publish_committing` and `publish_many_committing` are unchanged; hosts can adopt the new functions incrementally.
  - **Durable route:** the outbox row is staged on the session immediately, uncommitted, and commits or rolls back with the caller's domain rows. A staging failure raises.
  - **Best-effort route:** the event is minted at publish time (capturing the ambient request id) and dispatched on the bus only after the session's outermost transaction commits, in publish order. Events from a rolled-back SAVEPOINT, or a rolled-back or closed transaction, are never dispatched; events from a released SAVEPOINT wait for the outermost commit. Dispatch failures are logged and swallowed. Pending events live in `Session.info`, so sessions never see each other's.
  - Publishing starts the session's transaction if none is active; an empty `publish_many_on_commit` is a no-op. Only synchronous `Session` objects are supported; anything else (an `AsyncSession`, a `scoped_session` proxy) raises `TypeError`.
- **Deployment topology.** `DeploymentTopology.multi_process`, `multi_node` and `multi_process_causes`, and `jasil.capabilities.topology_of(settings)`, the one place settings become a topology. Consistency refusals say what made the deployment multi-process (`profile=distributed`, `web_workers=N`, `job_worker_mode=standalone`), and the startup capability report shows `job_worker_mode`, `multi_process` and `multi_node`.
- **New consistency rules.** `jasil.capabilities.check_mail_consistency` refuses `memory://` mail in a multi-process deployment, where no message would be delivered. `jasil.capabilities.check_job_worker_consistency` refuses `job_worker_mode=standalone` while `jobs.enabled` is false.

### Changed

- **Breaking (0.x):** every consistency rule takes the resolved topology instead of its raw inputs. `check_state_consistency`, `check_lock_consistency`, `check_storage_consistency` and `check_mail_consistency` take `topology=` in place of `profile=` and `web_workers=` (pass `topology_of(settings)`), and `resolve_topology` takes keyword-only `profile`, `web_workers` and `job_worker_mode`. `DeploymentTopology.requires_shared_state` is removed: use `multi_process` for state, the lock and mail, and `multi_node` for storage.
- **Breaking (0.x):** `run_job_worker()` requires `job_worker_mode=standalone`, and `start_job_worker()` requires `in_process`. Existing deployments that call `run_job_worker()` must set `job_worker_mode=standalone` in every process.
- `jobs.lease_seconds` now means how long a worker may go silent before its jobs are reclaimed. `build_runner` (both worker entry points) refuses `lease_seconds` below three `heartbeat_interval_seconds`, a non-positive `heartbeat_interval_seconds`, and a `max_runtime_seconds` below `lease_seconds`. The mail lease warning at startup is keyed on `max_runtime_seconds`.
- SQLite is limited to one API process with one in-process all-queue consumer. A durable-job consumer is refused on SQLite whenever the configured topology is multi-process, which includes `profile=distributed` with a single web worker; standalone selective workers require a concurrency-capable database such as PostgreSQL.
- Internal, not covered by SemVer: `JobRunner` requires `max_runtime_seconds` and `heartbeat_interval_seconds`, and `run_worker` / `BackgroundWorker` take `heartbeat=`. Hosts use `build_runner`, `start_job_worker` and `run_job_worker`.

### Fixed

- An IPv4-mapped IPv6 address (`::ffff:a.b.c.d`) is now judged by its embedded IPv4 address in the outbound-host check, so a public address is no longer refused on Python 3.12.0-3.12.3.

### Tested

- Real PostgreSQL conformance for selective and competing workers, `FOR UPDATE SKIP LOCKED`, deferred queue draining, lease recovery, worker lifecycle status, and queue/worker summaries under concurrent claims.

## [0.5.1]

### Changed

- Refreshed all Python dependency minimums and the locked dependency graph to the newest releases allowed by the supply-chain cooldown.
- Updated pinned GitHub Actions to their latest releases: `astral-sh/setup-uv`, `github/codeql-action`, `actions/upload-artifact`, `actions/download-artifact`, and `ossf/scorecard-action`.

### Security

- Updated `urllib3` to 2.8.0, which fixes PYSEC-2026-4175, PYSEC-2026-4176, and PYSEC-2026-4177. It is exempted from the supply-chain cooldown until the release ages out of the window.

## [0.5.0]

### Breaking

- `jasil.migrations.stamp` has been removed because it could certify an incompatible physical schema. Hosts adopting complete, unversioned JASIL tables must use `adopt_existing_schema(engine)`, which validates the installed head schema before recording its revision and fails closed on partial or incompatible tables.

### Added

- `jasil.migrations.adopt_existing_schema` safely adopts complete, compatible, unversioned JASIL tables without requiring hosts to know a migration revision or duplicate JASIL's schema definition.
- `jasil.migrations.SchemaCompatibilityError` reports actionable physical-schema differences before adoption without modifying or repairing the database.

### Changed

- Refreshed all Python dependency minimums and the locked dependency graph to the current tested baseline allowed by the supply-chain cooldown.
- Updated pinned GitHub Actions to their latest releases except `astral-sh/setup-uv`, which remains unchanged together with the project's uv constraints.

## [0.4.0]

### Breaking

- `StorageProvider` now requires `begin_upload`, `upload_part`, `complete_upload`, `abort_upload`, and `cleanup_uploads`. This intentional pre-1.0 protocol expansion requires third-party storage backends to implement the resumable lifecycle before they satisfy the runtime-checkable protocol.
- Storage areas, keys, and prefixes must now be canonical slash-delimited paths. Dot components, repeated or trailing separators, and backslashes are rejected before I/O so local path normalization cannot address or delete a different object set than S3. Areas are single namespace components; hierarchy belongs in keys, preserving the area/key boundary on every backend.
- `UploadSession` exposes an opaque `session_id`; native backend upload IDs stay private.
- `PartRef` exposes an opaque `validator` instead of the S3-shaped `etag` name, and `upload_part` accepts an exact-size, read-once `BinaryIO` instead of buffering a whole part as `bytes`.
- `.jasil-objects` and `.jasil-upload-sessions` are reserved area names used by private backend state.

### Added

- Durable resumable upload sessions with provider-neutral `UploadSession` and size-bearing `PartRef` values. Parts can arrive out of order, replacement is atomic, completion validates every current reference in ascending order, and the destination changes only when completion succeeds.
- Portable multipart constraints and session-wide `max_bytes` enforcement on both built-in backends. The 5 MiB minimum, 5 GiB maximum, and 10,000-part cap are JASIL's built-in portability limits. Invalid, cleaned, completed, or foreign sessions raise `StorageUploadSessionError` without leaking filesystem or S3 exceptions.
- Bounded-memory part uploads from non-seekable sources with required exact byte sizes and retryable short/long-source failures.
- Cross-process local sessions backed by private filesystem staging, and S3 sessions backed directly by native multipart upload operations.
- Idempotent upload aborts and explicit age-based cleanup for abandoned local and S3 sessions.
- Composable `StorageObjects`, `StorageStreams`, `StorageDelivery`, `StorageManagement`, and `ResumableUploads` protocols. `StorageProvider` remains their complete aggregate on `Platform.storage`.

### Changed

- Local storage uses a private versioned leaf-file layout so an object can coexist with descendant keys, matching S3. Objects written by JASIL 0.3 and earlier remain readable and migrate when overwritten.
- S3 resumable sessions persist private manifests that map opaque session IDs to native multipart uploads. Cleanup follows only those manifests and no longer risks aborting unrelated multipart work under the configured prefix.

## [0.3.0]

### Breaking

- `StorageProvider` now requires `serve`, `stat`, `copy`, and `delete_prefix`. This is another intentional pre-1.0 protocol expansion: existing calls remain source-compatible, but third-party storage backends must implement the new members before they satisfy the runtime-checkable protocol.

### Added

- Framework-neutral serving plans. Local storage returns `ServeFile` for zero-copy file responses and reverse-proxy handoff, S3 returns a presigned `ServeRedirect`, and custom backends may return `ServeStream`. Both built-in backends verify object existence before creating a plan.
- `ObjectStat` metadata with size and modification time on both backends, plus content type and ETag where the backend exposes them. Portable local filesystems return `None` for the latter two fields.
- Backend-native object copying. Local copies remain atomic, while S3 uses boto3's managed copy so large objects switch to multipart copy without passing bytes through the application process.
- Boundary-safe subtree deletion with `delete_prefix`, returning the number of objects removed and batching S3 deletions at 1,000 keys per request.

### Changed

- Local `delete` and `delete_prefix` now prune empty directories while retaining the configured storage root.
- Local object operations reject filesystem paths that traverse symbolic links, and listings omit symlink aliases, preserving area and subtree boundaries if the storage tree is modified outside JASIL.

## [0.2.0]

### Breaking

- `StorageProvider` now requires `save_stream`, range-aware `open_stream`, `iter_objects`, and `check_writable`, and widens `url` with optional response controls. This is intentionally slated for the next minor release: provider protocols are runtime-checkable and custom backends are supported, so adding required members breaks those backends even though existing callers of `save`, `get`, and `url` remain source-compatible. The new members are not optional capability checks; third-party storage backends must implement the complete contract.

### Added

- Bounded-memory storage I/O. `save_stream` consumes non-seekable sources and enforces `max_bytes` mid-stream; local disk writes through a temporary file, while S3 uses multipart upload and aborts it on failure. `open_stream` returns the same read-once, non-seekable stream contract on both backends and supports `offset`/`length` range selection. Missing objects raise `FileNotFoundError`.
- Lazy reconciliation via `iter_objects`, yielding each key with its modification epoch, and a storage-specific `check_writable` readiness probe.
- `StorageBackendUnavailableError` hides local filesystem and botocore failures, and `StorageSizeLimitError` identifies a streaming size-limit breach.
- S3 presigned URLs accept `download_as` and `content_type`, allowing a host to force attachment download and pin the response media type for untrusted blobs.

### Changed

- The local backend warns once when URL expiry or response-header controls are requested, because those controls belong to the host web server for `local://`. Existing whole-object `save`/`get` behavior remains available for small blobs.

## [0.1.1]

### Security

- The reverse-geocoding backend no longer logs the coordinates it was asked to resolve. A latitude/longitude pair is a location fix belonging to the host's user, and a library does not get to decide that belongs in someone's log. The debug line still names the upstream service, which is what it was useful for. Only the `geocoding` extra reached this, and only at `DEBUG`.

### Changed

- Development lockfile only: `cryptography` moved to `50.0.0` for CVE-2026-69247. It is reached through the `hatch` toolchain and is not a dependency of JASIL or any of its extras, so no installed copy of `0.1.0` ever contained it.

## [0.1.0]

First release. Extracted from [Endurain](https://github.com/endurain-project/endurain),
where this code ran in production as an internal `infra` package. The API is
still settling: `0.x` releases may break it, and the SemVer guarantees in
[API stability](docs/api-stability.md) begin at `1.0.0`.

### Added

**Capability providers and backends**

- `StateProvider` — ephemeral keyed state with TTLs, backed by process-local memory (`memory://`) or Redis (`redis://` / `rediss://` / `unix://`). Beyond plain key/value access it exposes the atomic primitives correctness actually depends on — `set_if_absent`, `get_and_delete`, and `record_tiered_failure` (an atomic tiered lockout, implemented under a lock in memory and as a Lua script on Redis) — so a store behaves identically on either backend.
- `StorageProvider` — opaque blob storage addressed by `(area, key)`, backed by the local filesystem (`local://`) or S3-compatible object storage (`s3://`). Both backends refuse the same addresses — empty, absolute, or containing a `..` component — before touching a disk or a client, so the contract a caller sees does not change with the deployment. `list_keys` is recursive on both, so a nested key is listed wherever it is stored.
- `EventBusProvider` — synchronous in-process dispatch (`memory://`) or Redis Streams with a consumer group (`redis://`), giving competing-consumer semantics across replicas.
- `LockProvider` — a no-op lock (`noop://`) or PostgreSQL session-level advisory locks (`postgres-advisory://`), which need no infrastructure beyond the database the host already has.
- `ClockProvider` and `GeocodingProvider`, the latter covering Nominatim, Photon, and geocode.maps.co behind one interface.
- A Redis outage surfaces to callers as `StateBackendUnavailableError`, so domain code never imports or catches a redis-py exception.

**Deployment profiles**

- `local`, `distributed`, and `custom` profiles. The profile supplies the default for each capability URI, and the `local` profile is the zero-config default: memory state, local disk, in-process events, no-op lock.
- `distributed` and `custom` **refuse to start** when a capability URI is unset rather than falling back to a process-local backend, which across replicas would diverge silently — the failure the profile system exists to prevent.
- A startup capability report showing how each capability resolved and why, plus consistency checks that reject a multi-process topology wired to process-local state. `build_platform()` runs both before it constructs a single backend; set `enforce_deployment_consistency=False` to downgrade a refusal to a warning.

**Events**

- One immutable `Event` envelope with an ISO-8601 UTC timestamp, a UUIDv4 id stable across retries, and a caller-owned `metadata` dict.
- `event_id`, `event_type` and `source` are length-checked when the envelope is minted, and `subscriber_id` when a durable subscriber registers, so a value too long for the column it is persisted in raises at the producing call site instead of failing at the write — where PostgreSQL and MySQL raise, SQLite does not, and the publish seam swallows the failure either way. `payload` and `metadata` are deliberately *not* capped; see the events documentation for what belongs in them.
- Payload schema versioning: `VersionedPayload` carries a `SCHEMA_VERSION` and per-step `UPGRADERS`. A payload written by an older build is walked forward one version at a time; one written by a *newer* build is refused rather than silently misread — the failure mode during a rolling deploy.
- `publisher.publish` is the single publish seam. Delivery failures are logged and swallowed so a publish never breaks the producer.
- `publish_committing` / `publish_many_committing` own the commit ordering, so durable delivery can be made atomic with the caller's domain write.
- `subscribers.best_effort` wraps a raising handler into a swallowing bus subscriber, so derived work can never fail the request that produced the event.

**Durable jobs**

- A transactional outbox relayed into leased per-subscriber jobs, with exponential backoff (equal jitter, to avoid a retry stampede), an attempt ceiling, and a dead-letter queue with replay.
- Idempotent consumers: `(event_id, subscriber_id)` uniqueness is enforced by the database, so a re-delivered event never runs a subscriber twice.
- Lease reclamation returns work stranded by a crashed worker; an attempt is counted at claim time, which is what bounds a crash loop.
- A worker's identity is bounded to the width of the lease column it is written to. When a long hostname forces truncation it carries a digest of the full value, so two machines sharing a hostname prefix never collapse onto one lease holder — which would hand each of them the other's claimed rows.
- On PostgreSQL, claiming and relaying use `FOR UPDATE SKIP LOCKED` so concurrent workers and relayers take disjoint batches with no coordinating lock.
- `DurableSubscriberNet` declares the reconciliation net — a scheduled backfill, or a documented exemption — that every subscriber writing durable derived state owes, since delivery is at-least-once but not guaranteed. `assert_nets_complete` holds the whole registry to it, so a missing net fails your own test suite rather than surfacing in production.

**Observability**

- An `event_log` table recording each event's lifecycle, written by the bus and the publish facade, with dashboard aggregates.
- Identifiers that are too long for the column they are persisted in are refused at the producing call site; derived, diagnostic values (failure text, the joined subscriber list, a worker identity) are truncated with a marker instead, so a reader can tell the value was cut.
- Retention pruning in bounded batches, on independently configurable windows. In-flight rows and dead-letters are never pruned. Register it with `jasil.retention.schedule_retention_maintenance(scheduler)`, the counterpart of `jasil.jobs.service.schedule_job_maintenance` — separate because retention also covers the `event_log`, so it applies without durable jobs.

**Host integration**

- The **host** owns the declarative base and the engine. JASIL maps its tables into the host's registry via `map_models(Base)` and takes a session factory via `configure_sessionmaker(...)`. JASIL never creates an engine. `map_models` must run before `jasil.jobs.crud` or `jasil.event_log.crud` is imported; every other public module — `jasil.publisher` above all, which every producer imports at module scope — defers its model imports and is safe to import from anywhere in the host's import graph.
- `jasil.settings` — immutable, grouped configuration installed by the host. JASIL reads no environment variables and no secret files.
- `jasil.correlation` — a pluggable correlation-id provider, defaulting to a module-local context variable, so events carry the host's request id.
- Packaged Alembic revisions (`jasil[migrations]`) on their own version table, `jasil_alembic_version`, scoped to JASIL's tables so the host's Alembic history and tables are never touched.
- FastAPI dependency helpers behind the `fastapi` extra, resolving `app.state.platform` when the host attached one and otherwise the process-wide platform, so the quick-start wiring needs nothing extra.
- `jasil.admin` — the operator-facing surface: `get_jobs_summary`, `get_event_log_summary`, and `replay_dead_letter_job`, plus the response schemas. Importable from anywhere in the host's import graph and takes no session, so an admin route cannot hand JASIL its own open transaction. The CRUD modules behind it stay internal.
- `jasil.testing` — `FixedClock`, `install_test_platform`, and `reset_all`, so a host's suite does not have to rediscover which process-wide slots JASIL installs. `reset_all` deliberately leaves the ORM mapping in place; model modules capture the declarative base at import time.
- `Platform.close()` releases what the platform owns — the event-bus consumer thread and the shared Redis clients — and never raises, so a shutdown failure cannot mask whatever prompted the shutdown. The durable-job worker and the host's engine are stopped separately, because the platform does not own them.
- `jasil.lifecycle.shutdown()` composes the two halves of that in the order that matters — the worker stops before the bus its subscribers publish through — so a host does not have to know the ordering. Idempotent, safe before anything has started, and never raises.

**Packaging**

- A core install requires only `sqlalchemy` and `pydantic`. Every backend client lives behind an extra (`redis`, `s3`, `postgres`, `jobs`, `fastapi`, `geocoding`, `migrations`, `all`) and is imported lazily, so a single-process deployment loads none of them.
- Ships `py.typed`.

### Security

- SSRF guard on every outbound host JASIL dials on the host's behalf. All resolved addresses must be public unicast — a single private, loopback, link-local, or reserved answer rejects the host, which is what defends against DNS rebinding. An allowlist escape hatch exists for self-hosted services on private networks, and every use of it is logged for audit. Configured values must be a bare `host[:port]`, so one carrying a scheme or path cannot redirect a request elsewhere. An allowlist entry that is a *hostname* exempts every address that name resolves to, so taking one is logged at `WARNING` naming the address and recommending a CIDR; a CIDR exemption logs at `INFO`.
- The geocoding backend refuses redirects, so a permitted host cannot 3xx-pivot onto an internal target. Its response body is read under a size cap, and its failures are logged by exception type and status code only: `requests` puts the request URL in an error message, and that URL carries the API key.
- The Redis state backend escapes glob metacharacters before turning a caller's key prefix into a `SCAN`/`MATCH` pattern, so a prefix holding `*`, `?` or `[...]` matches only itself and never widens onto keys the caller did not name. A prefix built from a tenant or user identifier is therefore safe.
- The local storage backend rejects absolute and parent-traversal area and key values before any filesystem access, and percent-encodes both into the URL it returns, so a key holding `?`, `#` or `%` cannot alter the URL it lands in.

[0.8.0]: https://github.com/endurain-project/jasil/releases/tag/v0.8.0
[0.7.0]: https://github.com/endurain-project/jasil/releases/tag/v0.7.0
[0.6.0]: https://github.com/endurain-project/jasil/releases/tag/v0.6.0
[0.5.1]: https://github.com/endurain-project/jasil/releases/tag/v0.5.1
[0.5.0]: https://github.com/endurain-project/jasil/releases/tag/v0.5.0
[0.4.0]: https://github.com/endurain-project/jasil/releases/tag/v0.4.0
[0.3.0]: https://github.com/endurain-project/jasil/releases/tag/v0.3.0
[0.2.0]: https://github.com/endurain-project/jasil/releases/tag/v0.2.0
[0.1.1]: https://github.com/endurain-project/jasil/releases/tag/v0.1.1
[0.1.0]: https://github.com/endurain-project/jasil/releases/tag/v0.1.0
