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
| `jasil.providers` | The capability protocols and their method signatures. |
| `jasil.events` | `Event`, `new_event`, `META_REQUEST_ID`, `INITIAL_SCHEMA_VERSION`, `MAX_EVENT_ID_LENGTH`, `MAX_EVENT_TYPE_LENGTH`, `MAX_SOURCE_LENGTH`. |
| `jasil.event_versioning` | `VersionedPayload`, `parse_payload`, `UnsupportedEventVersionError`. |
| `jasil.profile` | `DeploymentProfile`, `parse_profile`, `classify_state_uri`, topology helpers. |
| `jasil.settings` | The settings dataclasses and `configure` / `get_settings`. |
| `jasil.orm` | `map_models`, `configure_sessionmaker`, `get_sessionmaker`, `get_engine`, `Base`. |
| `jasil.correlation` | `configure_provider`, `get_correlation_id`, `set_correlation_id`. |
| `jasil.container` | `Platform`, `build_platform`. |
| `jasil.capabilities` | `check_deployment_consistency` and the individual `check_*` rules. The *rendered* report is not covered — see below. |
| `jasil.runtime` | `set_active_platform`, `get_active_platform`, `is_platform_active`, `get_state`, `reset`. |
| `jasil.lifecycle` | `shutdown`, and the order in which it releases what JASIL owns. |
| `jasil.testing` | `FixedClock`, `install_test_platform`, `reset_all`. Covered because a host's test suite depends on it as much as its production code does. |
| `jasil.admin` | `get_jobs_summary`, `get_event_log_summary`, `replay_dead_letter_job`, and the response schemas re-exported alongside them. |
| `jasil.publisher` | `publish`, `publish_committing`, `publish_many_committing`. |
| `jasil.subscribers` | `best_effort`. |
| `jasil.deps` | `get_platform`, `get_state`, `get_storage`, `get_events`, `get_lock`, `get_clock`, and the order they resolve a platform in. |
| `jasil.jobs.service` | `start_job_worker`, `stop_job_worker`, `schedule_job_maintenance`, `build_runner`. |
| `jasil.retention` | `prune_expired_records`, `schedule_retention_maintenance`. |
| `jasil.jobs.registry` | `JobHandlerRegistry`, `MAX_SUBSCRIBER_ID_LENGTH`, and the process-wide `registry`. |
| `jasil.jobs.reconciliation` | `DurableSubscriberNet`, `undeclared_subscribers`, `assert_nets_complete`. |
| `jasil.migrations` | `upgrade`, `downgrade`, `stamp`, `head_revision`, `db_revision`, `verify_schema_current`. |

### Provider protocols

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
(`serialize_event` / `deserialize_event`). A replica running the old version must
be able to read what a new one writes, or a rolling deploy breaks.

### Database schema

The columns, indexes and constraints of `event_log`, `event_outbox` and
`processing_jobs`. Schema changes ship as Alembic revisions, and a revision must
be forward-compatible with the previous release: during a rolling deploy the old
code runs against the new schema.

Removing a column is a major change. Adding a nullable one is not.

### Exception types

`StateBackendUnavailableError`, `StorageBackendUnavailableError`,
`StorageSizeLimitError`, `StorageUploadSessionError`, and
`UnsupportedEventVersionError` — what raises them and what they inherit from.

## Not covered

These may change in any release, including a patch.

### Private modules

`jasil._core` is internal, and the leading underscore is the notice. It holds
vendored helpers (the SSRF guard, the config slot) whose signatures serve JASIL's
needs, not yours.

Anything else prefixed with `_`, at any level.

### Log messages

The text, level, and structured fields of any log record. Do not parse them or
alert on their exact wording.

### Error message text

The `str()` of an exception. The **type** is stable; the wording is not.

### Defaults

Tuning defaults — `lease_seconds`, `batch_size`, backoff parameters, retention
windows — may be adjusted in a minor release if a better value is found. Pin any
you depend on by setting them explicitly.

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
