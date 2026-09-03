# Changelog

All notable changes to JASIL are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

What counts as a breaking change — and what does not — is defined in [API stability](docs/api-stability.md). In short: the `jasil` namespace, the capability provider protocols, the event envelope, the capability URI schemes, and the database schema of JASIL's own tables are covered by SemVer; anything under `jasil._core`, log message text, and the wording of error messages are not.

## [Unreleased]

### Added

- Named durable-job queues. Subscriber registrations default to `default` and
  can select a validated queue; the relay persists it, all-queue workers rotate
  fairly, and selective workers claim only an explicit non-empty allowlist.
  `run_job_worker()` is the supported blocking standalone-worker API.
- A portable `job_workers` registry with restart-unique instance ids, bounded
  heartbeats that remain live during handlers, graceful-stop timestamps, queue
  selection, optional host metadata, active-claim counts, derived
  running/stale/stopped status, cursor-paginated public reads through
  `jasil.admin`, 16 KiB worker-metadata bounds, and bounded retention.
- Async admin and lifecycle wrappers that offload synchronous database work and
  bounded thread joins from an application's event-loop thread.
- Real PostgreSQL conformance for selective and competing workers,
  `FOR UPDATE SKIP LOCKED`, deferred queue draining, lease recovery, worker
  lifecycle status, and queue/worker summaries under concurrent claims.

### Changed

- Durable job leases now use the restart-unique worker instance UUID that also
  keys telemetry. SQLite is explicitly limited to one API process with one
  in-process all-queue consumer; standalone selective workers require a
  concurrency-capable database such as PostgreSQL.
- The queue migration backfills existing rows and keeps a database-side
  `default` default so writers from the previous release remain compatible
  during rolling deployment. Selective workers adopt only their subscribers'
  legacy `default` rows, and selective `default` workers exclude subscribers
  assigned to named queues.
- Job completion and failure use the worker id plus attempt generation as a
  compare-and-set token, so a handler that outlives its lease cannot overwrite a
  replacement worker's newer claim.

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

[0.4.0]: https://github.com/endurain-project/jasil/releases/tag/v0.4.0
[0.3.0]: https://github.com/endurain-project/jasil/releases/tag/v0.3.0
[0.2.0]: https://github.com/endurain-project/jasil/releases/tag/v0.2.0
[0.1.1]: https://github.com/endurain-project/jasil/releases/tag/v0.1.1
[0.1.0]: https://github.com/endurain-project/jasil/releases/tag/v0.1.0
