# Changelog

All notable changes to JASIL are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

What counts as a breaking change — and what does not — is defined in
[API stability](docs/api-stability.md). In short: the `jasil` namespace, the
capability provider protocols, the event envelope, the capability URI schemes,
and the database schema of JASIL's own tables are covered by SemVer; anything
under `jasil._core`, log message text, and the wording of error messages are not.

## [0.1.0]

First release. Extracted from [Endurain](https://github.com/endurain-project/endurain),
where this code ran in production as an internal `infra` package. The API is
still settling: `0.x` releases may break it, and the SemVer guarantees in
[API stability](docs/api-stability.md) begin at `1.0.0`.

### Added

**Capability providers and backends**

- `StateProvider` — ephemeral keyed state with TTLs, backed by process-local
  memory (`memory://`) or Redis (`redis://` / `rediss://` / `unix://`). Beyond
  plain key/value access it exposes the atomic primitives correctness actually
  depends on — `set_if_absent`, `get_and_delete`, and `record_tiered_failure`
  (an atomic tiered lockout, implemented under a lock in memory and as a Lua
  script on Redis) — so a store behaves identically on either backend. Both
  backends are held to one shared conformance test suite for exactly that reason.
- `StorageProvider` — opaque blob storage addressed by `(area, key)`, backed by
  the local filesystem (`local://`) or S3-compatible object storage (`s3://`).
  Every area and key is validated against path traversal before touching the
  filesystem.
- `EventBusProvider` — synchronous in-process dispatch (`memory://`) or Redis
  Streams with a consumer group (`redis://`), giving competing-consumer
  semantics across replicas.
- `LockProvider` — a no-op lock (`noop://`) or PostgreSQL session-level advisory
  locks (`postgres-advisory://`), which need no infrastructure beyond the
  database the host already has.
- `ClockProvider` and `GeocodingProvider`, the latter covering Nominatim,
  Photon, and geocode.maps.co behind one interface.
- A Redis outage surfaces to callers as `StateBackendUnavailableError`, so
  domain code never imports or catches a redis-py exception.

**Deployment profiles**

- `local`, `distributed`, and `custom` profiles. The profile supplies the
  default for each capability URI, and the `local` profile is the zero-config
  default: memory state, local disk, in-process events, no-op lock.
- `distributed` and `custom` **refuse to start** when a capability URI is unset
  rather than falling back to a process-local backend, which across replicas
  would diverge silently — the failure the profile system exists to prevent.
- A startup capability report showing how each capability resolved and why, plus
  consistency checks that reject a multi-process topology wired to
  process-local state.

**Events**

- One immutable `Event` envelope with an ISO-8601 UTC timestamp, a UUIDv4 id
  stable across retries, and a caller-owned `metadata` dict.
- Payload schema versioning: `VersionedPayload` carries a `SCHEMA_VERSION` and
  per-step `UPGRADERS`. A payload written by an older build is walked forward
  one version at a time; one written by a *newer* build is refused rather than
  silently misread — the failure mode during a rolling deploy.
- `publisher.publish` is the single publish seam. Delivery failures are logged
  and swallowed so a publish never breaks the producer.
- `publish_committing` / `publish_many_committing` own the commit ordering, so
  durable delivery can be made atomic with the caller's domain write.
- `subscribers.best_effort` wraps a raising handler into a swallowing bus
  subscriber, so derived work can never fail the request that produced the event.

**Durable jobs**

- A transactional outbox relayed into leased per-subscriber jobs, with
  exponential backoff (equal jitter, to avoid a retry stampede), an attempt
  ceiling, and a dead-letter queue with replay.
- Idempotent consumers: `(event_id, subscriber_id)` uniqueness is enforced by
  the database, so a re-delivered event never runs a subscriber twice.
- Lease reclamation returns work stranded by a crashed worker; an attempt is
  counted at claim time, which is what bounds a crash loop.
- On PostgreSQL, claiming and relaying use `FOR UPDATE SKIP LOCKED` so
  concurrent workers and relayers take disjoint batches with no coordinating lock.
- `DurableSubscriberNet` declares the reconciliation net — a scheduled backfill,
  or a documented exemption — that every subscriber writing durable derived state
  owes, since delivery is at-least-once but not guaranteed. `assert_nets_complete`
  holds the whole registry to it in one conformance test.

**Observability**

- An `event_log` table recording each event's lifecycle, written by the bus and
  the publish facade, with dashboard aggregates.
- Retention pruning in bounded batches, on independently configurable windows.
  In-flight rows and dead-letters are never pruned.

**Host integration**

- Option B ORM: the **host** owns the declarative base and the engine. JASIL maps
  its tables into the host's registry via `map_models(Base)` and takes a session
  factory via `configure_sessionmaker(...)`. JASIL never creates an engine.
- `jasil.settings` — immutable, grouped configuration installed by the host.
  JASIL reads no environment variables and no secret files.
- `jasil.correlation` — a pluggable correlation-id provider, defaulting to a
  module-local context variable, so events carry the host's request id.
- Packaged Alembic revisions (`jasil[migrations]`) on their own version table,
  `jasil_alembic_version`, scoped to JASIL's tables so the host's Alembic
  history and tables are never touched.
- FastAPI dependency helpers behind the `fastapi` extra.
- `jasil.async_bridge`, for synchronous code that must hand work to the main
  event loop.

**Packaging**

- A core install requires only `sqlalchemy` and `pydantic`. Every backend client
  lives behind an extra (`redis`, `s3`, `postgres`, `jobs`, `fastapi`,
  `geocoding`, `migrations`, `all`) and is imported lazily, so a single-process
  deployment loads none of them. This is enforced by a test, not just intended.
- Ships `py.typed`.
- Architectural import contracts (`lint-imports`) encode the invariants the
  module docstrings describe: the pure providers never reach a backend, only the
  composition root selects one, the substrate reaches the jobs layer only through
  the publisher, and the vendored `_core` helpers stay leaf utilities.

### Security

- SSRF guard on every outbound host JASIL dials on the host's behalf. All
  resolved addresses must be public unicast — a single private, loopback,
  link-local, or reserved answer rejects the host, which is what defends against
  DNS rebinding. An allowlist escape hatch exists for self-hosted services on
  private networks, and every use of it is logged for audit. Configured values
  must be a bare `host[:port]`, so one carrying a scheme or path cannot redirect
  a request elsewhere.
- The geocoding backend refuses redirects, so a permitted host cannot 3xx-pivot
  onto an internal target.
- The local storage backend rejects absolute and parent-traversal area and key
  values before any filesystem access.

[0.1.0]: https://github.com/endurain-project/jasil/releases/tag/v0.1.0
