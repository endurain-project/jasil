# Providers & backends

A **provider** is a protocol your domain code depends on. A **backend** is a
concrete implementation of one. The composition root
(`jasil.container.build_platform`) is the only place that decides which backend
serves which provider, so everything else stays swappable.

```
domain code  ──depends on──▶  jasil.providers  ◀──implements──  jasil.backends
                                                                      ▲
                                                          selected by │
                                                       jasil.container┘
```

This is enforced, not merely intended: import contracts fail the build if
`jasil.providers` ever imports a backend, or if anything outside the composition
root selects one. That invariant is what keeps `import jasil` from dragging in
`redis` or `boto3`.

## StateProvider

Ephemeral keyed state: counters, TTL flags, small blobs.

| Backend | URI | Notes |
|---|---|---|
| `MemoryState` | `memory://` | Process-local dict with per-key TTL. Not shared across workers. |
| `RedisState` | `redis://`, `rediss://`, `unix://` | Shared across workers and replicas. |

```python
platform.state.set("session:abc", b"payload", ttl_seconds=3600)
platform.state.get("session:abc")  # b"payload" | None
platform.state.incr("rate:1.2.3.4", ttl_seconds=60)
```

Beyond plain key/value access it exposes the primitives whose correctness would
otherwise depend on the backend:

- **`set_if_absent(key, value, ttl)`** — atomic claim. Returns `True` for exactly
  one caller.
- **`get_and_delete(key)`** — atomic read-and-consume, for single-use tokens.
- **`record_tiered_failure(...)`** — an atomic tiered lockout. Under a lock in
  memory, as a Lua script on Redis. A caller that is already locked out gets its
  count back *without* incrementing, so hammering while locked cannot inflate the
  counter into the next tier and extend its own lockout.

!!! note "One conformance suite, both backends"
    The two backends are held to a single shared test suite, because a
    behavioural difference between them is a bug that only appears after you
    switch profile — the worst time to find it.

A Redis outage surfaces as `StateBackendUnavailableError`, never as a redis-py
exception, so domain code catching it needs no knowledge of the backend. The
memory backend never raises it.

## StorageProvider

Opaque byte blobs addressed by a key within a named *area*.

| Backend | URI | Notes |
|---|---|---|
| `LocalStorage` | `local://`, `local://<path>` | Files under `{data_dir}/{area}/{key}`. |
| `S3Storage` | `s3://<bucket>` | Key prefix per area. Requires the `s3` extra. |

```python
platform.storage.save("avatars", "42.webp", data)
platform.storage.get("avatars", "42.webp")  # bytes | None
platform.storage.url("avatars", "42.webp")
platform.storage.list_keys("avatars", prefix="42-")
```

An area is a domain-owned namespace. Store only the key in your database — `url`
is computed at serialization time, so migrating local → S3 needs no data
migration.

`list_keys` exists for subsystems whose keys are not derivable from a domain id
(for example when a key carries a random component). Without it, such a subsystem
would have to reach past the provider to the filesystem to clean up, which is
exactly what the provider exists to prevent.

!!! warning "Traversal is rejected"
    Area and key values are validated before any filesystem access; an absolute
    path or a `..` segment raises `ValueError`. Keys are expected to be
    server-generated, but a stray value must never escape the storage root.

## EventBusProvider

| Backend | URI | Notes |
|---|---|---|
| `InProcessEventBus` | `memory://` | Synchronous. `publish` runs subscribers inline. |
| `RedisStreamEventBus` | `redis://`, `rediss://`, `unix://` | Redis Streams with a consumer group. |

In-process dispatch is a direct function call, so a handler exception propagates
to the publisher. Under Redis Streams, replicas form one consumer group, giving
competing-consumer semantics: each derived computation runs once per event across
the cluster, while in-process fan-out to every handler of an event type happens
on whichever replica claims the entry.

Redis delivery is at-least-once — an entry is acked only after its handlers
succeed. There is no in-bus retry or reclaim: an entry orphaned by a crashed
consumer stays pending. For retry, backoff, dead-lettering and replay, enable
[durable jobs](durable-jobs.md).

## LockProvider

| Backend | URI | Notes |
|---|---|---|
| `NoopLock` | `noop://` | Always acquires. Correct for one process. |
| `PgAdvisoryLock` | `postgres-advisory://` | Session-level advisory locks on the host's database. |

```python
with platform.lock.try_acquire("nightly-backfill") as acquired:
    if acquired:
        run_backfill()
```

`try_acquire` is non-blocking — it yields `False` rather than waiting, so a
replica that loses the race simply skips the work. The Postgres backend needs no
infrastructure beyond the database you already have; the lock name is hashed to a
signed 64-bit key because `pg_advisory_lock` keys are `bigint`.

## ClockProvider

`SystemClock` returns timezone-aware UTC. It exists so time can be injected in
tests without patching `datetime` globally — every module that needs "now" takes
it from the platform.

## GeocodingProvider

| Backend | Notes |
|---|---|
| `NullGeocoding` | Resolves nothing. Selected when geocoding is unconfigured or misconfigured. |
| `HttpGeocoding` | Nominatim, Photon, or geocode.maps.co. Requires the `geocoding` extra. |

Geocoding is the one capability that never fails startup: an unsupported
provider, a missing API key, or a host that fails SSRF validation disables the
capability and logs why, rather than preventing the application from starting.
"Disabled" is an explicit backend so callers never branch on whether the
capability exists.

!!! danger "Outbound hosts are SSRF-checked"
    An operator-configured host is validated before the first request: every
    address it resolves to must be public unicast, and redirects are refused on
    every request so a permitted host cannot 3xx-pivot onto an internal target.
    See [Configuration](configuration.md#networksettings) for the allowlist
    escape hatch.

## Writing your own backend

A backend is any object satisfying the protocol — the provider protocols are
`runtime_checkable`, and nothing inherits from anything:

```python
class MyStorage:
    def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str: ...
    def get(self, area: str, key: str) -> bytes | None: ...
    def exists(self, area: str, key: str) -> bool: ...
    def delete(self, area: str, key: str) -> None: ...
    def list_keys(self, area: str, prefix: str = "") -> list[str]: ...
    def url(self, area: str, key: str, expires_in: int = 3600) -> str: ...
```

Construct the `Platform` yourself to use it, rather than going through
`build_platform`:

```python
from jasil.container import Platform

platform = Platform(
    profile=...,
    state=...,
    storage=MyStorage(),
    events=...,
    lock=...,
    clock=...,
    geocoding=...,
    recorder=None,
)
```
