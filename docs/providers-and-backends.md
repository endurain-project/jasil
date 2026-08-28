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

Object storage addressed by a key within a named *area*. The whole-object API is
kept for small blobs, while the streaming API handles packages and other objects
that must not be materialized in memory.

| Backend | URI | Notes |
|---|---|---|
| `LocalStorage` | `local://`, `local://<path>` | Files under `{data_dir}/{area}/{key}`. |
| `S3Storage` | `s3://<bucket>` | Key prefix per area. Requires the `s3` extra. |

```python
platform.storage.save("avatars", "42.webp", data)
platform.storage.get("avatars", "42.webp")  # bytes | None
platform.storage.url("avatars", "42.webp")
platform.storage.list_keys("avatars", prefix="42-")

with source.open("rb") as stream:
    stored = platform.storage.save_stream(
        "packages",
        "release.zip",
        stream,
        max_bytes=20 * 1024**3,
        content_type="application/zip",
    )

with platform.storage.open_stream("packages", "release.zip", offset=1024, length=4096) as stream:
    chunk = stream.read()
```

An area is a domain-owned namespace. Store only the key in your database — `url`
is computed at serialization time, so migrating local → S3 needs no data
migration.

### Streaming contract

- `save_stream` reads `source` once and never seeks it. Both backends enforce
    `max_bytes` while reading. On a breach they raise `StorageSizeLimitError`, and
    no partial new object is visible: local disk removes its temporary file and S3
    calls `AbortMultipartUpload`. The previous object, if any, stays in place.
- `open_stream` returns a read-once, non-seekable binary stream on **both**
    backends. Always close it, normally with `with`. A missing object raises
    `FileNotFoundError`; it never becomes an empty stream.
- `offset` and `length` select a range before the stream is returned. This is the
    primitive a host uses for HTTP range responses without assuming the stream can
    seek. A zero `length`, or an offset beyond the end, yields an empty stream for
    an object that exists. Negative values raise `ValueError`.

S3 uses multipart upload rather than `upload_fileobj`. This is deliberate: the
latter cannot provide the same mid-stream limit guarantee for an unknown-length,
non-seekable source. There is no local/S3 divergence in `max_bytes` behavior.

### Reconciliation and readiness

`list_keys` remains the convenient materialized, sorted listing for small
namespaces. `iter_objects(area, prefix)` is the reconciliation path: it lazily
yields `(key, modified_epoch)` without collecting the namespace first. The epoch
comes from `st_mtime` locally and S3 `LastModified`; use it as an age guard before
deleting an object that has no database row.

`check_writable()` performs a write-and-delete probe. S3 uses a unique private
probe key. Local storage requires the configured root to exist, then creates a
temporary file inside it; it deliberately does not recreate a missing root, so a
missing volume path is not reported ready. Failures raise
`StorageBackendUnavailableError`.

This probe lives on `StorageProvider`, not on `Platform.health()`. The operation
being promised is specifically "this object store can persist and remove a
blob". A platform-wide readiness result would need host policy about which
capabilities are required and whether degraded events or state should fail the
process. Adding speculative methods to every provider would define none of that.
A host can aggregate `check_writable()` with future capability-specific probes
without changing this contract.

S3 client failures and local filesystem failures surface as
`StorageBackendUnavailableError`, never as botocore or backend-specific
exceptions. `StorageSizeLimitError` is reserved for `max_bytes` breaches.

!!! warning "Traversal is rejected"
    Area and key values are validated before any filesystem access; an absolute
    path or a `..` segment raises `ValueError`. Keys are expected to be
    server-generated, but a stray value must never escape the storage root.

!!! danger "URL controls are honoured by S3 only"
    This is the one place the two backends genuinely differ. `s3://` returns a
    presigned URL that honours `expires_in`, `download_as`, and `content_type`.
    `download_as` pins an `attachment` content disposition, so an untrusted
    object is downloaded rather than rendered inline. `local://` returns a plain
    path for **your** web server to serve and cannot enforce any of those controls
    — JASIL does not run that server and holds no key to sign with.

    So `url(area, key, expires_in=60)` is a one-minute link on S3 and a forever
    link on local disk; `download_as` and `content_type` are likewise host policy
    on local disk. Do not rely on these controls unless you know the deployment
    uses object storage. The local backend logs one warning per instance the
    first time it is given a control it cannot honour.

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

!!! danger "An entry the bus cannot process is stuck, not retried"
    A handler that raises, or an entry the consumer cannot deserialize, is logged
    and left **pending** — unacked, and never redelivered. The consumer reads
    with `XREADGROUP ... >`, which only ever returns entries no one has claimed
    yet, so nothing brings a claimed-but-unacked entry back. Two consequences to
    plan for:

    - the event is **not** processed, and nothing retries it; and
    - the pending-entries list (PEL) grows without bound, while the stream itself
      is capped at roughly 10 000 entries — so a long-pending entry is eventually
      trimmed away underneath its own PEL record.

    This is why [`best_effort`](events-and-outbox.md) swallows handler
    exceptions: on this bus, letting one escape loses the event rather than
    retrying it.

    **Check for it.** The count is the second field of `XPENDING`:

    ```console
    $ redis-cli XPENDING jasil:events jasil
    ```

    A number that only grows is the symptom.

    **Recover from it.** Claim the stuck entries onto a consumer of your own and
    decide per entry — reprocess it, or ack it to drop it:

    ```console
    $ redis-cli XAUTOCLAIM jasil:events jasil recovery 0 0 COUNT 100
    $ redis-cli XACK jasil:events jasil <entry-id>
    ```

    **Avoid it.** If losing an event matters, enable
    [durable jobs](durable-jobs.md). Publishing then routes through the
    transactional outbox and `processing_jobs`, which have the retry, backoff and
    dead-letter path this bus deliberately does not.

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
from collections.abc import Iterator
from typing import BinaryIO


class MyStorage:
    def save(self, area: str, key: str, data: bytes, content_type: str | None = None) -> str: ...
    def save_stream(
        self,
        area: str,
        key: str,
        source: BinaryIO,
        *,
        max_bytes: int | None = None,
        content_type: str | None = None,
    ) -> int: ...
    def get(self, area: str, key: str) -> bytes | None: ...
    def open_stream(
        self,
        area: str,
        key: str,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO: ...
    def exists(self, area: str, key: str) -> bool: ...
    def delete(self, area: str, key: str) -> None: ...
    def list_keys(self, area: str, prefix: str = "") -> list[str]: ...
    def iter_objects(self, area: str, prefix: str = "") -> Iterator[tuple[str, float]]: ...
    def check_writable(self) -> None: ...
    def url(
        self,
        area: str,
        key: str,
        expires_in: int = 3600,
        *,
        download_as: str | None = None,
        content_type: str | None = None,
    ) -> str: ...
```

These members are required. Their addition is an intentional pre-1.0 protocol
break: runtime checks against `StorageProvider` now reject an older custom
backend until it implements them. JASIL chose one complete storage contract over
optional feature checks that would make every caller branch by backend.

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
