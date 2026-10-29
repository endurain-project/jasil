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
- **`record_tiered_failure(counter_key, gate_key, tiers, counter_ttl_seconds)`**
  — an atomic escalating gate over any repeated event (failed attempts, abusive
  requests, repeated errors; what they are is yours). Each call counts one
  occurrence, and when the count reaches a tier's threshold the gate closes for
  that tier's seconds. Under a lock in memory, as a Lua script on Redis. While
  the gate is closed a call gets the count back *without* incrementing, so
  hammering cannot push the count into the next tier and extend its own penalty.

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

`StorageProvider` is the complete capability installed on `Platform.storage`,
so every URI-selected backend implements the same behavior. Domain services and
small test doubles can depend on only the slice they use:

| Protocol | Responsibility |
|---|---|
| `StorageObjects` | Whole-object persistence, metadata, deletion, and enumeration. |
| `StorageStreams` | Bounded-memory reads and writes. |
| `StorageDelivery` | Framework-neutral serving plans and URLs. |
| `StorageManagement` | Copying, subtree deletion, and readiness. |
| `ResumableUploads` | Durable multipart upload sessions and cleanup. |

| Backend | URI | Notes |
|---|---|---|
| `LocalStorage` | `local://`, `local://<path>` | Logical objects in a private versioned filesystem layout. |
| `S3Storage` | `s3://<bucket>` | Key prefix per area. Requires the `s3` extra. |

```python
platform.storage.save("avatars", "42.webp", data)
platform.storage.get("avatars", "42.webp")  # bytes | None
platform.storage.url("avatars", "42.webp")
platform.storage.list_keys("avatars", prefix="42-")
platform.storage.stat("avatars", "42.webp")
platform.storage.copy("avatars", "42.webp", "archive", "42.webp")
platform.storage.delete_prefix("packages", "release/42")

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

session = platform.storage.begin_upload(
    "packages",
    "release.zip",
    max_bytes=20 * 1024**3,
    content_type="application/zip",
)
parts = [
    platform.storage.upload_part(session, 1, first_stream, size=first_size),
    platform.storage.upload_part(session, 2, final_stream, size=final_size),
]
stored = platform.storage.complete_upload(session, parts)
```

An area is one domain-owned namespace component; hierarchy belongs in the key.
This keeps `(area, key)` boundaries unambiguous on every backend. Store only the
key in your database. `url` is computed at serialization time, so migrating
local to S3 needs no data migration.

The logical namespace permits both an object and its descendants, such as
`release/1` and `release/1/archive.bin`. S3 supports that directly. Local
storage maps each logical object to a leaf file under
`.jasil-objects/v1` using a fixed-depth SHA-256 address plus verified area and
key sidecars. This avoids file/directory, path-length, case-folding,
Unicode-normalization, and private-marker collisions; a digest collision or
tampered sidecar is rejected rather than aliased. The physical path is private
and may change in a future major layout version; use `serve()` or `url()` rather
than constructing it.

For local installs that need ordinary files, opt selected areas into the plain
layout with `StorageSettings(local_plain_areas=frozenset({"activity_files"}))`.
Those objects live at `{data_dir}/{area}/{key}`; all other areas continue to use
the private layout. The setting is ignored by S3. In a plain area, a key and one
of its descendants (for example, `a` and `a/b`) cannot both be stored because a
filesystem path cannot be both a file and a directory. The conflicting write
raises `StorageKeyConflictError`. This is the one difference in key/descendant
namespace semantics between a plain local area and S3; non-conflicting keys keep
the same provider API and behavior.

Plain-area reads and listings also find objects still in the private hashed
layout. A plain copy takes precedence when both exist; `serve()` and `url()` use
the existing copy until an overwrite writes to the plain path, then the hashed
copy is removed. `delete()` removes both copies. Plain writes use atomic
replacement and stage under the private upload namespace when it is on the same
filesystem; internal temporary filenames are reserved and excluded from listings.
Plain writes do not create identity sidecars. Because the area is explicitly
opted in, regular files placed under its directory are included in storage
listings. Path traversal and symlinks remain rejected.

While hashed-layout objects remain in a plain area, each write scans that opaque
area to detect descendant-key conflicts; hashed paths cannot be queried by key
prefix. Write latency therefore grows with the number of unmigrated objects.
Overwrite those objects into the plain layout before relying on write latency in
a large area.

Unlike the private layout, plain paths inherit the host filesystem's path-length,
case-sensitivity, and Unicode-normalization behavior; choose keys that remain
distinct and addressable on the target filesystem.

Objects in the flat `{data_dir}/{area}/{key}` layout, written by JASIL 0.3 and
earlier and by the application JASIL was extracted from, are **not** consulted by
default outside explicitly selected plain areas, so no other file under
`data_dir` can be served as an object. Set
`storage.local_legacy_layout=True` ([Configuration](configuration.md#storagesettings))
to read, list, serve, and delete them transparently in other areas. Rewriting a
legacy object there moves that logical key into the versioned layout; in a plain
area, the same flat path is already the active layout. Untouched legacy files
remain valid and can be removed through normal `delete` or `delete_prefix`
operations.

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

### Resumable upload contract

`begin_upload` creates durable staging state and returns an `UploadSession` with
an opaque `session_id`. Persist that value as trusted server-side state when an
upload spans requests.
Do not let an untrusted client construct or alter its target, total limit, or
part constraints. A session can be resumed by another process configured with
the same local root or S3 bucket and prefix. It is not portable across backend
changes or different storage configurations.

The returned session reports JASIL's built-in portability limits. They use the
common denominator supported by local and S3-compatible storage so application
code never branches by backend:

- Part numbers are integers from 1 through 10,000. Parts may be uploaded out of
    order or concurrently, and uploading the same number again atomically
    replaces that part. Wait for every part call to finish before completion or
    cleanup.
- A part is at most 5 GiB. At completion, every part except the final one must
    be at least 5 MiB. The final part may be smaller or empty.
- `upload_part(session, part_number, source, size=...)` reads a non-seekable
    binary source once. `size` is required and must exactly match the source: a
    short or longer source raises `ValueError` and leaves the session available
    for a replacement upload. The caller owns and closes the source. A failed
    attempt may have consumed it, so retry with a newly opened source and the
    same session and part number. Local storage writes the part straight to its
    staging file. S3 stages it first, in memory up to 8 MiB and in a temporary
    file (under `TMPDIR`) beyond that, so the client can checksum it and resend
    it after a transient failure: allow temporary disk for the largest part in
    flight.
- `PartRef` records the part number, actual byte size, and an opaque
    `validator`. The validator may internally be a digest, an S3 ETag, or
    another backend token; callers compare or persist it but never interpret
    it. Pass the current reference for every uploaded part to
    `complete_upload`, exactly once and in ascending part-number order. A
    replaced part invalidates its old reference.
- `max_bytes` is checked against the currently visible staged parts before each
    part upload and authoritatively against the complete part set. Parallel calls
    can temporarily stage more than the limit when they race on the same observed
    set, but completion refuses the aggregate. A breach raises
    `StorageSizeLimitError` without changing the destination object.

An active upload is not an object: `exists`, `stat`, `list_keys`, and
`iter_objects` do not expose it. `complete_upload` validates every reference,
assembles the object atomically, returns its byte count, and consumes the
session. The previous destination remains visible until that commit. Local
storage uses durable manifests and atomic part files under the reserved
`.jasil-upload-sessions` directory. S3 stores private session manifests under
the same reserved namespace and maps their opaque IDs to native multipart upload
IDs. Both backends reserve that area name so switching backends cannot expose
session state as ordinary objects. `.jasil-objects` is also reserved for the
local physical layout.

`upload_part` and `complete_upload` raise `StorageUploadSessionError` for an
unknown, cleaned, completed, mismatched, or foreign session and for a stale part
reference. `abort_upload` is idempotent, including after completion or cleanup.
`complete_upload` is terminal but not retry-idempotent: if its response is lost,
inspect the destination with `stat` before deciding how the host should recover.

Sessions do not expire automatically. Call
`cleanup_uploads(older_than_epoch=cutoff)` from host-owned maintenance to abort
uploads initiated before an epoch cutoff; it returns the number removed. The
cutoff is based on initiation time, not the last uploaded part, so choose a
window longer than any valid upload. Cleanup attempts every eligible session
before reporting a provider-neutral backend failure; retry the pass after the
backend recovers. S3 cleanup covers every multipart upload
registered by a JASIL session manifest and leaves `save_stream`, managed copy,
and independently initiated multipart work untouched. If the process dies in
the narrow interval after S3 creates a native multipart upload but before its
manifest is stored, only the bucket's incomplete-multipart lifecycle policy can
remove that unregistered upload. Configure that policy as a final safety net.

### Serving plans and metadata

`serve(area, key, ...)` tells a host how the selected backend can serve an
object efficiently without importing Starlette, FastAPI, or another web
framework:

- `ServeFile(path)` is returned by local storage. The absolute path can be
    passed to a framework file response, used with `sendfile`, or mapped to an
    internal `X-Accel-Redirect` or `X-Sendfile` target. It remains the object's
    local address while that object exists, but it is not a snapshot: deleting or
    replacing the object after plan creation changes what a later open sees. Do
    not expose the raw filesystem path as a public URL.
- `ServeRedirect(url)` is returned by S3. The URL is presigned with the requested
    expiry, download disposition, and response content type, so object bytes do
    not pass through the application process.
- `ServeStream(stream)` is the complete-contract fallback for a custom backend
    that can do neither. The host owns and must close the read-once stream.

Both built-in backends verify existence before returning a plan, so a missing
object raises `FileNotFoundError`. S3 performs a `HeadObject` before signing its
redirect. As with any path or URL handed to another component, deletion can
still race with the eventual response.

!!! danger "`ServeFile` controls belong to the host"
        Local storage cannot enforce `expires_in`, `download_as`, or `content_type`.
        Apply the filename and media type when constructing the framework response,
        and enforce authorization before serving the path. `serve()` and `url()`
        share the same one-warning-per-backend behavior when a caller requests a
        control that local storage cannot honour.

`stat(area, key)` returns `ObjectStat(size, modified_epoch, content_type, etag)`
without reading object content, or `None` when the object is absent. Size and
modification time are available on both backends and agree with `save_stream`
and `iter_objects`. S3 also returns its stored content type and ETag. Portable
local filesystems expose neither, so both fields are `None` locally even when
`save` received a content type. JASIL does not create hidden metadata sidecars
or depend on non-portable filesystem extended attributes. Treat an ETag as an
opaque backend validator, not necessarily a content digest.

### Object management

`copy(src_area, src_key, dst_area, dst_key)` overwrites the destination while
leaving the source intact. Local storage copies through the same atomic
temporary-file path as `save_stream`. S3 uses boto3's managed server-side copy,
which automatically switches to multipart copy for large objects; bytes never
round-trip through the application process.

`delete_prefix(area, prefix)` requires a non-empty key root and returns the
number of objects removed. Its matching is deliberately hierarchical rather
than lexical: it removes the exact key and slash-delimited descendants, so
`delete_prefix("packages", "pkg/1")` never removes `pkg/10` or `pkg/1-old`.
S3 deletes in batches of at most 1,000 objects. Local storage removes the
matching subtree, and both `delete` and `delete_prefix` prune directories they
leave empty without removing the configured storage root.

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

!!! warning "Traversal and aliases are rejected"
    Area, key, source, destination, and deletion-prefix values are validated
    before backend I/O. Addresses must be canonical slash-delimited paths: an
    absolute path, a `.` or `..` component, a repeated or trailing separator,
    or any backslash raises `ValueError`. Dots inside a component, such as
    `.metadata.json`, remain valid. Keys are expected to be server-generated,
    but a stray value must never escape or alias another address. Local storage
    also rejects object addresses that traverse symbolic links and omits symlink
    aliases from listings, preserving area and subtree boundaries if the storage
    tree is modified outside JASIL.

!!! danger "URL controls are honoured by S3 only"
    In addition to the plain-area key/descendant limitation above, this is a
    delivery-contract difference between the backends. `s3://` returns a
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
    transactional outbox and `jasil_processing_jobs`, which have the retry, backoff and
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
| `HttpGeocoding` | Nominatim, Photon, or geocode.maps.co, chosen by the `geocoding.uri` scheme. Requires the `geocoding` extra. |

`reverse(latitude, longitude)` returns a `GeocodedPlace` or `None`. Its fields are
administrative levels, filled the same way whichever service answered:
`locality` (the city, town, village or hamlet), `sublocality` (district, suburb),
`region` (state or province), `country`, `country_code` (ISO 3166-1 alpha-2) and
`display_name` (where the service supplies one). Map them onto your own
vocabulary in your own code.

Geocoding is the one capability that never fails startup: an unsupported
scheme, a missing API key, or a host that fails SSRF validation disables the
capability and logs why, rather than preventing the application from starting.
"Disabled" is an explicit backend so callers never branch on whether the
capability exists.

!!! danger "Outbound hosts are SSRF-checked"
    An operator-configured host is validated before the first request: every
    address it resolves to must be public unicast, and redirects are refused on
    every request so a permitted host cannot 3xx-pivot onto an internal target.
    See [Configuration](configuration.md#networksettings) for the allowlist
    escape hatch.

Geocoding settings can also come from a runtime source: see
[Runtime sources](configuration.md#runtime-sources). `Platform.geocoding` is then a
reloading provider that rebuilds when the settings change; it satisfies the same
protocol, and returns `None` when the source fails and nothing worked before.

## MailProvider

```python
platform.mail.send(MailMessage(to=["a@example.com"], subject="Hi", text="Hello"))
```

| Backend | Selected by | Notes |
|---|---|---|
| `NullMail` | `mail.uri` unset | `send` raises `MailNotConfiguredError`. |
| `MemoryMail` | `memory://` | Records each `MailMessage` in a public `outbox` list. For tests; refused in a multi-process deployment. |
| `SmtpMail` | `smtp://host[:port]` / `smtps://host[:port]` | STARTTLS (default port 587) or implicit TLS (465). Standard library only — no extra. |

`send` returns the `Message-ID` header of the message it built, angle brackets
included. It raises `MailError` on any delivery failure — mail is not
best-effort, so a raised error is what makes a durable job retry — and
`ValueError` for an invalid message (no recipient, no body, a malformed address,
CR/LF in a header value). `MemoryMail` validates through the same builder as
`SmtpMail`, so a test fails on the message production would reject.

!!! danger "TLS is verified unless you opt out, narrowly"
    `smtp://` refuses a server that does not offer STARTTLS rather than falling
    back, certificates and hostnames are verified, and AUTH is sent only after
    TLS is up. `mail.secure = False` sends in cleartext, and is accepted only
    for `smtp://` to a host resolving exclusively to private, loopback or
    link-local addresses, and never with credentials. A bad configuration fails
    `build_platform`; no connection is made at startup.

`MailError` messages carry a failure category and SMTP status code only — never
an address, subject, body or server text.

The sender is `from_address`, independent of the SMTP `username`: relays such as
Brevo log you in as a generated identity and reject a `From` that is not a
verified sender. The `From` header keeps any display name; the envelope sender is
the bare address. If a server refuses *some* recipients the send succeeds and a
warning logs the refused count, because failing would make a retry re-send to
recipients that already received it. Every recipient of a message is visible in
`To`, and a refused one is only counted in that warning — send one message per
recipient when either matters.

### Runtime-configured mail

`build_platform(settings, sources=RuntimeSources(mail=load_mail))` makes
`Platform.mail` follow the settings `load_mail()` returns, without a restart. The
failure behaviour is in [Runtime sources](configuration.md#runtime-sources).
`mail_problems(mail_settings)` validates a `MailSettings` without building a
backend and returns a list of `MailProblem(code, field, message)`; `code` is a
`MailProblemCode`:

`invalid_uri`, `unsupported_scheme`, `uri_carries_credentials`,
`uri_has_extra_parts`, `invalid_host`, `invalid_port`, `missing_from_address`,
`invalid_from_address`, `invalid_ehlo_hostname`, `credentials_incomplete`,
`credentials_not_ascii`, `memory_has_host`, `memory_cleartext_unsupported`,
`cleartext_with_implicit_tls`, `cleartext_with_credentials`,
`cleartext_public_host`, `cleartext_host_unresolvable` (the lookup failed; a build retries it).

## RealtimeProvider

Carries a small "something changed, refetch" hint from whichever process
noticed the change to **every** process that may hold the live connection it is
for. The host keeps the WebSocket or SSE endpoint, its auth and its connection
registry; JASIL only moves the hint between processes.

The `key` names the hint's audience and is an opaque string: a user id, a device
id, an organisation, a cache name. Its meaning belongs entirely to the host, and
JASIL does no routing or filtering on it. It must be a non-empty `str` of at most
256 characters (`MAX_REALTIME_KEY_LENGTH`; code points, not bytes) with no unpaired Unicode surrogate. The
key appears in the warning logged when a hint is dropped, so do not put a secret
(a token, a session id) in it.

```python
# At startup, in every process that holds client connections:
platform.realtime.subscribe(lambda key, hint: connections.push(key, hint))
platform.realtime.start()

# Anywhere — a route, a durable-job handler, a standalone worker that never started it:
platform.realtime.notify(str(account.id), {"kind": "order.updated", "id": order.id})
```

| Backend | URI | Notes |
|---|---|---|
| `InProcessRealtime` | `memory://` | `notify` calls the handlers inline on the caller's thread. `start` / `stop` are no-ops. |
| `RedisRealtime` | `redis://`, `rediss://`, `unix://` | Redis Pub/Sub on one channel (`realtime_channel`, default `jasil:realtime`; server-wide, so unique per deployment). Every started process receives every hint. |

`realtime_uri` follows `events_uri` when unset, so an existing deployment needs
no new setting.

!!! warning "A hint is a latency optimisation, never the record"
    - Delivery is **best-effort, at-most-once and unordered**. A replica that is
      reconnecting to Redis misses what was published meanwhile, and nothing
      replays it. The client must treat a hint as "go and look", and must stay
      correct — through polling or a refetch on reconnect — if it never arrives.
    - **Transport failures never raise** out of `notify`: Redis being down
      is logged and the hint dropped, so a request or job never fails over a push.
    - **Programmer errors do raise** `ValueError`: a `key` that is not a
      `str`, an empty `key`, a `key` over 256 characters, a `key` or `hint`
      holding an unpaired Unicode surrogate, a `hint` that is not a
      JSON-serialisable `dict`, or an envelope over 4096 bytes. Both backends
      validate identically, so the bug shows up in a local test. A received
      hint that breaks these rules is logged and dropped, never delivered.
    - **Every started process receives every hint**, its own included. Filter by
      your connection registry; there is no per-key routing, topic, ack or
      history.

Handlers run on the provider's subscriber thread (`InProcessRealtime`: on the
publisher's thread). Keep them short — on an async server, hand the hint to the
event loop with `loop.call_soon_threadsafe` — and note that a handler that
raises is logged and skipped, never retried. A hint arrives as JSON decodes it:
tuples become lists and keys become strings, in both backends.

The Redis subscriber holds a **dedicated** connection, not one from the shared
pool, reconnects with capped exponential backoff and jitter, and PINGs an idle
subscription so a silently dead connection is replaced within about a minute.
Pub/Sub channels are **server-wide**: the database number in the URI does not
separate them, so two deployments sharing one Redis server must not share the
channel name. `Platform.close()` (and `jasil.lifecycle.shutdown()`) stops the
subscriber.

In a test, `jasil.testing.capture_hints(platform)` returns a list that collects
every hint published afterwards as `(key, hint)`.

## Writing your own backend

A backend is any object satisfying the protocol. The provider protocols are
`runtime_checkable`; no concrete backend has to inherit from them. Depend on a
narrow storage protocol when a service uses only one slice:

```python
from typing import BinaryIO

from jasil.providers import StorageStreams


def persist_export(storage: StorageStreams, source: BinaryIO) -> int:
    return storage.save_stream("exports", "latest.bin", source)
```

The complete custom backend shape used by `Platform.storage` is:

```python
from collections.abc import Iterator, Sequence
from typing import BinaryIO

from jasil.providers import ObjectStat, PartRef, ServePlan, UploadSession


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
    def begin_upload(
        self,
        area: str,
        key: str,
        *,
        max_bytes: int | None = None,
        content_type: str | None = None,
    ) -> UploadSession: ...
    def upload_part(
        self,
        session: UploadSession,
        part_number: int,
        source: BinaryIO,
        *,
        size: int,
    ) -> PartRef: ...
    def complete_upload(self, session: UploadSession, parts: Sequence[PartRef]) -> int: ...
    def abort_upload(self, session: UploadSession) -> None: ...
    def cleanup_uploads(self, *, older_than_epoch: float) -> int: ...
    def get(self, area: str, key: str) -> bytes | None: ...
    def open_stream(
        self,
        area: str,
        key: str,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO: ...
    def stat(self, area: str, key: str) -> ObjectStat | None: ...
    def serve(
        self,
        area: str,
        key: str,
        *,
        download_as: str | None = None,
        content_type: str | None = None,
        expires_in: int = 3600,
    ) -> ServePlan: ...
    def exists(self, area: str, key: str) -> bool: ...
    def delete(self, area: str, key: str) -> None: ...
    def delete_prefix(self, area: str, prefix: str) -> int: ...
    def copy(self, src_area: str, src_key: str, dst_area: str, dst_key: str) -> None: ...
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

Every member shown is required only for the complete `StorageProvider`
aggregate. Each narrow protocol contains the corresponding subset. Protocol
expansions are intentional pre-1.0 breaks: runtime checks against
`StorageProvider` reject an older custom backend until it implements the complete
contract. JASIL chooses one complete platform capability over optional feature
checks that would make callers branch by backend.

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
    mail=...,
    realtime=...,  # optional; defaults to InProcessRealtime()
    recorder=None,
)
```

## AuditSink

A write-once copy of the [audit checkpoints](audit.md#the-external-sink), kept outside the audited database.
`Platform.audit_sink` is `None` unless `audit.external_sink_uri` is set.

| Backend | URI | Notes |
|---|---|---|
| `FileAuditSink` | `file:///absolute/directory` | Exclusive-create, read-only files. |
| `StorageAuditSink` | `s3://bucket/prefix` | Over the S3 storage backend; use a separate bucket with Object Lock and a default retention period. |

`write_once(name, content)` never replaces an object: the same bytes again are a no-op, different bytes raise
`AuditSinkConflictError`.
