# Configuration

JASIL never reads environment variables or secret files. The host builds a
`JasilSettings` from whatever source it likes — env vars, a config file, a
secrets manager — and installs it once at startup:

```python
import jasil.settings as jasil_settings

jasil_settings.configure(
    jasil_settings.JasilSettings(
        profile=jasil.DeploymentProfile.DISTRIBUTED,
        state_uri="redis://cache:6379/0",
        events_uri="redis://cache:6379/1",
        storage_uri="s3://my-bucket",
        lock_uri="postgres-advisory://",
    )
)
```

Every component reads the installed settings through `get_settings()`. Settings
are **immutable** — frozen dataclasses — so one component cannot mutate
configuration another has already read.

Calling `configure()` is optional. With nothing installed, `get_settings()`
returns an all-defaults instance, which is a working single-process deployment.

## Shape

Configuration is grouped by concern rather than being one flat list, so enabling
durable jobs means reading one small class instead of scanning forty unrelated
fields.

### Top level — `JasilSettings`

| Field | Default | Meaning |
|---|---|---|
| `profile` | `DeploymentProfile.LOCAL` | The deployment shape. Supplies capability-URI defaults. |
| `web_workers` | `1` | How many web-server worker processes you run. Drives the [consistency checks](deployment-profiles.md#consistency-checks) as much as the profile does. |
| `job_worker_mode` | `JobWorkerMode.IN_PROCESS` | Where durable-job consumers run: `in_process` (each API process calls `start_job_worker()`) or `standalone` (dedicated processes call `run_job_worker()`). A fact about the shape, not a count: **load the same value in every process.** Drives the [consistency checks](deployment-profiles.md#job-workers-are-processes-too); `standalone` requires `jobs.enabled`. Parse an environment value with `jasil.profile.parse_job_worker_mode`. |
| `enforce_deployment_consistency` | `True` | Refuse to build a platform whose wiring contradicts its topology. `False` logs the issues as warnings instead. |
| `data_dir` | `"data"` | Root directory for the local storage backend. |
| `state_uri` | `None` | `memory://`, or `redis://` / `rediss://` / `unix://`. |
| `storage_uri` | `None` | `local://` or `s3://`. |
| `events_uri` | `None` | `memory://`, or `redis://` / `rediss://` / `unix://`. |
| `lock_uri` | `None` | `noop://` or `postgres-advisory://`. |
| `realtime_uri` | `None` | `memory://`, or `redis://` / `rediss://` / `unix://`: the channel carrying [realtime hints](providers-and-backends.md#realtimeprovider) to every process. Unset, it follows the resolved `events_uri` in every profile, so it is `memory://` under the `local` defaults and the event bus's Redis otherwise. `memory://` is refused in a multi-process deployment. |
| `realtime_channel` | `"jasil:realtime"` | The Redis Pub/Sub channel carrying realtime hints. Pub/Sub channels are server-wide (the database number does not separate them), so deployments sharing one Redis server must set different channels. Ignored for `memory://`. |
| `events_stream` | `"jasil:events"` | The Redis Stream key carrying best-effort events. Streams live in one Redis database, so two deployments sharing a database must use different streams. Ignored for `memory://`. |
| `events_group` | `"jasil"` | The Redis consumer group the event bus reads as. Replicas of one deployment share it, so each event is handled once; deployments reading the same stream must not. Ignored for `memory://`. |
| `jobs` | `JobSettings()` | Durable-job pipeline. |
| `event_log` | `EventLogSettings()` | Observability trail. |
| `geocoding` | `GeocodingSettings()` | Reverse-geocoding backend. |
| `storage` | `StorageSettings()` | Options of the `local://` storage backend. |
| `mail` | `MailSettings()` | Outbound mail backend. |
| `network` | `NetworkSettings()` | Outbound egress. |

### `JobSettings`

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `False` | Route events through the outbox instead of the bus. |
| `lease_seconds` | `300` | How long a worker may go **silent** before the reaper may reclaim the jobs it holds. A running worker renews its leases on every heartbeat, so this is not a cap on handler runtime. At least three `heartbeat_interval_seconds`. |
| `max_runtime_seconds` | `1800` | The longest a single handler is kept leased. Past it the job is abandoned to the reaper, the handler is signalled (`jasil.jobs.lease_lost`), and the rest of the batch is released. Not below `lease_seconds`. |
| `batch_size` | `20` | Maximum rows claimed or relayed per pass. |
| `backoff_base_seconds` | `60` | First retry delay; doubles per attempt. |
| `backoff_max_seconds` | `3600` | Ceiling for the exponential backoff. |
| `poll_interval_seconds` | `5.0` | Idle wait between empty polls. |
| `record_attempts` | `True` | Write one `jasil_job_attempts` row per attempt to run a job: worker, timing, outcome, error. `False` skips every attempt write and the attempt-derived admin fields read `None`. See [Job attempts](observability.md#job-attempts). |
| `heartbeat_interval_seconds` | `15.0` | Interval between worker heartbeats. Each beat renews the leases the worker holds and refreshes its registry row. The default stale threshold is three intervals. |
| `max_attempts` | `5` | Attempts before a job is dead-lettered. |
| `retention_days` | `30` | Age at which relayed outbox rows, completed jobs, their attempt history, and stopped/stale worker rows are pruned. A job's history goes after the job row itself is pruned; pending, claimed and dead-lettered jobs keep theirs. `<= 0` disables. |

### `EventLogSettings`

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `False` | Record every event's lifecycle to `jasil_event_log`. |
| `retention_days` | `30` | Age at which trail rows are pruned. `<= 0` disables. |

Both `jobs.enabled` and `event_log.enabled` default to **off**, because both
write to the database. A library should not start writing to your database
because you installed it.

### `GeocodingSettings`

| Field | Default | Meaning |
|---|---|---|
| `uri` | `None` | `nominatim://host[:port]` or `photon://host[:port]` (HTTPS), `nominatim+http://…` or `photon+http://…` (cleartext, for a self-hosted instance on a private network), or `geocode://` (geocode.maps.co, a fixed host). A bare authority only: no userinfo, path, query or fragment. The host is SSRF-checked. Unset disables the capability; so does any value that cannot be used, which is logged. |
| `rate_limit` | `1.0` | Maximum requests per second; `<= 0` disables throttling. |
| `api_key` | `None` | Required by `geocode://`. |
| `user_agent` | `"jasil (ReverseGeocoding)"` | Nominatim's usage policy requires an identifying value — set your own. |

### `StorageSettings`

Settings of the `local://` backend; `s3://` takes none.

| Field | Default | Meaning |
|---|---|---|
| `local_url_prefix` | `""` | URL path prefix of the links `local://` builds, for a host serving the data directory under a path such as `/media`. |
| `local_legacy_layout` | `False` | Also read, list and delete objects in the flat `{data_dir}/{area}/{key}` layout written by JASIL 0.3 and earlier and by the application JASIL was extracted from. Off, that directory is never consulted, so no other file under `data_dir` can be served as an object. |

### `MailSettings`

| Field | Default | Meaning |
|---|---|---|
| `uri` | `None` | `memory://`, `smtp://host[:port]` (STARTTLS, port 587) or `smtps://host[:port]` (implicit TLS, port 465). Unset disables mail: `send` raises `MailNotConfiguredError`. No userinfo, path or query. The host is a DNS name or an IP literal. `memory://` is refused in a multi-process deployment. |
| `secure` | `True` | Verified TLS. `False` allows cleartext `smtp://` and only to a host that resolves exclusively to private, loopback or link-local addresses (re-checked on every send, and the connection is pinned to the checked addresses, tried in order; the shared address space `100.64.0.0/10` used by carrier-grade NAT and Tailscale is not private for this purpose), with no `username` / `password`; anything else, including `memory://`, fails `build_platform`. |
| `from_address` | `""` | `"addr"` or `"Name <addr>"`. Required for the SMTP schemes. Independent of `username`. |
| `username` | `None` | SMTP login. Must be set together with `password`. |
| `password` | `None` | SMTP password. Hidden from `repr`. |
| `ehlo_hostname` | `None` | Name announced in `EHLO`. Defaults to the machine's hostname; set a fully qualified one for a relay that rejects a bare name. |

Host names follow RFC 1123, so a name with an underscore (some Docker Compose
service names) is refused for `uri` and `ehlo_hostname`; use an IP literal or a
network alias without one.

A bad value fails `build_platform` with `ValueError`; no connection is attempted
at startup, so a mail server that is down does not block boot. With
`secure = False` the host is resolved at startup to confirm it is private, so
that one case does need a working DNS resolver to boot.

### `NetworkSettings`

| Field | Default | Meaning |
|---|---|---|
| `ssrf_allowed_hosts` | `()` | Hostnames and CIDRs exempt from the SSRF address denylist. |

## Capability URIs

Each capability resolves its backend by URI scheme, independently of the profile:

| Capability | Schemes |
|---|---|
| `state_uri` | `memory://`, `redis://`, `rediss://`, `unix://` |
| `storage_uri` | `local://`, `local://<path>`, `s3://<bucket>` |
| `events_uri` | `memory://`, `redis://`, `rediss://`, `unix://` |
| `lock_uri` | `noop://`, `postgres-advisory://` |

An unrecognised scheme raises `ValueError` at startup. Failing to start beats
silently running on the wrong backend.

### Profile defaults

Leaving a URI unset falls back to the profile's default:

| Profile | `state` | `storage` | `events` | `lock` |
|---|---|---|---|---|
| `local` | `memory://` | `local://` | `memory://` | `noop://` |
| `distributed` | **required** | **required** | **required** | **required** |
| `custom` | **required** | **required** | **required** | **required** |

!!! danger "Non-local profiles refuse to guess"
    A Redis host or bucket name cannot be inferred, and defaulting to a
    process-local backend across replicas would mean each one silently keeping
    its own copy of state that is supposed to be shared. So an unset URI under
    `distributed` or `custom` raises `ValueError` at startup rather than
    starting a deployment that looks healthy and is not.

## Runtime sources

Everything above is read once, at startup. Mail and geocoding can instead follow
settings a host's operators edit while it runs (stored in the host's database,
say), without a restart. The host supplies a callable that returns the current
settings; JASIL decides when to rebuild the backend, caches it, reports it, and
defines what happens when something fails. JASIL learns nothing about where the
settings are stored, how they are encrypted or who edits them.

```python
from jasil.settings import RuntimeSources

platform = jasil_container.build_platform(settings, sources=RuntimeSources(mail=load_mail))
```

`load_mail` and `load_geocoding` are plain callables returning a `MailSettings` /
`GeocodingSettings`. A capability with a source ignores `JasilSettings.mail` /
`.geocoding`; a capability without one is built once from them, exactly as before.

### The contract

- **Called on every use** of the provider: every `send`, every `reverse`. There is
  no refresh interval, so the host owns making the call cheap. A process-local
  cache with a short expiry is the usual shape; a version stamp the host bumps on
  save and checks with one cheap read works too. Each process must notice an
  edit on its own: JASIL has no push invalidation.
- **Not called by `build_platform`.** The host may not be able to read its store
  yet (migrations have not run). The first call is the first `send` / `reverse`.
- **May raise.** The failure table below says what happens.
- Called outside JASIL's lock, from whichever thread is sending.

JASIL compares what the source returns with what the cached backend was built
from (including `password` and `api_key`, which are hidden from `repr` but still
compared), and rebuilds only on a difference. The result of a build, backend or
rejection, is cached, so a bad configuration is logged once per change and not on
every call. The same bad value arriving again after a different one is logged
again.

### Failure behaviour

| Situation | Mail | Geocoding |
|---|---|---|
| The source raises, and the last build produced a working backend | That backend is used, **including its credentials, for as long as the source keeps failing**: revoking them in a store that is down does not take effect until it answers. A warning logs on the first failure of a run, debug after, until the source works again. | Same. |
| The source raises, and there is no working backend (never loaded, or the last settings were rejected) | `send` raises `MailError("Mail settings could not be loaded")`, retryable, the original exception chained. | `reverse` returns `None`. |
| The settings are invalid | `send` raises `MailNotConfiguredError` whose message carries the reason (never a setting's value), also logged once. The previous backend and its credentials are **never** reused. | `NullGeocoding`; the reason is logged once, as at startup. |
| The relay or geocoding host does not resolve (mail `secure=False`, geocoding) | `send` raises a plain `MailError`, retryable. The build is retried after 30 seconds, not remembered as a rejection. | `reverse` returns `None`; the build is retried after 30 seconds. |
| The settings are unset (`uri` empty) | As `NullMail`: `send` raises `MailNotConfiguredError`. | As `NullGeocoding`. |

Both mail errors have no SMTP status, so a durable job retries either one.

### Consistency on every rebuild

The startup deployment-consistency check ignores `settings.mail` when mail has a
source, since that value is not what will be used. The same rule runs on every
rebuild instead: a `memory://` URI arriving from the source in a multi-process
deployment is rejected (`MailNotConfiguredError`) when
`enforce_deployment_consistency` is on, and built with a warning when it is off.
A runtime edit cannot bypass the rule.

### Validating on save

`jasil.settings.mail_problems(mail_settings)` lists every problem with a
`MailSettings` without building a backend, one `MailProblem(code, field, message)`
each, so an admin form can reject a bad edit instead of storing it. It is the same
set of rules the builder applies. With `secure=False` on `smtp://` it resolves the
host to confirm it is private, so it needs a working resolver. The messages never
echo a value. The one rule it leaves out is the multi-process `memory://` one,
which needs the topology.

### Good to know

- A geocoding rebuild creates a new `HttpGeocoding`, which resets its throttle: at
  most one extra immediate request after an edit.
- A `memory://` mail source gets a fresh, empty outbox on every rebuild.
- A rebuild happens under a lock, once per change, so concurrent senders wait for
  it. For `secure=False` that includes one DNS lookup.
- Nothing is held open between uses, so `Platform.close()` has nothing extra to release.

### What is not reloadable

State, storage, events, the lock and realtime describe the deployment's shape:
which processes exist and what they share. Changing one under a running process
would silently split it from its peers, so they stay static and a change needs a
deploy. Mail and geocoding are leaves: nothing depends on their identity.

## Host integration

Three seams the host wires once at startup, in this order:

```python
import jasil.correlation as correlation
import jasil.orm as jasil_orm
import jasil.settings as jasil_settings

# 1. The ORM: you own the base and the engine.
jasil_orm.map_models(Base)
jasil_orm.configure_sessionmaker(sessionmaker(bind=engine))

# 2. Settings.
jasil_settings.configure(jasil_settings.JasilSettings(...))

# 3. Optional: where the correlation id comes from.
correlation.configure_provider(my_middleware.get_request_id)
```

`map_models` must run **before any JASIL model module is imported** — importing
one beforehand raises a `RuntimeError` telling you so.

In practice that constrains two modules only:

| Module | Safe to import at module scope? |
|---|---|
| `jasil.jobs.crud`, `jasil.event_log.crud` | **No** — import them inside a function, or after `map_models` has run. |
| everything else, including `jasil.publisher`, `jasil.retention`, `jasil.jobs.service`, `jasil.container`, `jasil.deps`, `jasil.testing` | Yes, from anywhere. |

That split matters because `jasil.publisher` is imported by every module that
publishes an event, deep in your import graph and long before any startup hook
runs. It defers its model imports so `from jasil.publisher import publish` at the
top of a domain module always works.

### Databases

JASIL runs its whole suite against **SQLite**, **PostgreSQL** and **MySQL** on
every change, so all three are supported rather than merely expected to work.
JASIL never creates the engine — whichever you hand it through
`configure_sessionmaker` is the one it uses.

What differs between them is handled internally, but two consequences are worth
knowing:

| | Consequence |
|---|---|
| **`FOR UPDATE SKIP LOCKED`** | Available on PostgreSQL, MySQL 8.0.1+ and MariaDB 10.6+. Where it is missing, concurrent workers and relayers fall back to a compare-and-set claim plus the idempotent fan-out, which is correct but does more redundant work. |
| **Timestamp precision** | MySQL's `DATETIME` stores whole seconds and *rounds*, so a persisted timestamp can differ from the one written by up to half a second, in either direction. Lease expiry, backoff and retention are all interval comparisons measured in seconds or days, so this does not affect them — but do not compare a timestamp JASIL persisted for equality with the value you passed in. |

### Logging

There is nothing to configure. JASIL uses `logging.getLogger(__name__)`
throughout, so records propagate to whatever handlers you have already
configured. Structured fields arrive via `extra`.

### Correlation ids

With no provider installed, `jasil.correlation` uses a module-local context
variable you can set yourself:

```python
correlation.set_correlation_id(request_id)
```

A provider that raises is treated as "no id" rather than propagating — a
correlation id is diagnostic metadata and must never break publishing.
