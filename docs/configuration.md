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
| `enforce_deployment_consistency` | `True` | Refuse to build a platform whose wiring contradicts its topology. `False` logs the issues as warnings instead. |
| `data_dir` | `"data"` | Root directory for the local storage backend. |
| `state_uri` | `None` | `memory://`, or `redis://` / `rediss://` / `unix://`. |
| `storage_uri` | `None` | `local://` or `s3://`. |
| `events_uri` | `None` | `memory://`, or `redis://` / `rediss://` / `unix://`. |
| `lock_uri` | `None` | `noop://` or `postgres-advisory://`. |
| `jobs` | `JobSettings()` | Durable-job pipeline. |
| `event_log` | `EventLogSettings()` | Observability trail. |
| `geocoding` | `GeocodingSettings()` | Reverse-geocoding backend. |
| `network` | `NetworkSettings()` | Outbound egress. |

### `JobSettings`

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `False` | Route events through the outbox instead of the bus. |
| `lease_seconds` | `300` | How long a claimed job is leased before the reaper may reclaim it. |
| `batch_size` | `20` | Maximum rows claimed or relayed per pass. |
| `backoff_base_seconds` | `60` | First retry delay; doubles per attempt. |
| `backoff_max_seconds` | `3600` | Ceiling for the exponential backoff. |
| `poll_interval_seconds` | `5.0` | Idle wait between empty polls. |
| `max_attempts` | `5` | Attempts before a job is dead-lettered. |
| `retention_days` | `30` | Age at which relayed outbox rows and completed jobs are pruned. `<= 0` disables. |

### `EventLogSettings`

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `False` | Record every event's lifecycle to `event_log`. |
| `retention_days` | `30` | Age at which trail rows are pruned. `<= 0` disables. |

Both `jobs.enabled` and `event_log.enabled` default to **off**, because both
write to the database. A library should not start writing to your database
because you installed it.

### `GeocodingSettings`

| Field | Default | Meaning |
|---|---|---|
| `provider` | `""` | `"nominatim"`, `"photon"`, or `"geocode"`. Anything else disables the capability. |
| `rate_limit` | `1.0` | Maximum requests per second; `<= 0` disables throttling. |
| `api_key` | `None` | Required by geocode.maps.co. |
| `nominatim_host` | `""` | Bare `host[:port]` authority. |
| `nominatim_use_https` | `True` | |
| `photon_host` | `""` | Bare `host[:port]` authority. |
| `photon_use_https` | `True` | |
| `user_agent` | `"jasil (ReverseGeocoding)"` | Nominatim's usage policy requires an identifying value — set your own. |

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

`map_models` must run **before** any JASIL model module is imported — importing
one beforehand raises a `RuntimeError` telling you so.

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
