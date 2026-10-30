# Deployment profiles

A profile describes the *shape* of a deployment. It does two things: it supplies
the default for each capability URI, and it decides which wiring mistakes are
fatal at startup.

| Profile | Shape | Capability defaults |
|---|---|---|
| `local` | One process, one node | memory state, local disk, in-process events, no-op lock |
| `distributed` | Many replicas, many nodes | none — every URI must be set |
| `custom` | You decide | none — every URI must be set |

The profile does **not** change how the platform is built. Every capability
resolves by URI scheme regardless; the profile only supplies the defaults those
URIs fall back to.

## local

The default, and the one that needs no infrastructure:

```python
jasil_settings.configure(jasil_settings.JasilSettings())
```

That is a complete, working configuration: memory state, blobs under `data/`,
synchronous in-process events, and a lock that always acquires. Nothing to
install, nothing to run alongside.

Valid for a single process. A multi-worker `local` deployment is a different
matter — see below.

With durable jobs enabled, the complete local shape is SQLite, one API process,
and `start_job_worker()` (`job_worker_mode` stays `in_process`, its default). That
one background consumer drains every named queue
serially and rotates queues fairly between batches for the lifetime of the
process. It needs neither PostgreSQL nor Redis.

## distributed

Many replicas on separate nodes. Every capability must point at something they
can share:

```python
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

!!! danger "It refuses to guess"
    Leaving a URI unset raises `ValueError` at startup:

    ```
    state_uri must be set explicitly for the 'distributed' deployment profile;
    only the 'local' profile has a default (memory://).
    ```

    This is the whole point of the profile. Falling back to `memory://` would
    give every replica its own private copy of state that is supposed to be
    shared — a rate limiter that permits N times the configured rate, an escalating
    gate that never closes, a session that exists on one replica and not another.
    None of that fails loudly. A refusal to start does.

Durable jobs have a separate database topology from the capability URIs above.
In a distributed deployment, use PostgreSQL for the outbox, jobs, and worker
registry. API replicas schedule relay/reaping but do not call
`start_job_worker()`; standalone processes call `run_job_worker(queues=...)`
and the deployment declares that with
[`job_worker_mode`](#job-workers-are-processes-too). Queue-specific process
counts provide independent concurrency, while several processes selecting the
same queue compete through `SKIP LOCKED`. Redis may back state or the event bus,
but durable jobs themselves do not use it.

## custom

No defaults at all; every capability is explicit. For deployments that do not
match either shape — say many replicas that genuinely want local disk because
each writes only its own scratch data.

## Consistency checks

Beyond the URI defaults, `build_platform()` validates the *combination* before it
constructs a single backend, and refuses inconsistent wiring. Every rule asks the
same question of the deployment's shape: **can two processes reach this
capability, or two machines?** A deployment is *multi-process* when any of these
holds, and *multi-node* only for the first:

| Input | Multi-process | Multi-node |
|---|---|---|
| `profile=distributed` | yes | yes |
| `web_workers > 1` | yes | |
| `job_worker_mode=standalone` | yes | |

**Cross-process state.** Ephemeral state, the event bus and the realtime hint
channel must not resolve to process-local memory when the deployment is
multi-process. The stores would diverge silently across processes, and a hint
published by one process (another replica, a standalone job worker) would never
reach the process holding the connection it is for. An unset `realtime_uri` follows
`events_uri`, so it is judged (and named in the refusal) as the event bus.

**Cross-node storage.** Blob storage must not resolve to the local filesystem
when the deployment is multi-node, where replicas run on separate nodes with no
shared disk. Several processes of a `local` deployment share one host disk, so
local storage stays valid there.

**Audit sink.** `audit.external_sink_uri` must not be a `file://` directory when
the deployment is multi-node. Audit maintenance runs on whichever node takes the
lock, so each day's checkpoint copy would land on a different node's disk. Use an
`s3://` sink there. The rule applies only while auditing is enabled.

**Coordination lock.** The lock must not resolve to the in-process `noop` lock
when more than one process could run the same scheduled job.

**Mail.** `mail.uri` must not be `memory://` when the deployment is
multi-process: each process would record messages in its own memory and none
would ever be delivered. Leaving mail unset is not flagged, because every send
then raises.

**Job workers.** `job_worker_mode=standalone` requires `jobs.enabled`: standalone
workers consume durable jobs, so declaring them while the pipeline is dormant is
a contradiction, not a shape.

What each shape requires:

| Capability | Local, one process | Local + standalone workers | Distributed |
|---|---|---|---|
| `job_worker_mode` | `in_process` | `standalone` | `standalone`, or `in_process` per replica |
| State, events, realtime | memory | Redis | Redis |
| Lock | `noop` or `postgres-advisory` | `postgres-advisory` | `postgres-advisory` |
| Mail | `memory`, or SMTP | SMTP | SMTP |
| Storage | `local` | `local` on a shared volume, or S3 | S3 |
| Audit sink, if any | `file` or S3 | `file` or S3 | S3 |
| Jobs database | SQLite or a server database | PostgreSQL, MySQL 8.0.1+ or MariaDB 10.6+ | the same |

The middle column needs Redis whenever state or events are used. That is not a
choice JASIL makes: two processes cannot share memory.

Note that **the shape matters as much as the profile**. A `local` deployment with
four web workers is four processes, and process-local memory cannot be shared
between them, so tell JASIL how many you run:

```python
jasil_settings.configure(jasil_settings.JasilSettings(web_workers=4))
platform = jasil.container.build_platform(jasil_settings.get_settings())
# ValueError: JASIL's deployment wiring is inconsistent:
#   - state_uri resolves to process-local memory, but this deployment runs
#     multiple processes (web_workers=4 runs several API processes) that need a
#     backend shared across them. ...
```

The `custom` profile promises no defaults, but it is held to the same state, lock
and mail rules: they compare the URIs you set with the topology you declared, and
a process-local backend contradicts several processes under any profile. A
`custom` deployment that genuinely runs one process (`web_workers=1`, in-process
jobs) may still use `memory://` and `noop://`.

### Job workers are processes too

A standalone worker runs your handlers in its own process, with its own platform:
one more process that reaches state, the event bus, the lock and mail. Counting
web workers alone cannot see it, so the deployment declares how its job workers
run:

```python
jasil_settings.JasilSettings(
    job_worker_mode=jasil.JobWorkerMode.STANDALONE,  # or parse_job_worker_mode(env)
    jobs=jasil_settings.JobSettings(enabled=True),
    ...
)
```

It is a fact about the shape, not a count, so it does not change when you scale
workers. **Load the same value in every process of the deployment**, API replicas
and workers alike: the API process must know that workers exist for its own
`build_platform()` to refuse a `memory://` bus that those workers cannot share.

The entry points enforce the declaration. `start_job_worker()` requires
`in_process` and `run_job_worker()` requires `standalone`; starting the wrong one
raises `RuntimeError` even with `enforce_deployment_consistency=False`, because
it is the entry point's contract rather than capability wiring.

On a development machine you may knowingly want an inconsistent setup — the
distributed profile without Redis, say. Set
`enforce_deployment_consistency=False` and each issue is logged as a warning
instead:

```python
jasil_settings.JasilSettings(web_workers=4, enforce_deployment_consistency=False)
```

If you want the issues without building a platform, call the check directly:

```python
from jasil.capabilities import check_deployment_consistency

for issue in check_deployment_consistency(settings):
    print(issue)
```

## The startup report

`build_platform()` logs how each capability actually resolved, at `INFO`:

```
JASIL platform capabilities:
Deployment profile: local (web_workers=1, job_worker_mode=in_process, multi_process=False, multi_node=False)
  state     -> memory  (source: profile default)
  storage   -> local  (source: profile default)
  events    -> memory  (source: profile default)
  realtime  -> memory  (source: follows events_uri (profile default))
  lock      -> noop  (source: profile default)
  mail      -> disabled  (source: not configured)
  geocoding -> disabled  (source: not configured)
  clock     -> system  (source: always the system clock)
```

The `source` column separates what you chose from what you inherited. A
capability that quietly resolved to the wrong backend is otherwise invisible
until it causes a bug that reproduces only under load, or only on one replica.

Call `jasil.capabilities.build_capability_report(settings).render()` yourself if
you want it somewhere other than the log.

## Moving from local to distributed

1. Stand up Redis, object storage, and Postgres.
2. Set the four URIs and switch the profile.
3. Run the [migrations](observability.md#migrations).
4. Set `job_worker_mode=standalone` in the API and the workers alike, stop
    starting the in-process worker in the API, and keep relay/reaper scheduling.
5. Start standalone PostgreSQL workers with their queue allowlists and desired
    process counts.

Subscriber registration keeps the queue assignment, so no domain code changes.
That is the point of depending on the providers rather than on infrastructure.

Run the queue migration before deploying queue-aware code. During a rolling
upgrade, previous-release relays still write `default`; new selective workers
use the current subscriber registrations to route those compatibility rows to
exactly one selected queue. Keep subscriber ids and queue assignments consistent
across the new worker groups before starting them.
