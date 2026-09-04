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
and `start_job_worker()`. That one background consumer drains every named queue
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
    shared — a rate limiter that permits N times the configured rate, a lockout
    that never triggers, a session that exists on one replica and not another.
    None of that fails loudly. A refusal to start does.

Durable jobs have a separate database topology from the capability URIs above.
In a distributed deployment, use PostgreSQL for the outbox, jobs, and worker
registry. API replicas schedule relay/reaping but do not call
`start_job_worker()`; standalone processes call `run_job_worker(queues=...)`.
Queue-specific process counts provide independent concurrency, while several
processes selecting the same queue compete through `SKIP LOCKED`. Redis may
back state or the event bus, but durable jobs themselves do not use it.

## custom

No defaults at all; every capability is explicit. For deployments that do not
match either shape — say many replicas that genuinely want local disk because
each writes only its own scratch data.

## Consistency checks

Beyond the URI defaults, `build_platform()` validates the *combination* before it
constructs a single backend, and refuses inconsistent wiring:

**Cross-process state.** Ephemeral state and the event bus must not resolve to
process-local memory when the topology requires shared state — the `distributed`
profile, or more than one web worker. Both stores would diverge silently across
processes.

**Cross-node storage.** Blob storage must not resolve to the local filesystem
under `distributed`, where replicas run on separate nodes with no shared disk. A
multi-worker `local` deployment shares one host disk, so local storage stays
valid there.

**Coordination lock.** The lock must not resolve to the in-process `noop` lock
when more than one process could run the same scheduled job.

Note that **worker count matters as much as profile**. A `local` deployment with
four web workers is four processes, and process-local memory cannot be shared
between them, so tell JASIL how many you run:

```python
jasil_settings.configure(jasil_settings.JasilSettings(web_workers=4))
# ValueError: JASIL's deployment wiring is inconsistent:
#   - state_uri resolves to process-local memory, but profile=local with
#     web_workers=4 requires a backend shared across processes. ...
```

The `custom` profile opts out of the state and lock rules: it promises no
defaults, so there is nothing for a setting to contradict.

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
Deployment profile: local (web_workers=1, requires_shared_state=False)
  state   -> memory  (source: profile default)
  storage -> local   (source: profile default)
  events  -> memory  (source: profile default)
  lock    -> noop    (source: profile default)
  clock   -> system  (source: always the system clock)
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
4. Stop starting the in-process worker in the API; keep relay/reaper scheduling.
5. Start standalone PostgreSQL workers with their queue allowlists and desired
    process counts.

Subscriber registration keeps the queue assignment, so no domain code changes.
That is the point of depending on the providers rather than on infrastructure.

Run the queue migration before deploying queue-aware code. During a rolling
upgrade, previous-release relays still write `default`; new selective workers
use the current subscriber registrations to route those compatibility rows to
exactly one selected queue. Keep subscriber ids and queue assignments consistent
across the new worker groups before starting them.
