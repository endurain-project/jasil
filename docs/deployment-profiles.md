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

## custom

No defaults at all; every capability is explicit. For deployments that do not
match either shape — say many replicas that genuinely want local disk because
each writes only its own scratch data.

## Consistency checks

Beyond the URI defaults, JASIL validates the *combination* at startup and refuses
inconsistent wiring:

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
between them:

```python
from jasil.profile import DeploymentProfile, resolve_topology

resolve_topology(DeploymentProfile.LOCAL, web_workers=4).requires_shared_state
# True
```

## The startup report

`jasil.capabilities` renders how each capability actually resolved, for the
startup log:

```
Deployment profile: local (WEB_WORKERS=1, requires_shared_state=False)
  state   -> memory       (source: STATE_URI)
  storage -> local        (source: STORAGE_URI)
  events  -> in-process   (source: EVENTS_URI)
  lock    -> none         (source: LOCK_URI)
  clock   -> system       (source: static)
```

Worth logging at every startup. A capability that quietly resolved to the wrong
backend is otherwise invisible until it causes a bug that reproduces only under
load, or only on one replica.

## Moving from local to distributed

1. Stand up Redis, object storage, and Postgres.
2. Set the four URIs and switch the profile.
3. Run the [migrations](observability.md#migrations) if you enable the event log
   or durable jobs.

No domain code changes. That is the point of depending on the providers rather
than on the infrastructure.
