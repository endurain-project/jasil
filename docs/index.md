# JASIL

**J**ust **A**nother **S**ubstrate & **I**nfrastructure **L**ibrary — a
framework-agnostic infrastructure substrate for Python services.

Your domain code depends on small composable capability protocols - state,
storage, events, locks, a clock - instead of on Redis, S3, or Postgres directly.
Complete aggregate protocols keep URI-selected backends interchangeable, so the
same code runs single-process on a laptop and multi-replica in production.

!!! warning "Status: 0.4.0"
    The API is still settling. Expect breaking changes on minor versions until
    `1.0.0`; see [API stability](api-stability.md).

## What you get

| Layer | What it does |
|---|---|
| **Providers** | Narrow protocols domain services can depend on, plus complete aggregates such as `StorageProvider` for the assembled platform. |
| **Backends** | A working implementation of each, selected by URI. |
| **Deployment profiles** | `local` needs no infrastructure at all. `distributed` requires shared backends and refuses to guess them. |
| **Event pipeline** | One envelope, one publish seam, payload versioning that survives a rolling deploy. |
| **Durable jobs** | A transactional outbox relayed into leased per-subscriber jobs, with backoff and a dead-letter queue. |
| **Observability** | An event lifecycle log and bounded retention pruning. |

## Install

```bash
pip install jasil                    # core: memory / local disk / in-process
pip install "jasil[redis,s3]"        # distributed state, events and storage
pip install "jasil[all]"             # every optional backend
```

A core install depends on **`sqlalchemy` and `pydantic` only**. Every backend
client lives behind an extra and is imported lazily, so a single-process
deployment never loads `redis`, `boto3`, or `requests`.

## Three things JASIL does not do

These are deliberate, and they are what make it embeddable:

**It does not own your database.** You own the declarative base and the engine;
JASIL maps its tables into *your* registry. One metadata object, one
`create_all`, one migration run.

**It does not read your configuration.** No environment variables, no secret
files. You build a settings object from whatever source you like and install it.

**It does not own a web framework.** Nothing in the core imports FastAPI. The
`Depends` helpers are behind an extra you may ignore entirely.

## One thing it cannot do yet

!!! warning "JASIL is synchronous"
    Every provider method, every subscriber, and the whole durable-jobs layer is
    ordinary blocking Python, and the ORM integration expects a synchronous
    `sessionmaker`. There is no `AsyncSession` support and no async provider
    variant.

    On an async framework you can still use JASIL, but calls into it have to run
    on a worker thread (FastAPI does that automatically for a `def` route or
    dependency, and `anyio.to_thread.run_sync` does it explicitly). If your
    application is built on `AsyncSession` throughout, the event log and durable
    jobs will not fit without a second, synchronous engine.

    Async support is a candidate for a later release; it is not in `0.1.0`.

## Quick start

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

import jasil.orm as jasil_orm
import jasil.settings as jasil_settings
from jasil.container import build_platform
from jasil.runtime import set_active_platform


class Base(DeclarativeBase):
    """Your application's declarative base."""


# 1. Map JASIL's tables into your registry, before any database use.
jasil_orm.map_models(Base)

# 2. Hand JASIL a session factory bound to your engine.
engine = create_engine("postgresql+psycopg://...")
jasil_orm.configure_sessionmaker(sessionmaker(bind=engine))

# 3. Configure, build, publish process-wide.
jasil_settings.configure(jasil_settings.JasilSettings(data_dir="/srv/data"))
set_active_platform(build_platform())
```

Then depend on capabilities rather than infrastructure:

```python
from jasil.runtime import get_active_platform

platform = get_active_platform()
platform.state.set("session:abc", b"...", ttl_seconds=3600)
platform.storage.save("avatars", "42.webp", image_bytes)

with platform.lock.try_acquire("nightly-backfill") as acquired:
    if acquired:
        run_backfill()
```

Nothing above changes when you move to Redis and S3 — only the
[configuration](configuration.md) does.

On shutdown, call `jasil.lifecycle.shutdown()`:

```python
import jasil.lifecycle as jasil_lifecycle

jasil_lifecycle.shutdown()
```

It stops the durable-job worker, then releases the platform — the event-bus
consumer thread and the shared Redis clients — and unpublishes it. That order
matters: the worker runs subscribers, and a subscriber that publishes needs the
bus still up. It never raises, and it is safe to call when nothing was started.

Your APScheduler instance and your database engine are yours to stop. JASIL never
created either, so it does not close them.

## On FastAPI

With the `fastapi` extra, `jasil.deps` exposes each capability as a dependency,
so a route depends on a provider rather than importing a backend:

```python
from fastapi import Depends

from jasil.deps import get_storage
from jasil.providers import StorageProvider


@app.post("/avatars/{user_id}")
def upload_avatar(user_id: int, image: bytes, storage: StorageProvider = Depends(get_storage)):
    return storage.save("avatars", f"{user_id}.webp", image)
```

The quick start above is all the wiring this needs: the dependencies fall back to
the process-wide platform published by `set_active_platform`. Attach one to
`app.state.platform` instead when a single process runs more than one platform —
two apps mounted together, or a test client — since that binding is scoped to the
app rather than to the process, and takes precedence when both are set.

## Testing against it

JASIL installs several things process-wide, and a suite has to put every one of
them back between cases. `jasil.testing` is that fixture, so you do not have to
work out the list:

```python
import jasil.testing as jasil_testing


@pytest.fixture(autouse=True)
def jasil(tmp_path):
    platform = jasil_testing.install_test_platform(tmp_path)
    yield platform
    jasil_testing.reset_all()
```

`install_test_platform` roots local storage inside `tmp_path`, installs a
`FixedClock` so lease expiry, retry backoff and retention windows can be
exercised without sleeping, and — the part that is easy to forget — *publishes*
the platform, without which every `publish` raises.

Map the models once for the whole session, though. `reset_all` deliberately
leaves the declarative base alone: JASIL's model modules capture it at import
time, so clearing it would strand every model already imported.

## Where to go next

- [Configuration](configuration.md) — the settings object and every capability URI.
- [Providers & backends](providers-and-backends.md) — what each protocol
  guarantees, and how the backends differ.
- [Deployment profiles](deployment-profiles.md) — going from one process to many.
- [Events & outbox](events-and-outbox.md) — publishing, delivery guarantees, and
  payload versioning.
- [Durable jobs](durable-jobs.md) — retries, leases, and dead-letters.
- [Observability](observability.md) — the event log and retention.
- [Threat model](security/threat-model.md) — what JASIL guards, and what it
  leaves to you.
- [Integration checklist](security/integration-checklist.md) — the list to work
  through before you run it in production.

## License

MIT.
