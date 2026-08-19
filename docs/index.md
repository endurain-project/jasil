# JASIL

**J**ust **A**nother **S**ubstrate & **I**nfrastructure **L**ibrary — a
framework-agnostic infrastructure substrate for Python services.

Your domain code depends on small capability protocols — state, storage, events,
locks, a clock — instead of on Redis, S3, or Postgres directly. Which
implementation backs each one is a URI in your configuration, so the same code
runs single-process on a laptop and multi-replica in production.

!!! warning "Status: 0.1.0"
    The API is still settling. Expect breaking changes on minor versions until
    `1.0.0`; see [API stability](api-stability.md).

## What you get

| Layer | What it does |
|---|---|
| **Providers** | Protocols your domain code depends on: `StateProvider`, `StorageProvider`, `EventBusProvider`, `LockProvider`, `ClockProvider`, `GeocodingProvider`. |
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

## Where to go next

- [Configuration](configuration.md) — the settings object and every capability URI.
- [Providers & backends](providers-and-backends.md) — what each protocol
  guarantees, and how the backends differ.
- [Deployment profiles](deployment-profiles.md) — going from one process to many.
- [Events & outbox](events-and-outbox.md) — publishing, delivery guarantees, and
  payload versioning.
- [Durable jobs](durable-jobs.md) — retries, leases, and dead-letters.
- [Observability](observability.md) — the event log and retention.

## Licence

MIT.
