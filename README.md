# JASIL

[![License](https://img.shields.io/badge/license-MIT-green)](https://github.com/endurain-project/jasil/blob/main/LICENSE.md)
[![Release](https://img.shields.io/github/v/release/endurain-project/jasil?label=release&color=blue)](https://github.com/endurain-project/jasil/releases)
[![PyPI version](https://img.shields.io/pypi/v/jasil)](https://pypi.org/project/jasil/)
[![PyPI downloads](https://img.shields.io/pypi/dm/jasil)](https://pypi.org/project/jasil/)
[![Python](https://img.shields.io/badge/python-3.12%2B-blue)](https://pypi.org/project/jasil/)
[![Docs](https://img.shields.io/badge/docs-jasil.endurain.com-blue)](https://jasil.endurain.com/)
[![Stars](https://img.shields.io/github/stars/endurain-project/jasil?label=stars&logo=github)](https://github.com/endurain-project/jasil)

**J**ust **A**nother **S**ubstrate & **I**nfrastructure **L**ibrary — a
framework-agnostic infrastructure substrate for Python services: swappable
capability backends, an event pipeline, durable jobs, and observability.

> **Status:** `0.5.0` - the API is still settling. Expect breaking changes on
> minor versions until `1.0.0`.

---

## What it gives you

| Layer | What it does |
|---|---|
| **Providers** | Small composable protocols - state, storage, events, lock, clock, geocoding - that domain code depends on instead of Redis, S3, or Postgres. Complete aggregate protocols keep URI-selected backends interchangeable. |
| **Backends** | A working implementation of each, selected by URI: `memory://` or `redis://`, `local://` or `s3://`, `noop://` or `postgres-advisory://`. |
| **Deployment profiles** | `local` runs single-process with zero extra infrastructure. `distributed` requires shared backends and refuses to guess them. |
| **Event pipeline** | One envelope, one publish seam, payload schema versioning that survives a rolling deploy. |
| **Durable jobs** | A transactional outbox relayed into leased per-subscriber jobs, with exponential backoff and a dead-letter queue. |
| **Observability** | An event lifecycle log and bounded retention pruning for every table it owns. |

A core install depends on **`sqlalchemy` and `pydantic` only**. Every backend
client lives behind an extra and is imported lazily, so a single-process
deployment never loads `redis`, `boto3`, or `requests`.

> **JASIL is synchronous.** Every provider, subscriber, and the durable-jobs
> layer is blocking Python, and the ORM integration expects a synchronous
> `sessionmaker` — there is no `AsyncSession` support. On an async framework,
> call into JASIL from a worker thread (a `def` FastAPI route or dependency does
> this for you). See [the docs](https://jasil.endurain.com/#one-thing-it-cannot-do-yet).

## Installation

```bash
pip install jasil                    # core: memory / local disk / in-process
pip install "jasil[redis,s3]"        # distributed state, events and storage
pip install "jasil[all]"             # every optional backend
```

| Extra | Enables |
|---|---|
| `redis` | Redis state store and Redis-Streams event bus |
| `s3` | S3-compatible object storage |
| `postgres` | PostgreSQL advisory locks |
| `jobs` | Durable-job scheduling |
| `fastapi` | `Depends` helpers exposing the providers to routes |
| `geocoding` | HTTP reverse geocoding |
| `migrations` | Packaged Alembic revisions |

### Verifying a release

Releases are built and published by [this repository's release workflow](.github/workflows/publish-jasil.yml) through PyPI Trusted Publishing, with [PEP 740](https://peps.python.org/pep-0740/) attestations. You can confirm a downloaded artifact came from that workflow and was not substituted:

```bash
uvx pypi-attestations verify pypi \
  --repository https://github.com/endurain-project/jasil \
  pypi:jasil-<version>-py3-none-any.whl
```

A successful run prints `OK: <filename>`. `Provenance for file ... was not found` means the artifact predates attested publishing rather than that verification failed.

Each release run also produces a CycloneDX SBOM and `SHA256SUMS`, generated from a clean install of the built wheel. These are retained as workflow artifacts on the release run rather than published to PyPI.

## Quick start

JASIL never reads your environment, creates your engine, or owns your
declarative base — the host supplies all three.

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

# 3. Configure, build, publish.
jasil_settings.configure(jasil_settings.JasilSettings(data_dir="/srv/data"))
set_active_platform(build_platform())
```

Then depend on capabilities, not on infrastructure:

```python
from jasil.runtime import get_active_platform

platform = get_active_platform()
platform.state.set("session:abc", b"...", ttl_seconds=3600)
platform.storage.save("thumbnails", "42.webp", image_bytes)

with platform.lock.try_acquire("nightly-backfill") as acquired:
    if acquired:
        run_backfill()
```

Publishing an event goes through one seam, so switching from best-effort
delivery to a transactional outbox is a configuration change, not a rewrite:

```python
from jasil.publisher import publish

publish("activity.created", {"activity_id": 42}, source="api:store_activity", db=db)
```

## Going distributed

Change the profile and point the capability URIs at real infrastructure:

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

The `distributed` profile **refuses to start** if a capability URI is unset. A
silent fallback to a process-local backend across replicas is the failure the
profile system exists to prevent.

## Development

```bash
uv sync --all-extras --group dev
uv run pytest              # tests + coverage gate
uv run ruff check .        # lint
uv run mypy                # type check
uv run lint-imports        # architectural import contracts
```

## Documentation

Full documentation is available at the [JASIL docs site](https://jasil.endurain.com/).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.md) file for details.

## Contributing

Contributions welcome! See [Contributing Guidelines](CONTRIBUTING.md) for guidelines.

<div align="center">
  <sub>Built with ❤️ from Portugal | Part of the <a href="https://github.com/endurain-project">Endurain</a> ecosystem</sub>
</div>
