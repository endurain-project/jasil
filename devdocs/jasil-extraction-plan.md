# JASIL — extraction plan

Turning Endurain's `infra` package into a standalone, MIT-licensed library that
follows the conventions already established by `jafaal` and `safeuploads`.

| | |
|---|---|
| **Name** | `jasil` — *Just Another Substrate & Infrastructure Library* (backronym family with JAFAAL) |
| **PyPI** | `jasil` — verified available (2026-08-17) |
| **Import name** | `jasil` |
| **Licence** | MIT (`LICENSE.md` already in place) |
| **Repo** | `github.com/endurain-project/jasil` (+ Forgejo mirror) |
| **Docs** | `https://jasil.endurain.com/` |
| **Status** | Not started — source is still the Endurain-coupled `infra/` tree |

Alternative expansions if the current one doesn't stick: *Just Another Service
Infrastructure Library*, *Just Another Ships-Its-Own-Infra Library*.

---

## 1. What is being extracted

41 modules, ~5,470 LOC. Five concerns, already cleanly separated in the source.

| Layer | Modules | LOC |
|---|---|---|
| Ports (capability protocols) | `providers.py` | 217 |
| Backends (adapters) | `backends/` — state (memory/redis), storage (local/s3), events (in-process/redis-streams), lock (noop/pg-advisory), clock, geocoding, route-map | ~800 |
| Composition root | `container.py`, `profile.py`, `capabilities.py`, `runtime.py`, `deps.py`, `redis.py`, `node.py`, `async_bridge.py` | ~890 |
| Event pipeline | `events.py`, `publisher.py`, `subscribers.py`, `event_versioning.py` | ~570 |
| Durable jobs | `jobs/` — outbox → relay → leased per-subscriber jobs with backoff + dead-letter | ~1,200 |
| Observability | `event_log/`, `retention.py`, `pruning.py` | ~750 |

**Scope decision:** ship *one* package with optional extras, not two. The
outbox/jobs/event-log half could stand alone, but it is wired into the
capability substrate through `publisher.py` and `container.py`; splitting it
doubles the release surface for no consumer benefit.

---

## 2. Phase 1 — break the Endurain host coupling

28 imports of `core.*` across 16 files. This is the blocking work; everything
else is scaffolding. Each fix has a JAFAAL precedent to copy.

| Coupling | Files | Fix |
|---|---|---|
| `core.logger` | 16 files | `logger = logging.getLogger(__name__)`. Replace every `core_logger.context(...)` call with a plain `extra={...}` dict. JAFAAL's `ports.py` is the reference. |
| `core.database` (`Base`, `SessionLocal`, `engine`) | `event_log/models.py`, `jobs/models.py`, `event_log/recorder.py`, `jobs/service.py`, `retention.py`, `backends/lock_pg.py` | **Option B — the host owns the `Base`.** Port `jafaal/orm.py`: `jasil.map_models(Base)` + `jasil.configure_sessionmaker(...)`. JASIL never creates the engine. |
| `core.config.settings` | `container.py`, `jobs/service.py`, `publisher.py`, `retention.py` | Own settings model + `jasil.configure()` / `get_settings()`, modelled on `jafaal.settings.AuthSettings`. Covers `JOBS_*`, `EVENT_LOG_*`, `*_RETENTION_DAYS`, `resolved_*_uri`, `DATA_DIR`. |
| `core.network` (SSRF host/URL guards) | `backends/geocoding_http.py`, `backends/route_map_static.py` | Vendor into a private `jasil/_core/network.py` (JAFAAL vendors and tests the same helpers). Only needed if the geocoding extra survives — see §3. |
| `core.middleware_request_id` | `publisher.py` | Pluggable `correlation_id_provider` callable, defaulting to a module-local contextvar. |

### Checklist

- [ ] Swap `core.logger` → stdlib `logging` in all 16 modules
- [ ] Add `jasil/orm.py` with `map_models` / `configure_sessionmaker` / `transactional`
- [ ] Rebase `event_log/models.py` and `jobs/models.py` onto the runtime-resolved base
- [ ] Add `jasil/settings.py` + `configure()` / `get_settings()`
- [ ] Replace every `core_config.settings.X` read with `get_settings().x`
- [ ] Add a request-id / correlation-id port
- [ ] Vendor or drop `core.network`

---

## 3. Phase 2 — strip the Endurain domain leakage

| Item | Where | Decision |
|---|---|---|
| `RouteMapRendererProvider`, `RouteMapRenderRequest`, `backends/route_map_static.py` | `providers.py`, `backends/` | **Remove.** Activity thumbnails are Endurain's domain; the `staticmap` dep has no place here. Endurain keeps it as a host-supplied adapter. |
| `GeocodingProvider`, `GeocodedPlace`, `backends/geocoding_http.py` | `providers.py`, `backends/`, `container.py` | **Decide:** drop entirely, or keep behind an optional `geocoding` extra. Currently hardcodes nominatim / photon / geocode.maps.co and pulls `requests`. Leaning drop. |
| `META_ACTIVITY_ID`, `META_USER_ID` | `events.py`, consumed by `subscribers.py` | Generalise — metadata keys are host-defined. Keep only `request_id`-style correlation keys, if any. |
| `Endurain/{API_VERSION}` user agent, `GEOCODES_MAPS_API`, `NOMINATIM_API_HOST`, `PHOTON_API_HOST`, `REVERSE_GEO_*` | `container.py` | Goes with the geocoding decision. |
| Docstring references to Garmin login thread, MFA, websocket tickets, activities | `async_bridge.py`, `capabilities.py`, `providers.py`, `retention.py`, `subscribers.py` | Rewrite as generic examples. Non-blocking but must be done before the docs ship. |

---

## 4. Phase 3 — naming / layout consistency with the family

Optional but worth doing before the first release, while nothing depends on the
names:

- [ ] `providers.py` → `ports.py` (JAFAAL calls these ports)
- [ ] `backends/` → `adapters/` (JAFAAL calls these adapters)
- [ ] Add `jasil/py.typed` — both siblings ship it, `infra` does not
- [ ] Consider a private `jasil/_core/` for vendored low-level helpers, per JAFAAL

---

## 5. Phase 4 — packaging

`pyproject.toml` does not exist yet. Base it on `jafaal/pyproject.toml`, which is
the more complete of the two siblings.

- [ ] `[project]` — name, version, MIT, authors, `requires-python`, classifiers, `[project.urls]` (Homepage / Documentation / Repository / Changelog / Issues / Security)
- [ ] `[build-system]` hatchling + `[tool.hatch.build.targets.wheel|sdist]`
- [ ] `[tool.uv]` — `required-version` floor+ceiling, `exclude-newer = "30 days"`, explicit pypi index, `[tool.uv.exclude-newer-package]` escape hatch for advisories
- [ ] `[tool.hatch.envs.default.scripts]` — `lint` / `format` / `test` / `typecheck` / `lint-imports` / `validate` / `check`
- [ ] `[tool.ruff]` + lint select set + per-file ignores; `[tool.mypy]` (strict, as safeuploads)
- [ ] `[tool.pytest.ini_options]` + `[tool.coverage.*]` with a `fail_under` gate
- [ ] `[tool.importlinter]` contracts — see below
- [ ] `jasil/migrations/` — Alembic revisions for `event_log`, `processing_jobs`, `event_outbox` (JAFAAL ships `jafaal/migrations`)

### Optional dependency extras

| Extra | Pulls | Guards |
|---|---|---|
| `redis` | `redis` | `backends/state_redis.py`, `backends/events_redis.py`, `redis.py` |
| `s3` | `boto3` | `backends/storage_s3.py` (already lazily imported) |
| `postgres` | `psycopg` | `backends/lock_pg.py`, jobs on Postgres |
| `jobs` | `apscheduler` | `jobs/service.py` |
| `fastapi` | `fastapi` | `deps.py` |
| `geocoding` | `requests` | `backends/geocoding_http.py` |
| `migrations` | `alembic` | packaged revisions |
| `all` | everything | convenience |

Core install must stay dependency-light: `sqlalchemy` + `pydantic` only.

### import-linter contracts

The architecture is already documented in the module docstrings — encode it so it
can't rot:

1. **Pure ports stay pure** — `jasil.ports`, `jasil.events`, `jasil.profile` must
   not import `jasil.adapters` (this invariant is stated verbatim in
   `providers.py`'s docstring today).
2. **Adapters stay optional** — nothing outside `jasil.adapters` may import an
   adapter; only the composition root selects them.
3. **The substrate never imports the jobs layer** except through `publisher.py`.

---

## 6. Phase 5 — tests

**`tests/` is currently empty. This is the single largest item in the plan.**

Reference points: `jafaal` has ~50 test modules with an 87% coverage gate;
`safeuploads` gates at 90% with a separate Hypothesis fuzz suite.

Priority order:

- [ ] `profile.py` / `capabilities.py` — pure logic, cheapest coverage, catches the fail-fast consistency rules
- [ ] `events.py` / `event_versioning.py` — envelope + upgrade/downgrade skew paths
- [ ] `backends/state_memory.py` + `state_redis.py` against **fakeredis** — one shared conformance suite run against both, since they must be behaviourally identical (`set_if_absent`, `get_and_delete`, `record_tiered_failure`)
- [ ] `jobs/` — claim/lease/backoff/dead-letter state machine, `(event_id, subscriber_id)` idempotency, relay fan-out
- [ ] `pruning.py` / `retention.py` — batch bounds, and that in-flight + dead-letter rows are never deleted
- [ ] `publisher.py` — outbox vs. bus routing, and that publish failures are swallowed
- [ ] `container.py` — every URI scheme resolves to the expected backend; unknown scheme raises
- [ ] Host-agnosticism test (JAFAAL's `test_host_agnostic.py`) — asserts no `core.*` import ever comes back
- [ ] Optional-deps test — `import jasil` works with zero extras installed

---

## 7. Phase 6 — repo, docs, CI

- [ ] `git init`, create `endurain-project/jasil`
- [ ] `.gitignore` (copy from `jafaal`, includes `devdocs/`), `renovate.json`
- [ ] `README.md`, `CHANGELOG.md`
- [ ] Fix `CONTRIBUTING.md` — currently titled "Contributing to Endurain"
- [ ] Fix `SECURITY.md` — stray `|` on line 1
- [ ] `mkdocs.yml` + `docs/`: `index.md`, `configuration.md`, `ports-and-adapters.md`, `deployment-profiles.md`, `events-and-outbox.md`, `durable-jobs.md`, `observability.md`, `api-stability.md`, `api.md`
- [ ] Workflows (from `jafaal/.github/workflows`): `lint`, `test`, `test-matrix`, `audit`, `docs`, `publish` (trusted publishing), `conventional-commits`, `mirror`
- [ ] `.github/scripts/smoke_import.py` + `check_conventional_commits.py`
- [ ] DNS + Pages for `jasil.endurain.com`

---

## 8. Phase 7 — Endurain integration

- [ ] Add `jasil` to Endurain's dependencies
- [ ] Replace `infra.*` imports with `jasil.*` across the backend
- [ ] Write the five host adapters: logger bridge (or drop — stdlib logging propagates to `core.logger`'s handlers), `map_models(Base)`, `configure_sessionmaker(SessionLocal)`, settings mapping, request-id provider
- [ ] Keep `route_map_static` (and geocoding, if dropped) in Endurain as host adapters implementing JASIL-shaped protocols
- [ ] Delete Endurain's `infra/` package

---

## 9. Decisions

Settled 2026-08-17.

| Question | Decision |
|---|---|
| Version to launch at | **`0.1.0`** — one consumer, no external users, API still settling |
| `requires-python` | **`>=3.12,<4.0`** — matches `jafaal` and Endurain's floor |
| Geocoding (§3) | **In**, behind an optional `geocoding` extra. Implies `core.network`'s SSRF guards must be vendored into `jasil/_core/network.py`, not dropped. |
| `async_bridge.py` | **Keep** |
| `deps.py` | **Keep**, behind the `fastapi` extra |

Note: `route_map_static.py` is still removed (§3) — it was a domain-leakage call,
not an extras call, and the `staticmap` dependency goes with it.

---

## Appendix — names considered

Verified against PyPI on 2026-08-17.

**Chosen:** `jasil`.

| Available | Taken |
|---|---|
| `underlay`, `subfloor`, `appsubstrate`, `platformsubstrate`, `platformkit`, `capabilitykit`, `runtimekit`, `scaffoldkit`, `groundplane`, `underpinning`, `undercarriage`, `moorings`, `pierhead`, `backbone`, `subsoil`, `abutment`, `eventspine`, `eventworks`, `eventbox`, `outboxkit`, `dispatchkit`, `adapterkit`, `portkit`, `hexaport`, `swappable`, `pluggables`, `compositionroot`, `deployprofile`, `profilekit`, `substratekit`, `girderkit`, `fastinfra`, `fastplatform`, `fastjobs`, `japsl`, `japis`, `jasl` | `substrate`, `substratum`, `bedrock`, `keystone`, `groundwork`, `girder`, `joist`, `rebar`, `ballast`, `plinth`, `trellis`, `pylon`, `truss`, `lintel`, `stanchion`, `chassis`, `keel`, `gantry`, `understory`, `loam`, `humus`, `stratum`, `strata`, `mortar`, `grout`, `cornerstone`, `underpin`, `infrakit`, `outbox`, `pgjobs`, `jobbox`, `jasper`, `nexus`, `plexus`, `conduit`, `spine`, `switchboard`, `patchbay`, `pluggable` |

`infra` was rejected regardless of availability: too generic to publish, and it
squats an obvious top-level namespace in every host application.
