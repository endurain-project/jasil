# Integration checklist

What a host has to do for a JASIL deployment to be sound. The
[threat model](threat-model.md) explains the reasoning; this is the list to work
through.

## Deployment shape

- [ ] Set `profile` to match reality. `distributed` refuses to start when a
      capability URI is unset, which is the point — a silent fallback to a
      process-local backend across replicas is the failure the profile system
      exists to prevent.
- [ ] Set `web_workers` to the number of worker processes you actually run. Four
      workers under the `local` profile are still four processes, and
      process-local state cannot be shared between them.
- [ ] Leave `enforce_deployment_consistency` on. It refuses to build a platform
      whose wiring contradicts its topology. Turning it off downgrades those to
      warnings — reasonable on a development machine, not in production.
- [ ] Read the capability report JASIL logs at `INFO` on startup and confirm each
      capability resolved to what you expected.

## Database

- [ ] JASIL never creates the engine. Give `configure_sessionmaker` a factory
      bound to an engine whose credentials, TLS, and pool sizing you control.
- [ ] Run the packaged migrations (`jasil.migrations.upgrade`) rather than
      `create_all` in production, and keep them in your deploy pipeline. JASIL
      uses its own `jasil_alembic_version` table and never touches yours.
- [ ] On MySQL, note that `DATETIME` stores whole seconds and rounds. Do not
      compare a timestamp JASIL persisted for equality with the value you passed.

## Secrets and event payloads

- [ ] Audit what your producers put in `payload` and `metadata`. Both are stored
      verbatim in three tables, serialized onto the Redis stream, and `metadata`
      is logged in full when a best-effort subscriber fails. Carry identifiers.
- [ ] Never put a token, password, key, or personal data in either.
- [ ] Remember that a dead-lettered job keeps its payload indefinitely — those
      rows are deliberately never pruned.

## Outbound egress

- [ ] Leave reverse geocoding unconfigured unless you need it. Unset means the
      capability is a no-op backend and no outbound call is ever made.
- [ ] If you allow-list for a self-hosted instance, prefer a **CIDR** over a
      hostname. A hostname entry exempts every address that name resolves to,
      including a cloud metadata endpoint.
- [ ] Alert on the `SSRF allowlist hit` log line. Every exemption taken is
      logged, and the hostname form logs at `WARNING`.
- [ ] Treat the host check as start-up-time only. If the upstream is somewhere an
      attacker could later repoint DNS at, put egress policy in the network too.

## Storage

- [ ] Keep keys server-generated. Traversal is rejected either way, but a key
      derived from a filename is a category of problem you do not need.
- [ ] For `local://`, restrict the URL prefix in the web server that serves it.
      Those URLs **cannot expire** — `expires_in` is ignored and the link is
      permanent.
- [ ] If you need genuinely time-limited links, use `s3://`, where `expires_in`
      produces a real presigned URL.
- [ ] Serve uploaded content from a separate origin, or with
      `Content-Disposition: attachment` and a restrictive `Content-Security-Policy`.

## Durable jobs

- [ ] Make every subscriber handler **idempotent**. Delivery is at-least-once.
- [ ] Give every durable subscriber a reconciliation net — a scheduled backfill,
      or a documented exemption — and hold the registry to it with
      `assert_nets_complete` in a test. Delivery is at-least-once but not
      guaranteed.
- [ ] Alert on `dead_letter` job count, and on the `Reaped N expired job lease(s)`
      warning: a lease only expires because a worker died or overran.
- [ ] Alert on the oldest pending job age from `jasil.admin.get_jobs_summary()`.

## The admin surface

- [ ] Put authentication and authorization in front of every `jasil.admin` route.
      It exposes operational data and `replay_dead_letter_job` changes state.
      JASIL has no notion of who is calling.
- [ ] Do not reflect the summaries to end users. `recent_failures` carries error
      messages and `event_metadata`.

## Logging

- [ ] JASIL logs through `logging.getLogger(__name__)` and configures nothing.
      Route the `jasil.*` loggers wherever your application logs go.
- [ ] Do not run production at `DEBUG`. The debug lines name every event
      published and every job batch claimed.
- [ ] Do not parse or alert on log *wording* — only levels and structured fields
      are stable. See [API stability](../api-stability.md).
- [ ] Wire `jasil.correlation.configure_provider` to your request-id middleware so
      events carry it.

## Shutdown

- [ ] Call `jasil.lifecycle.shutdown()` on the way down. It stops the durable-job
      worker before releasing the bus its subscribers publish through.
- [ ] Stop your own scheduler and dispose your own engine — JASIL created neither
      and does not touch them.

## Dependencies

- [ ] Install only the extras you use. A core install pulls `sqlalchemy` and
      `pydantic` and nothing else, and every backend client is imported lazily.
- [ ] Pin JASIL. While the version is `0.x`, a minor release may break the API.
- [ ] Verify the release artifact if you care about provenance — see
      [Verifying a release](https://github.com/endurain-project/jasil#verifying-a-release).
