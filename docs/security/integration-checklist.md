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
- [ ] Set `job_worker_mode=standalone` in **every** process — API and workers —
      when durable jobs run in dedicated worker processes. A worker is a process
      too: without the declaration the checks cannot see it, and a handler sends
      mail or touches state through a backend its API never shares.
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
- [ ] Upgrade through JASIL's packaged revisions before rolling out queue-aware
      code. The queue column is non-null and has a database-side `default`
      default so previous-release writers remain valid during the rollout.
      Selective new workers adopt legacy `default` rows only for subscribers
      assigned to their queues, and a selective `default` worker excludes
      subscribers assigned to named queues.

## Secrets and event payloads

- [ ] Audit what your producers put in `payload` and `metadata`. Both are stored
      verbatim in three tables, serialized onto the Redis stream, and `metadata`
      is logged in full when a best-effort subscriber fails. Carry identifiers.
- [ ] Never put a token, password, key, or personal data in either.
- [ ] Apply the same rule to worker metadata. It appears in operator summaries
      and is capped at 16 KiB of serialized JSON.
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

## Outbound mail

- [ ] Use `smtp://` (STARTTLS) or `smtps://`. Cleartext (`mail.secure = False`) is for a relay on the same host or a private network only; it is refused with credentials.
- [ ] Put credentials in `MailSettings.username` / `password`, never in the URI.
- [ ] Set `from_address` to an identity your relay has verified.
- [ ] Remember the SMTP host is **not** SSRF-checked — it is expected to be an
      internal relay. Keep that setting out of attacker-influenced configuration.
- [ ] Do not put a rendered body or secrets in an event payload that drives a
      mail job; build the message in the handler (see
      [Durable jobs](../durable-jobs.md#sending-mail)).

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
- [ ] Alert on `dead_letter` job count, on the `Reaped N expired job lease(s)`
      warning (a lease only expires because a worker died or went silent), and on
      `exceeded its maximum runtime` errors (a handler is hung or too slow).
- [ ] Alert on the oldest pending job age from `jasil.admin.get_jobs_summary()`.
- [ ] Alert on `superseded` attempts (a handler finished after its job was taken:
      it ran twice) and on `claimed_by_stale_workers` from
      `jasil.admin.get_workers_summary()` (jobs waiting out a lease behind a
      silent worker). Read a job's whole history with `jasil.admin.get_job`.
- [ ] Assign queue names at subscriber registration. Keep them lowercase ASCII,
      stable, and domain-neutral from JASIL's point of view; renaming a queue
      while old rows remain requires workers for both names during the drain.
- [ ] For SQLite, run exactly one API process and one in-process worker
      (`job_worker_mode=in_process`, the default). For PostgreSQL, do not start
      handlers in API replicas; set `job_worker_mode=standalone` and run
      standalone workers with explicit non-empty allowlists, sizing each queue's
      process group.
- [ ] Remember that durable jobs do not require Redis. Do not introduce a second
      queue as a source of truth beside `jasil_processing_jobs`.
- [ ] Set `max_runtime_seconds` above the longest handler you expect, and keep
      `lease_seconds` at least three `heartbeat_interval_seconds`. The heartbeat
      renews a running job's lease, so `lease_seconds` bounds how long a worker may
      go silent, not how long a handler may run. A handler past
      `max_runtime_seconds` is abandoned and may be run again, and a stale worker
      cannot finalize over a replacement claim, but both may already have
      performed external side effects, so idempotency still matters.
- [ ] In handlers that loop or perform several side effects, check
      `jasil.jobs.lease_lost()` between units of work and return early.

## The admin surface

- [ ] Put authentication and authorization in front of every `jasil.admin` route.
      It exposes operational data and `replay_dead_letter_job` changes state.
      JASIL has no notion of who is calling.
- [ ] Do not reflect the summaries to end users. `recent_failures` carries error
      messages and `event_metadata`.
- [ ] Expose worker and queue summaries through host-owned authenticated routes.
      JASIL supplies typed reads, not FastAPI routes or UI.
- [ ] Treat running/stale/stopped as operator telemetry. Decide alert and
      container-health policy in the host rather than mapping status to health
      automatically.

## The audit trail

- [ ] Run migration `rev0006` as the role that should own the audit table and functions, and
      [grant](../audit.md#roles-and-grants) the application `INSERT` on `jasil_audit_log` and nothing else on it.
- [ ] Give the observability product a separate, `SELECT`-only role, and run `run_audit_maintenance` as a role
      that can read the log, ideally not the application's.
- [ ] Set the context (`set_context`/`audit_context`) in every entry point: with none, rows say `anonymous`/`api`.
- [ ] Keep `details` to ids and changed field names. Never a secret, a token or a password.
- [ ] Schedule `schedule_audit_maintenance` on every replica, each using a role with the
      [maintenance grants](../audit.md#roles-and-grants) (or run it from a separate maintenance process), and alert
      on `get_audit_health()` (`write_failures`, `furthest_partition_date`, `last_checkpoint_at`): a missing
      partition fails audited writes, and the runtime grants cannot seal checkpoints or apply retention.
- [ ] Set `audit.signing_key` (kept out of the database) and `audit.external_sink_uri` (a separate bucket with Object Lock and a default compliance-mode retention period),
      give the bucket a default compliance-mode retention period and a policy denying any `PutObject` without
      `If-None-Match: *`, and verify on a schedule with `verify_checkpoints`, passing every key the trail was signed with.
      Reconcile the sink with `jasil_audit_checkpoints` from outside the database operators' reach: against the table
      owner the signing key alone detects nothing, because the application re-signs whatever it re-seals.
- [ ] Do not expose a read route for the trail from the host application.

## Logging

- [ ] JASIL logs through `logging.getLogger(__name__)` and configures nothing.
      Route the `jasil.*` loggers wherever your application logs go.
- [ ] Do not run production at `DEBUG`. The debug lines name every event
      published and every job batch claimed, and at `DEBUG` the `urllib3` logger
      writes each geocoding request line, with the coordinates and, for
      `geocode://`, the API key in its query string.
- [ ] Do not parse or alert on log *wording* — only levels and structured fields
      are stable. See [API stability](../api-stability.md).
- [ ] Wire `jasil.correlation.configure_provider` to your request-id middleware so
      events carry it.

## Shutdown

- [ ] Call `jasil.lifecycle.shutdown()` on the way down. It stops the durable-job
      worker before releasing the bus its subscribers publish through.
- [ ] Standalone workers must pass a stop event and set it from their signal
      handler so JASIL can record a graceful stop.
- [ ] Stop your own scheduler and dispose your own engine — JASIL created neither
      and does not touch them.

## Dependencies

- [ ] Install only the extras you use. A core install pulls `sqlalchemy` and
      `pydantic` and nothing else, and every backend client is imported lazily.
- [ ] Pin JASIL. While the version is `0.x`, a minor release may break the API.
- [ ] Verify the release artifact if you care about provenance — see
      [Verifying a release](https://github.com/endurain-project/jasil#verifying-a-release).
