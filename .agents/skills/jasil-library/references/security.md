# Security

JASIL runs inside a host's trust boundary; `docs/security/threat-model.md` and `docs/security/integration-checklist.md` are authoritative. Update them when a change moves the boundary.

## Secrets and credentials
- No hardcoded secrets; settings carry them and the host supplies them.
- Never log or put in an exception message a password, token, API key, SMTP credential or connection string with credentials.
- A rejected mail configuration must not reuse earlier credentials.

## Outbound requests
- Treat any URL from settings or users as untrusted; geocoding and other HTTP backends go through the vendored `jasil._core` network checks (host, port, scheme allow-listing, SSRF).
- Do not call `requests` or `httpx` directly from a port or the core.
- Set timeouts on every network call; a backend never waits unbounded.

## Data and injection
- Parameterized queries or ORM expressions only; no string-built SQL.
- Event payloads and job arguments are JSON; never `pickle`.
- Validate and bound untrusted input: event type, id, source and queue names have maximum lengths (`MAX_*` constants) that must be enforced, not assumed.

## Concurrency and shared state
- State shared across processes (locks, rate counters, leases) goes through the state, lock and event providers, never a module-level dict or lock.
- Process-local backends (`memory://`, `noop://`) are refused where the topology says multi-process; do not weaken `check_deployment_consistency`.
- Never hold a database transaction across a network, storage or mail call.

## Resource limits
- Bound every batch, retry count, payload size and fan-out; a durable job has bounded attempts, a lease and a timeout.
- A new background loop or worker is released by `jasil.lifecycle.shutdown` and never starts at import time.

When you add a mitigation, state what it protects against; when you flag a vulnerability, explain the risk.
