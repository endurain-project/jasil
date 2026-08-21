# Threat model

What JASIL defends against, how, and — just as importantly — what it leaves to
you. A substrate sits underneath your domain code, so the boundary between "the
library handles this" and "you handle this" has to be explicit rather than
assumed.

JASIL's attack surface is narrow by design: it serves no requests, and it makes
exactly **one** outbound call to a third party. Most of what follows is about
values flowing *through* it from your application.

## Server-side request forgery (CWE-918)

**Where.** Reverse geocoding is the only capability that dials a host JASIL did
not choose. The host is operator-configured, which makes it attacker-influenced
whenever configuration is.

**Mitigations.**

- The configured value must be a bare `host[:port]`. One carrying a scheme,
  path, or credentials is refused, because it would otherwise be interpolated
  into a URL — `evil.example.com/x` becomes `https://evil.example.com/x/reverse`,
  whose hostname check passes.
- Every A/AAAA record the name resolves to must be public unicast. A single
  private, loopback, link-local, multicast, unspecified, or reserved answer
  rejects the host. Requiring *all* answers is what defeats a name that returns
  a public address on the first lookup and a private one on the next.
- Redirects are refused on every request, so a permitted host cannot `3xx`-pivot
  onto an internal target.
- The response body is read under a 1 MiB cap, streamed, so a hostile upstream
  cannot exhaust memory.
- Failures are logged by exception type and status code only. The `requests`
  error message embeds the request URL, and one supported service carries its
  API key in that URL's query string.

**Escape hatch.** `NetworkSettings.ssrf_allowed_hosts` exempts hostnames and
CIDRs so a self-hosted instance on a private network stays reachable. Every use
is logged. Prefer a CIDR: a *hostname* entry exempts every address that name
resolves to — including a cloud metadata endpoint — and taking one logs at
`WARNING` saying so.

**Limits you should know.**

- The check is **time-of-check, performed once**, while the platform is
  assembled. A name that resolves publicly at startup and privately afterwards
  is not caught. What *is* caught is the case that matters for an
  operator-supplied setting: a host that was already internal when configured.
- `socket.getaddrinfo` takes no timeout, so resolution is bounded by your
  resolver configuration rather than by JASIL. At startup this shows up as a
  slow boot rather than a hung request.

## Path traversal (CWE-22)

**Where.** Every storage call is addressed by a caller-supplied `(area, key)`
pair.

**Mitigations.**

- Both segments are validated before any filesystem or client access: empty,
  absolute (`/` or `\`), and `..`-bearing values raise `ValueError`. The check is
  pure and shared, so **both** backends enforce it — the S3 backend needs it
  because `..` is a literal character in an object key, so an unchecked value is
  silently stored under a nonsense key rather than refused.
- The local backend additionally resolves the final path and requires it to stay
  under the base directory, and never follows a symlink out of an area when
  listing.
- URLs percent-encode the area and key, so a key holding `?`, `#`, or `%` cannot
  alter the URL it lands in.

A conformance suite runs the same rules against both backends, so they cannot
drift apart.

## Pattern injection into the state store (CWE-77)

**Where.** `StateProvider.delete_prefix` and `iter_keys` take a literal key
prefix. Redis matches `SCAN` patterns as globs, so a prefix is not a literal to
it.

**Mitigation.** Glob metacharacters (`\ * ? [ ]`) are escaped before the prefix
becomes a pattern. Unescaped, `delete_prefix("*")` would empty the keyspace and
`iter_keys("user:[1]")` would match keys the caller never named while missing the
one it did — while the in-memory backend, which compares with `startswith`,
matched only the literal.

If you build a prefix from a tenant or user identifier, you do not control the
characters in it. This is the mitigation that makes that safe.

## Secrets in transit through the pipeline (CWE-532)

**Where.** An event's `payload` and `metadata` are stored and transmitted
**verbatim** — to `event_outbox`, `processing_jobs`, and `event_log`, serialized
onto the Redis stream, and `metadata` is logged in full when a best-effort
subscriber fails.

**This is not mitigated, deliberately** — the substrate cannot know which of your
keys are sensitive. Carry identifiers, not secrets and not blobs. See
[Events & outbox](../events-and-outbox.md#what-not-to-put-in-one).

JASIL does not sanitise the values it logs. It bounds the *length* of the
identifiers it persists (`event_type`, `source`, `subscriber_id`), which are
developer-authored, but a `metadata` dict is yours and reaches the log intact.

## SQL injection (CWE-89)

**Not a surface.** Every query is SQLAlchemy Core or ORM with bound parameters.
The only raw SQL in the package is the two PostgreSQL advisory-lock statements,
which pass their key as a bound `:key` parameter. Table and column names are
never interpolated from input.

## Resource exhaustion (CWE-400)

- Retention pruning deletes in bounded batches with a cap on batches per pass, so
  a backlog cannot hold locks on a hot table.
- Job claims and outbox relays are batch-size limited.
- Stored failure text is truncated; the joined subscriber list is clamped to its
  column width.
- The geocoding response body is capped (above).

What is **not** bounded: event `payload` and `metadata` size. A producer can
write an arbitrarily large payload into the outbox.

## Duplicate execution

Delivery is **at-least-once**, never exactly-once. A durable subscriber can run
twice for the same event — after a lease expires, after a crash mid-batch, or on
a replay.

**Mitigations.** `(event_id, subscriber_id)` uniqueness makes the fan-out
idempotent; the claim is a compare-and-set so two workers cannot take the same
row; and a worker's identity carries a digest when a long hostname forces
truncation, so two machines never collapse onto one lease holder.

**Yours.** Subscriber handlers must be idempotent. If running one twice would
double-charge, double-send, or double-count, that is a correctness bug in the
handler, not in the queue.

## Out of scope

These are real risks that JASIL deliberately does not address, because it cannot:

| Risk | Why it is yours |
|---|---|
| Authentication and authorization on `jasil.admin` | It exposes operational data and a state-changing replay. JASIL has no notion of who is calling. |
| Serving and restricting local storage URLs | They cannot expire — JASIL neither runs that web server nor holds a key to sign with. See [providers & backends](../providers-and-backends.md#storageprovider). |
| Namespacing keys and areas per tenant | The substrate sees opaque strings; only you know the tenancy boundary. |
| Securing the database and Redis themselves | Network policy, TLS, and credentials belong to the deployment. JASIL never creates the engine. |
| Rate limiting inbound requests | JASIL serves no requests. It provides the atomic primitives (`incr`, `set_if_absent`, `record_tiered_failure`) a limiter needs. |
| Vulnerabilities in an application *using* JASIL | Reported to that application. |

## Reporting

See [SECURITY.md](https://github.com/endurain-project/jasil/blob/main/SECURITY.md)
for the disclosure process and what is in scope.
