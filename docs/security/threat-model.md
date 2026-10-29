# Threat model

What JASIL defends against, how, and — just as importantly — what it leaves to
you. A substrate sits underneath your domain code, so the boundary between "the
library handles this" and "you handle this" has to be explicit rather than
assumed.

JASIL's attack surface is narrow by design: it serves no requests, and it makes
exactly **two** kinds of outbound call to a third party: reverse geocoding, and —
only when configured — SMTP. Most of what follows is about
values flowing *through* it from your application.

## Server-side request forgery (CWE-918)

**Where.** Reverse geocoding is the capability that dials a host JASIL did
not choose and then returns its response (SMTP is covered under
[Outbound mail](#outbound-mail)). The host is operator-configured, which makes it attacker-influenced
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

## Outbound mail

**Header injection (CWE-93).** Addresses are parsed, never concatenated; the
`SMTP` email policy rejects CR/LF in a header value; and `MailMessage` has no
field through which a caller could set an arbitrary header.

**Transport.** TLS is verified by default; `smtp://` refuses a server that does
not offer STARTTLS rather than downgrading, and AUTH is sent only after TLS is
up. The one opt-out, `mail.secure = False`, is cleartext and is accepted only for
`smtp://`, only when the host resolves exclusively to private, loopback or
link-local addresses, and never with credentials. The check runs at startup and
again on every send, and the connection goes to the address that was checked
rather than to the name, so a DNS change cannot redirect cleartext mail to a
public host. Every
socket operation has a fixed timeout, and a whole send has a 90-second deadline
that a watchdog enforces by shutting the socket down — a per-read timeout alone
would let a relay that drips its replies hold a send open indefinitely. The
deadline covers the TLS handshake and every read after the connection is made;
the DNS lookup and the TCP connect (30 seconds per address) are bounded by their
own timeouts only.

**Personal data and secrets in logs and job rows.** The job runner persists
`str(error)` and logs `exc_info`, so `MailError` carries only a category and an
SMTP status code and is raised `from None`. Log lines record the recipient count,
`Message-ID` and outcome, never addresses, subjects or bodies. The password is
excluded from `repr(settings)`, and credentials are refused in the URI so they
cannot end up in a logged string.

**The SSRF address denylist is not applied to the SMTP host — a decision, not an
omission.** The denylist exists for geocoding because that backend fetches a
response from an operator-named host and hands it to the caller. A mail relay is
normally on a private network or loopback, and the SMTP backend returns nothing
from the server to its caller.

## Path traversal (CWE-22)

**Where.** Every storage call is addressed by a caller-supplied `(area, key)`
pair.

**Mitigations.**

- Both segments are validated before any filesystem or client access: empty,
  absolute, dot-component, parent-traversing, repeated-separator,
  trailing-separator, and backslash-bearing values raise `ValueError`. The check
  is pure and shared, so **both** backends enforce one canonical slash-delimited
  grammar. Without it, local paths normalize aliases such as `.` and `a//b`
  while S3 preserves them as distinct literal keys. An area is exactly one
  component, so `(area="a/b", key="c")` cannot collapse onto
  `(area="a", key="b/c")` in an object-store key.
- The local backend additionally resolves the final path and requires it to stay
  under the base directory, and never follows a symlink out of an area when
  listing. Logical objects use a reserved fixed-depth digest layout with
  verified identity sidecars, avoiding path-length and file/directory collisions
  between a key and its descendants without trusting the digest alone.
  Resumable parts live under a separate reserved private staging directory;
  session manifests and part files are validated before they are read, and
  symbolic-link aliases are rejected.
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
**verbatim** — to `jasil_event_outbox`, `jasil_processing_jobs`, and `jasil_event_log`, serialized
onto the Redis stream, and `metadata` is logged in full when a best-effort
subscriber fails.

**This is not mitigated, deliberately** — the substrate cannot know which of your
keys are sensitive. Carry identifiers, not secrets and not blobs. See
[Events & outbox](../events-and-outbox.md#what-not-to-put-in-one).

JASIL does not sanitise the values it logs. It bounds the *length* of the
identifiers it persists (`event_type`, `source`, `subscriber_id`), which are
developer-authored, but a `metadata` dict is yours and reaches the log intact.

The same holds for failure text. A handler's exception message is stored,
truncated to 4000 characters, in `jasil_processing_jobs.last_error` and, one row per
attempt, in `jasil_job_attempts.error`, and `jasil.admin.get_job` returns it. Do not
put a secret in an exception message, and keep `jasil.admin` behind the host's own
authorization.

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
- Streaming and resumable storage writes accept a host-owned `max_bytes` limit.
  Resumable parts stream under an exact declared size rather than being buffered
  in memory. JASIL's built-in portability limits cap parts at 5 GiB and part
  numbers at 10,000. Active parts remain backend resources until completion,
  abort, or explicit `cleanup_uploads` maintenance. Parallel part calls may
  temporarily stage more than the session limit because completion is the
  authoritative aggregate check. S3 cleanup follows JASIL-owned manifests and
  cannot abort unrelated multipart operations. Configure an S3
  incomplete-multipart lifecycle rule as a final safety net for a process death
  between native upload creation and manifest persistence.

What is **not** bounded: event `payload` and `metadata` size. A producer can
write an arbitrarily large payload into the outbox. JASIL also does not limit
the number of upload sessions a caller can begin; authenticate and rate-limit
that host endpoint, bound its request concurrency, retain session handles as
trusted server-side state, and run cleanup with a cutoff longer than a valid
upload.

## Duplicate execution

Delivery is **at-least-once**, never exactly-once. A durable subscriber can run
twice for the same event — after a lease expires, after a crash mid-batch, or on
a replay.

**Mitigations.** `(event_id, subscriber_id)` uniqueness makes the fan-out
idempotent; the claim is a compare-and-set so two workers cannot take the same
row; a running worker's heartbeat renews its leases, so a slow healthy handler is
not requeued and run a second time; and a handler whose lease is lost is
signalled (`jasil.jobs.lease_lost`) and its result discarded by the claim's
worker-id and attempt guard. What remains is a database partition, a handler
that ignores the signal mid-side-effect, and a handler abandoned past
`jobs.max_runtime_seconds`.

**Yours.** Subscriber handlers must be idempotent. If running one twice would
double-charge, double-send, or double-count, that is a correctness bug in the
handler, not in the queue.

## Tampering with the audit trail

**Guarded.** `jasil_audit_log` is append-only for every role but its owner: `UPDATE`, `DELETE` and `TRUNCATE` are
rejected by triggers, retention drops whole partitions through a `SECURITY DEFINER` function that refuses a cutoff
inside a floor fixed in the database, and the recommended grants give the application `INSERT` only. Signed,
hash-chained checkpoints over fixed windows, copied to a write-once sink, make a change by anyone else detectable
(`verify_checkpoints`). An audited change and its row share a transaction, so a change cannot happen unrecorded.

**Yours.** Nothing stops the table owner or a database superuser: they can disable the triggers and rewrite rows,
checkpoints and chain together, and if they delete a period's checkpoints the application re-seals and signs the
rewritten rows with its own key. Against them only the external sink holds the original checkpoints. Keep the sink
and the signing key outside the database operators' reach, and reconcile the sink with the database, and verify,
somewhere they do not control. See
[Audit trail → What this does not do](../audit.md#what-this-does-not-do).

## Out of scope

These are real risks that JASIL deliberately does not address, because it cannot:

| Risk | Why it is yours |
|---|---|
| Authentication and authorization on `jasil.admin` | It exposes operational data and a state-changing replay. JASIL has no notion of who is calling. |
| Serving and restricting local storage paths and URLs | They cannot expire or enforce response headers; JASIL neither runs that web server nor holds a key to sign with. Authorize every request, apply headers in the host response, and never expose a `ServeFile` filesystem path as a public URL. See [providers & backends](../providers-and-backends.md#storageprovider). |
| Namespacing keys and areas per tenant | The substrate sees opaque strings; only you know the tenancy boundary. |
| Securing the database and Redis themselves | Network policy, TLS, and credentials belong to the deployment. JASIL never creates the engine. |
| Rate limiting inbound requests | JASIL serves no requests. It provides the atomic primitives (`incr`, `set_if_absent`, `record_tiered_failure`) a limiter needs. |
| Vulnerabilities in an application *using* JASIL | Reported to that application. |

## Reporting

See [SECURITY.md](https://github.com/endurain-project/jasil/blob/main/SECURITY.md)
for the disclosure process and what is in scope.
