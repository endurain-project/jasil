# Security Policy

## Supported versions

JASIL is at `0.x`. Security fixes are released against the latest published
version only; there are no maintained release branches yet. Once `1.0.0` ships,
this section will state a support window.

| Version | Supported |
|---|---|
| `0.1.x` | Yes |
| `< 0.1` | No |

## Scope

In scope: anything in the `jasil` package. That includes the SSRF guard on
outbound hosts, the path-traversal checks in the local storage backend, and any
way a host application's data could be exposed, corrupted, or lost through
JASIL's own code.

Out of scope: vulnerabilities in an application that *uses* JASIL but stem from
its own code or configuration — for example allow-listing an internal host via
`NetworkSettings.ssrf_allowed_hosts` and then dialing it. Report those to that
application's maintainers.

Vulnerabilities in a dependency should be reported upstream first. If JASIL's use
of it makes the impact materially worse, report that here too.

## Reporting a vulnerability

1. **Do not** open a public issue.
2. Email <joao@endurain.com> with the details.
3. Include:
   - steps to reproduce;
   - the affected version;
   - potential impact;
   - any suggested fix, if you have one.
4. You will get an acknowledgement when possible.

Please include as much detail as you can — it is what makes a report actionable
rather than a starting point for investigation.

## What to expect

This project is maintained by one person in their spare time, so response times
vary. A fix will be released as soon as it is ready, and the advisory will credit
you unless you would rather stay anonymous.

Thank you for helping keep this project secure.
