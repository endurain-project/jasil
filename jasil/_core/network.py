"""SSRF guard for operator-configured outbound hosts.

Vendored from Endurain's ``core.network``, trimmed to the outbound half. The
inbound half (proxy-aware client-IP extraction, ``TRUSTED_PROXIES`` resolution)
is a web-framework concern and deliberately did not come across: JASIL serves no
requests, and keeping it out is what lets this module stay free of ``fastapi``.

The allowlist is passed in rather than read from a global, so the host owns the
policy and the guard stays a pure function.
"""

import ipaddress
import logging
import re
import socket
from collections.abc import Sequence

logger = logging.getLogger(__name__)

# RFC 1123 hostname syntax: labels of 1-63 alphanumeric/hyphen characters,
# separated by dots. Hyphens may not start or end a label.
_HOSTNAME_RE = re.compile(
    r"^(?:[a-z0-9](?:[a-z0-9\-]{0,61}[a-z0-9])?)"  # first label
    r"(?:\.(?:[a-z0-9](?:[a-z0-9\-]{0,61}[a-z0-9])?))*$",  # more
    re.IGNORECASE,
)

# Reasons a destination is refused. Phrased to read correctly after a host value.
_UNRESOLVABLE = "hostname could not be resolved"
_UNPARSEABLE = "resolves to an unparseable address"
_NON_PUBLIC = "resolves to a non-public address"
_NOT_AN_AUTHORITY = "is not a bare host[:port] authority"


def _is_valid_hostname(value: str) -> bool:
    """Return True when ``value`` is a syntactically valid RFC 1123 hostname.

    Rejects values carrying URL schemes, ports, or other non-hostname characters
    (e.g. ``caddy:8080`` or ``http://caddy``).

    Args:
        value: Candidate hostname to validate.

    Returns:
        True when ``value`` conforms to RFC 1123 hostname syntax.
    """
    return len(value) <= 253 and _HOSTNAME_RE.match(value) is not None


def _is_private_or_reserved(addr: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Return True if ``addr`` belongs to any non-routable range.

    Combines every "do not dial" predicate ``ipaddress`` exposes: private
    (RFC1918, fc00::/7), loopback, link-local, multicast, unspecified, and
    reserved. Any of these would let an attacker pivot to internal
    infrastructure or a cloud metadata service.
    """
    return (
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
        or addr.is_unspecified
        or addr.is_reserved
    )


def _split_allowlist(
    allowed_hosts: Sequence[str],
) -> tuple[frozenset[str], tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]]:
    """Classify allowlist entries into hostnames and IP networks."""
    hosts: set[str] = set()
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for entry in allowed_hosts:
        try:
            networks.append(ipaddress.ip_network(entry, strict=False))
        except ValueError:
            hosts.add(entry.lower())
    return frozenset(hosts), tuple(networks)


def _is_allowlisted(
    hostname: str,
    addr: ipaddress.IPv4Address | ipaddress.IPv6Address,
    allowed_hosts: Sequence[str],
) -> bool:
    """Return True if ``hostname`` or ``addr`` is allowlisted.

    Only consulted when the resolved address would otherwise be rejected. Both
    the hostname (exact, case-insensitive) and the resolved IP (CIDR membership)
    are checked, so an operator can opt in by either dimension.
    """
    hosts, networks = _split_allowlist(allowed_hosts)
    if hostname.lower() in hosts:
        return True
    return any(addr in network for network in networks)


def _address_rejection_reason(
    hostname: str,
    *,
    allowed_hosts: Sequence[str],
    purpose: str | None = None,
) -> str | None:
    """Return why ``hostname`` must not be dialed, or None when every address is safe.

    Resolves every A/AAAA record and requires all of them to be public unicast. A
    single private/loopback/link-local answer rejects the host — this defends
    against DNS rebinding, where an attacker-controlled name returns a public IP
    on the first lookup and a private IP on the next.

    Args:
        hostname: The hostname to resolve, without a port.
        allowed_hosts: Hostnames and CIDRs exempt from the address denylist.
        purpose: Optional short tag identifying the outbound call, used only for
            audit logging.

    Returns:
        A reason string, or ``None`` when the host may be dialed.
    """
    try:
        infos = socket.getaddrinfo(hostname, None)
    except socket.gaierror:
        return _UNRESOLVABLE

    for info in infos:
        ip_text = info[4][0]
        try:
            addr = ipaddress.ip_address(ip_text)
        except ValueError:
            # Defensive: a resolver answer we cannot parse is treated as unsafe.
            return _UNPARSEABLE
        if _is_private_or_reserved(addr):
            if _is_allowlisted(hostname, addr, allowed_hosts):
                # Audit trail: every allowlisted private destination is logged so
                # operators can review what the exception is being used for.
                logger.info(
                    "SSRF allowlist hit: dialing private address %s for host %s (purpose=%s)",
                    ip_text,
                    hostname,
                    purpose or "unspecified",
                )
                continue
            return _NON_PUBLIC
    return None


def host_rejection_reason(
    host: str | None,
    *,
    allowed_hosts: Sequence[str] = (),
    purpose: str | None = None,
) -> str | None:
    """Return why an operator-configured ``host[:port]`` must not be dialed, or None.

    Returns a reason rather than raising, because callers handed a host from
    configuration must *degrade* (disable the feature) rather than fail.

    The value must be a plain ``host[:port]``. One carrying a scheme, path, or
    credentials would otherwise be interpolated into a URL by the caller and
    silently redirect the request elsewhere — ``"evil.example.com/x"`` becomes
    ``"https://evil.example.com/x/reverse"``, whose hostname check passes.

    This is a *time-of-check* guard. Callers wanting full TOCTOU safety should
    also pin the resolved public IP and dial it directly with the original Host
    header.

    Args:
        host: The configured host authority, or ``None``.
        allowed_hosts: Hostnames and CIDRs exempt from the address denylist, for
            reaching a self-hosted service on a private network.
        purpose: Optional short tag identifying the outbound call, used only for
            audit logging.

    Returns:
        A short human-readable reason, or ``None`` when the host may be dialed.
    """
    if host is None:
        return _NOT_AN_AUTHORITY

    hostname, separator, port = host.rpartition(":")
    if not separator:
        hostname = host
    elif not (port.isdigit() and len(port) <= 5):
        return _NOT_AN_AUTHORITY

    if not _is_valid_hostname(hostname):
        return _NOT_AN_AUTHORITY

    return _address_rejection_reason(hostname, allowed_hosts=allowed_hosts, purpose=purpose)
