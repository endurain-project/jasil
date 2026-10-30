"""The ambient audit context: who is acting, through what, and from where.

Modelled on :mod:`jasil.correlation`. The host sets the context once per request (or per job or CLI
command) and every :func:`~jasil.audit.stage` / :func:`~jasil.audit.record_detached` in that scope
reads it, so call sites name only the action and the target::

    import jasil.audit as jasil_audit

    jasil_audit.set_context(jasil_audit.AuditContext(actor_type="user", actor_id="42", source="api"))

    with jasil_audit.audit_context(actor_type="system", source="worker"):
        ...

A host that already carries this information installs a reader instead with
:func:`configure_provider`. ``request_id`` is deliberately absent: it is always read from
:mod:`jasil.correlation`, so there is one source of truth for it.
"""

import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any

from jasil._core.limits import (
    MAX_AUDIT_ID_LENGTH,
    MAX_AUDIT_NAME_LENGTH,
    MAX_IP_ADDRESS_LENGTH,
    MAX_USER_AGENT_LENGTH,
    fit_length,
)
from jasil.events import MAX_EVENT_ID_LENGTH

logger = logging.getLogger(__name__)

__all__ = [
    "MAX_USER_AGENT_LENGTH",
    "ActorType",
    "AuditContext",
    "Source",
    "audit_context",
    "configure_provider",
    "get_context",
    "reset",
    "set_context",
]


class ActorType(StrEnum):
    """What kind of principal performed an action."""

    USER = "user"
    API_KEY = "api_key"
    SYSTEM = "system"
    ANONYMOUS = "anonymous"


class Source(StrEnum):
    """Which part of the host performed an action."""

    API = "api"
    WORKER = "worker"
    CLI = "cli"
    MIGRATION = "migration"


@dataclass(frozen=True)
class AuditContext:
    """Who is acting, and from where. Every field is optional; the defaults describe an anonymous API call.

    Attributes:
        actor_type: The kind of principal.
        actor_id: The principal's id; ``None`` for an anonymous or system actor.
        credential_type: How the principal authenticated (e.g. ``password``, ``api_key``).
        credential_id: The id of that credential, never its value.
        on_behalf_of_id: The principal the actor is acting for, when different (impersonation,
            delegation).
        source: Which part of the host is running.
        ip_address: The client address. Clipped to 45 characters.
        user_agent: The client's ``User-Agent``. Clipped to :data:`MAX_USER_AGENT_LENGTH`
            characters, with a trailing ``...`` marking the cut.
        event_id: The domain event that triggered the work, when it runs in a handler.
    """

    actor_type: ActorType = ActorType.ANONYMOUS
    actor_id: str | None = None
    credential_type: str | None = None
    credential_id: str | None = None
    on_behalf_of_id: str | None = None
    source: Source = Source.API
    ip_address: str | None = None
    user_agent: str | None = None
    event_id: str | None = None

    def __post_init__(self) -> None:
        """Coerce the enums and clip the client-controlled strings.

        Raises:
            ValueError: When ``actor_type`` or ``source`` is not a known value, or an id exceeds its column or contains a NUL character.
        """
        object.__setattr__(self, "actor_type", ActorType(self.actor_type))
        object.__setattr__(self, "source", Source(self.source))
        # A NUL cannot be stored in PostgreSQL text; a client controls these two, so they are repaired, not refused.
        for name, limit in (("user_agent", MAX_USER_AGENT_LENGTH), ("ip_address", MAX_IP_ADDRESS_LENGTH)):
            value = getattr(self, name)
            object.__setattr__(self, name, fit_length(value.replace("\x00", "\ufffd") if value else value, limit))
        for name, limit in (
            ("actor_id", MAX_AUDIT_ID_LENGTH),
            ("credential_type", MAX_AUDIT_NAME_LENGTH),
            ("credential_id", MAX_AUDIT_ID_LENGTH),
            ("on_behalf_of_id", MAX_AUDIT_ID_LENGTH),
            ("event_id", MAX_EVENT_ID_LENGTH),
        ):
            value = getattr(self, name)
            if value is not None and len(value) > limit:
                raise ValueError(f"audit context {name} is {len(value)} characters; the limit is {limit}")
            if value is not None and "\x00" in value:
                raise ValueError(f"audit context {name} must not contain a NUL character")


_context: ContextVar[AuditContext | None] = ContextVar("jasil_audit_context", default=None)

# Set only by audit_context(): an explicit scope, which wins over an installed provider.
_override: ContextVar[AuditContext | None] = ContextVar("jasil_audit_context_override", default=None)

_provider: Callable[[], AuditContext | None] | None = None


def configure_provider(provider: Callable[[], AuditContext | None] | None) -> None:
    """Install a host callable that returns the current audit context.

    Call once at startup. Passing ``None`` restores the built-in contextvar.

    Args:
        provider: Returns the ambient context, or ``None`` when there is none.
    """
    global _provider
    _provider = provider


def set_context(context: AuditContext | None) -> None:
    """Set the audit context for the current execution context.

    Only consulted while no provider is installed via :func:`configure_provider` and no
    :func:`audit_context` block is open. Each thread and each
    asyncio task has its own value, so setting it in one request never leaks into another.

    Args:
        context: The context to read from now on, or ``None`` to clear it.
    """
    _context.set(context)


def get_context() -> AuditContext:
    """Return the ambient context, or the anonymous default when there is none.

    Never raises: a provider that fails is logged at ``WARNING`` by exception type and treated as "no
    context", because the row is still worth writing without it.
    """
    override = _override.get()
    if override is not None:
        return override
    if _provider is not None:
        try:
            provided = _provider()
        except Exception as error:
            # The type only: the host's message may name the user the context is about.
            logger.warning(
                "audit context provider raised %s; the row is written without its context", type(error).__name__
            )
            provided = None
        return provided if provided is not None else AuditContext()
    return _context.get() or AuditContext()


@contextmanager
def audit_context(**fields: Any) -> Generator[AuditContext]:
    """Run a block under a context built on top of the current one, restoring it afterwards.

    For workers and CLI commands, which have no request middleware to set the context for them. The
    block's context wins over an installed provider, so a scheduled job can attribute its rows to the
    system even in a host whose provider only knows about requests.

    Args:
        **fields: :class:`AuditContext` fields to override.

    Yields:
        The context in force inside the block.

    Raises:
        ValueError: When a field value is invalid.
        TypeError: When a field name is not an :class:`AuditContext` field.
    """
    context = replace(get_context(), **fields)
    token = _override.set(context)
    try:
        yield context
    finally:
        _override.reset(token)


def reset() -> None:
    """Clear the installed provider and the current context's value. For tests."""
    global _provider
    _provider = None
    _context.set(None)
    _override.set(None)
