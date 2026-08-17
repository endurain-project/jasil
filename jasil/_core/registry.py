"""A tiny host-configuration slot backing JASIL's ``configure_*`` accessors.

JASIL takes several things from the host — a session factory, a settings object,
a correlation-id provider — through a uniform ``configure_* / get_*`` pair backed
by a process-wide singleton. This centralises the None-check-and-raise
boilerplate so each module does not restate it.

Two modes:

* **required** — construct with only a ``missing_message``. :meth:`get` raises
  :class:`RuntimeError` until :meth:`configure` installs a value.
* **defaulted** — construct with a ``default_factory``. :meth:`get` always
  returns a value and :meth:`reset` restores a freshly built default.

The slot is a plain attribute with no locking: the contract is that hosts call
``configure`` once at startup, before serving traffic.
"""

from collections.abc import Callable

__all__ = ["ConfigSlot"]


class ConfigSlot[T]:
    """A process-wide, host-configured singleton value."""

    def __init__(
        self,
        *,
        default_factory: Callable[[], T] | None = None,
        missing_message: str = "This JASIL component has not been configured.",
    ) -> None:
        """Create a slot.

        Args:
            default_factory: Builds the default value. When given, the slot is
                *defaulted* (never raises); when omitted, the slot is *required*.
            missing_message: Error raised by :meth:`get` on a required slot that
                has not been configured.
        """
        self._default_factory = default_factory
        self._missing_message = missing_message
        self._value: T | None = default_factory() if default_factory is not None else None

    def configure(self, value: T) -> None:
        """Install ``value`` for the process."""
        self._value = value

    def get(self) -> T:
        """Return the installed value.

        Raises:
            RuntimeError: If a required slot has not been configured.
        """
        value = self._value
        if value is None:
            raise RuntimeError(self._missing_message)
        return value

    def is_configured(self) -> bool:
        """Return whether a value is currently installed."""
        return self._value is not None

    def reset(self) -> None:
        """Restore the default (defaulted slot) or clear the value (required slot)."""
        self._value = self._default_factory() if self._default_factory is not None else None
