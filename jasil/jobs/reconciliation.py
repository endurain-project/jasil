"""The reconciliation-net contract every durable subscriber is held to.

A durable subscriber reacts to an event to derive state. Delivery is
at-least-once but never *guaranteed*: a Redis-Streams consumer can drop a
message, a provider can be briefly down, and some write paths publish no event at
all (a bulk import that persists rows directly). So a subscriber that writes
**durable** derived state must ship a scheduled backfill that re-derives whatever
the create path missed.

The vocabulary lives in the substrate rather than in whichever module happened to
need it first. A module owning the type would force every other module to import
it just to declare a net — a dependency between two bounded contexts for the sake
of a shared word. Owning it here is what lets every module declare its nets
without depending on any other, and what lets one conformance test
(:func:`assert_nets_complete`) hold them all to it.
"""

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from jasil.jobs.registry import JobHandlerRegistry


@dataclass(frozen=True)
class DurableSubscriberNet:
    """A durable subscriber's reconciliation net, or a documented exemption.

    Exactly one of ``backfill`` / ``exempt_reason`` is set. Neither is not an
    option: a subscriber with no net and no stated reason is one whose derived
    state silently goes missing, which is the failure this declaration exists to
    make impossible to introduce by omission.

    Attributes:
        subscriber_id: The stable durable-subscriber id (as registered on the
            :class:`jasil.jobs.registry.JobHandlerRegistry`).
        backfill: The scheduled, argument-free backfill that re-derives anything
            the create-path handler missed, or ``None`` when the subscriber is
            exempt (its derived state is transient / self-healing).
        exempt_reason: Why no backfill is required, when ``backfill`` is ``None``.
            Must be set for exempt subscribers and unset otherwise.
    """

    subscriber_id: str
    backfill: Callable[[], None] | None
    exempt_reason: str | None = None

    def __post_init__(self) -> None:
        """
        Reject a declaration that sets both fields, or neither.

        Returns:
            None.

        Raises:
            ValueError: When ``backfill`` and ``exempt_reason`` are both set or
                both unset.
        """
        if self.backfill is not None and self.exempt_reason is not None:
            raise ValueError(
                f"durable subscriber {self.subscriber_id!r} declares both a backfill and an exemption; "
                "an exemption means there is nothing to run"
            )
        if self.backfill is None and self.exempt_reason is None:
            raise ValueError(
                f"durable subscriber {self.subscriber_id!r} declares no reconciliation net; "
                "give it a backfill, or an exempt_reason saying why its derived state needs none"
            )


def undeclared_subscribers(
    nets: Iterable[DurableSubscriberNet],
    *,
    registry: JobHandlerRegistry,
) -> frozenset[str]:
    """
    Return the registered subscriber ids that no net accounts for.

    Args:
        nets: The reconciliation nets declared across every module.
        registry: The registry holding the durable subscribers to check —
            normally the process-wide one, with every subscriber module imported.

    Returns:
        The subscriber ids present in the registry but absent from ``nets``.
        Empty when every durable subscriber is accounted for.
    """
    return registry.subscriber_ids() - {net.subscriber_id for net in nets}


def assert_nets_complete(
    nets: Iterable[DurableSubscriberNet],
    *,
    registry: JobHandlerRegistry,
) -> None:
    """
    Fail unless every registered durable subscriber declares a net.

    Args:
        nets: The reconciliation nets declared across every module.
        registry: The registry holding the durable subscribers to check.

    Returns:
        None.

    Raises:
        AssertionError: When a registered subscriber declares no net. This is a
            host's conformance test in one call, where an assertion is the idiom.
    """
    missing = undeclared_subscribers(nets, registry=registry)
    if missing:
        raise AssertionError(
            "durable subscribers with no declared reconciliation net: "
            + ", ".join(sorted(missing))
            + " — declare a DurableSubscriberNet with a backfill, or an exempt_reason"
        )
