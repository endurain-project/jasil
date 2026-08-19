"""The reconciliation-net contract, and the conformance check that enforces it.

Delivery is at-least-once but not guaranteed, so a subscriber writing durable
derived state must ship a backfill. The point of these tests is that the gap is
found by a failing check rather than by a user noticing missing data months later.
"""

import pytest

from jasil.jobs.reconciliation import (
    DurableSubscriberNet,
    assert_nets_complete,
    undeclared_subscribers,
)
from jasil.jobs.registry import JobHandlerRegistry


@pytest.fixture
def registry():
    return JobHandlerRegistry()


class TestDurableSubscriberNet:
    def test_a_backfill_is_a_valid_net(self):
        net = DurableSubscriberNet("invoice.render", backfill=lambda: None)

        assert net.exempt_reason is None

    def test_a_documented_exemption_is_a_valid_net(self):
        net = DurableSubscriberNet("cache.warm", backfill=None, exempt_reason="derived state is transient")

        assert net.backfill is None

    def test_declaring_neither_is_refused(self):
        """A subscriber with no net and no stated reason is the failure this
        type exists to make impossible to introduce by omission."""
        with pytest.raises(ValueError, match="declares no reconciliation net"):
            DurableSubscriberNet("invoice.render", backfill=None)

    def test_declaring_both_is_refused(self):
        with pytest.raises(ValueError, match="declares both a backfill and an exemption"):
            DurableSubscriberNet("invoice.render", backfill=lambda: None, exempt_reason="also exempt?")


class TestUndeclaredSubscribers:
    def test_a_registered_subscriber_with_no_net_is_reported(self, registry):
        registry.register("order.created", "invoice.render", lambda _e: None)

        assert undeclared_subscribers([], registry=registry) == {"invoice.render"}

    def test_a_declared_subscriber_is_accounted_for(self, registry):
        registry.register("order.created", "invoice.render", lambda _e: None)
        nets = [DurableSubscriberNet("invoice.render", backfill=lambda: None)]

        assert undeclared_subscribers(nets, registry=registry) == frozenset()

    def test_every_event_type_is_covered(self, registry):
        """Subscriber ids are collected across event types, not per type \u2014 a net
        declared for one channel must not excuse a subscriber on another."""
        registry.register("order.created", "invoice.render", lambda _e: None)
        registry.register("order.shipped", "notify.customer", lambda _e: None)

        assert undeclared_subscribers([], registry=registry) == {"invoice.render", "notify.customer"}

    def test_a_net_for_an_unregistered_subscriber_is_not_an_error(self, registry):
        """Nets are declared at import time and registration may be conditional;
        an unused net is stale, not a missing safety net."""
        nets = [DurableSubscriberNet("invoice.render", backfill=lambda: None)]

        assert undeclared_subscribers(nets, registry=registry) == frozenset()


class TestAssertNetsComplete:
    def test_a_fully_declared_registry_passes(self, registry):
        registry.register("order.created", "invoice.render", lambda _e: None)
        nets = [DurableSubscriberNet("invoice.render", backfill=lambda: None)]

        assert_nets_complete(nets, registry=registry)

    def test_an_empty_registry_passes(self, registry):
        assert_nets_complete([], registry=registry)

    def test_a_missing_net_fails_and_names_the_subscriber(self, registry):
        registry.register("order.created", "invoice.render", lambda _e: None)
        registry.register("order.shipped", "notify.customer", lambda _e: None)

        with pytest.raises(AssertionError, match=r"invoice\.render, notify\.customer"):
            assert_nets_complete([], registry=registry)
