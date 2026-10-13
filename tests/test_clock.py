"""The system clock is the one capability with no alternative backend."""

import time
from datetime import UTC, datetime, timedelta

from jasil.backends.clock_system import SystemClock
from jasil.providers import ClockProvider


class TestSystemClock:
    def test_it_satisfies_the_protocol(self):
        assert isinstance(SystemClock(), ClockProvider)

    def test_now_is_timezone_aware_utc_wall_time(self):
        before = datetime.now(UTC)

        now = SystemClock().now()

        assert now.utcoffset() == timedelta(0)
        assert before <= now <= datetime.now(UTC)

    def test_monotonic_never_goes_backwards(self):
        clock = SystemClock()
        first = clock.monotonic()

        time.sleep(0.001)

        assert clock.monotonic() > first
