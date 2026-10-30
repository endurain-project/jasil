"""System ``ClockProvider`` backend."""

import time
from datetime import UTC, datetime


class SystemClock:
    """``ClockProvider`` backed by the system clock (UTC wall time + monotonic)."""

    def now(self) -> datetime:
        """Return the current UTC time as a timezone-aware datetime."""
        return datetime.now(UTC)

    def monotonic(self) -> float:
        """Return ``time.monotonic()``."""
        return time.monotonic()
