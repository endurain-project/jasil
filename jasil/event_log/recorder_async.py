"""The async event-log recorder — the asynchronous twin of :mod:`jasil.event_log.recorder`.

Implements the :class:`~jasil.providers_async.AsyncEventRecorder` protocol. The
async composition root injects one instance into the async event bus when
``event_log.enabled`` is set. Each write opens its own short-lived
``AsyncSession`` and swallows/logs any storage error, so a database hiccup never
breaks event processing — observability is best-effort by design, in both faces.

The one structural difference from the sync recorder is ``track``: it is an
``asynccontextmanager``, so the handlers it wraps are awaited inside the
``yield``. That keeps the *ordering* guarantee the ``record_processing=False``
optimisation depends on — the ``published -> completed`` pair still brackets the
handler run, and the intermediate ``processing`` row is still skippable for the
in-process bus, where it would never be observed.
"""

import logging
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession

import jasil.event_log.crud_async as event_log_crud_async
import jasil.orm as jasil_orm
from jasil.events import Event

logger = logging.getLogger(__name__)


class AsyncEventLogRecorder:
    """Records event lifecycle transitions to the ``event_log`` table, asynchronously."""

    async def record_published(self, event: Event) -> None:
        """
        Record the initial ``published`` row for an event.

        Args:
            event: The event envelope being published.

        Returns:
            None.
        """
        await self._safely("record_published", lambda db: event_log_crud_async.record_published(event, db))

    async def record_queued(self, event: Event) -> None:
        """
        Record a terminal ``queued`` row for an event routed to durable jobs.

        Args:
            event: The event envelope staged for durable delivery.

        Returns:
            None.
        """
        await self._safely("record_queued", lambda db: event_log_crud_async.record_queued(event, db))

    @asynccontextmanager
    async def track(
        self,
        event: Event,
        *,
        worker_id: str,
        handler_name: str | None,
        record_processing: bool = True,
    ) -> AsyncIterator[None]:
        """
        Record ``processing`` then ``completed`` / ``failed`` around handlers.

        Args:
            event: The event being processed.
            worker_id: The process/consumer handling the event.
            handler_name: The subscriber(s) that process the event.
            record_processing: Whether to write the intermediate ``processing``
                row. False for the in-process bus, where the row goes
                published -> completed within one call and the intermediate state
                is never observed — saving a database round-trip on the hot path.

        Yields:
            None — control returns to the caller to await the handlers.

        Raises:
            Exception: Re-raises any handler exception after recording failure.
        """
        if record_processing:
            await self._safely(
                "mark_processing",
                lambda db: event_log_crud_async.mark_processing(event.event_id, worker_id, db),
            )
        start = time.monotonic()
        try:
            yield
        except Exception as error:
            error_message = str(error)
            elapsed_ms = int((time.monotonic() - start) * 1000)
            await self._safely(
                "mark_failed",
                lambda db: event_log_crud_async.mark_failed(
                    event.event_id, handler_name, error_message, elapsed_ms, db
                ),
            )
            raise
        elapsed_ms = int((time.monotonic() - start) * 1000)
        await self._safely(
            "mark_completed",
            lambda db: event_log_crud_async.mark_completed(event.event_id, handler_name, elapsed_ms, db),
        )

    async def _safely(self, operation: str, write: Callable[[AsyncSession], Awaitable[None]]) -> None:
        """
        Run a recording write, swallowing and logging any storage error.

        Args:
            operation: Label for the write, used in the log message.
            write: Callable returning the awaitable that performs the write.

        Returns:
            None.
        """
        try:
            async with jasil_orm.get_async_sessionmaker()() as db:
                await write(db)
        except Exception as error:  # observability must never break event processing
            logger.warning("event_log %s failed: %r", operation, error)
