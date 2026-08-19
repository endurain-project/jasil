"""The async bridge: running async I/O from synchronous callers.

The whole point of this module is thread-crossing — a sync route on Starlette's
threadpool, or an in-process subscriber running inline on the publisher's thread,
handing work to the one loop that owns the resources. So the tests drive it from
a real second thread against a real loop rather than mocking ``asyncio``; a
mocked version would prove nothing about the case it exists for.

Nothing here sleeps: every wait is on an event with a timeout.
"""

import asyncio
import inspect
import logging
import threading

import pytest

import jasil.async_bridge as async_bridge

# Generous upper bound on a loop hop; every wait fails the test rather than hanging.
WAIT_TIMEOUT = 5.0


@pytest.fixture(autouse=True)
def _clear_main_loop():
    """The loop handle is process-wide, so no test may leak one to the next."""
    async_bridge.set_main_loop(None)
    yield
    async_bridge.set_main_loop(None)


@pytest.fixture
def running_loop():
    """A real event loop running on its own thread, as in a live application."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="test-main-loop", daemon=True)
    thread.start()
    try:
        yield loop
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=WAIT_TIMEOUT)
        loop.close()


class _CapturingHandler(logging.Handler):
    """Signals when a record arrives, so a test can wait instead of polling.

    The failure log is written by a done-callback on the *loop* thread, which may
    run after ``future.result()`` has already returned — waiting on the record
    itself is the only race-free way to assert on it.
    """

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []
        self.received = threading.Event()

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.received.set()


@pytest.fixture
def bridge_logs():
    handler = _CapturingHandler()
    logger = logging.getLogger("jasil.async_bridge")
    logger.addHandler(handler)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)


class TestMainLoopHandle:
    def test_no_loop_is_registered_by_default(self):
        assert async_bridge.get_main_loop() is None

    async def test_capturing_records_the_running_loop(self):
        async_bridge.capture_running_loop()

        assert async_bridge.get_main_loop() is asyncio.get_running_loop()

    def test_setting_none_clears_it(self, running_loop):
        async_bridge.set_main_loop(running_loop)

        async_bridge.set_main_loop(None)

        assert async_bridge.get_main_loop() is None


class TestDispatch:
    def test_a_coroutine_submitted_from_another_thread_runs_on_the_main_loop(self, running_loop):
        """The motivating case: sync code handing async I/O to the owning loop."""
        async_bridge.set_main_loop(running_loop)
        ran_on = {}

        async def work():
            ran_on["loop"] = asyncio.get_running_loop()
            return 42

        future = async_bridge.dispatch(work())

        assert future is not None
        assert future.result(timeout=WAIT_TIMEOUT) == 42
        assert ran_on["loop"] is running_loop

    def test_it_returns_immediately_without_awaiting(self, running_loop):
        """Fire-and-forget: the caller must not block on the dispatched work."""
        async_bridge.set_main_loop(running_loop)
        release = threading.Event()

        async def work():
            await asyncio.get_running_loop().run_in_executor(None, release.wait, WAIT_TIMEOUT)

        future = async_bridge.dispatch(work())

        assert future is not None
        assert not future.done()
        release.set()
        future.result(timeout=WAIT_TIMEOUT)

    def test_a_failing_coroutine_is_logged_rather_than_surfaced(self, running_loop, bridge_logs):
        """A dropped websocket must not break the synchronous work that triggered it."""
        async_bridge.set_main_loop(running_loop)

        async def work():
            raise RuntimeError("delivery failed")

        async_bridge.dispatch(work())

        assert bridge_logs.received.wait(timeout=WAIT_TIMEOUT)
        record = bridge_logs.records[-1]
        assert record.levelno == logging.ERROR
        assert "RuntimeError" in record.getMessage()

    def test_dispatching_with_no_loop_drops_the_coroutine(self, bridge_logs):
        async def work():
            return None

        coro = work()

        assert async_bridge.dispatch(coro) is None
        assert bridge_logs.records[-1].levelno == logging.WARNING

    def test_a_dropped_coroutine_is_closed(self):
        """Left un-closed it would emit a 'never awaited' warning at collection."""

        async def work():
            return None

        coro = work()

        async_bridge.dispatch(coro)

        assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED

    def test_a_closed_loop_is_treated_as_absent(self, bridge_logs):
        """Shutdown order is not guaranteed; dispatching into a dead loop must not raise."""
        loop = asyncio.new_event_loop()
        loop.close()
        async_bridge.set_main_loop(loop)

        async def work():
            return None

        coro = work()

        assert async_bridge.dispatch(coro) is None
        assert inspect.getcoroutinestate(coro) == inspect.CORO_CLOSED
