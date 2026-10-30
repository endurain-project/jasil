"""The helpers behind the library's daemon threads: winding one down, and pacing its retries."""

import threading

import pytest

from jasil._core.threads import backoff_delay, signal_and_join


class TestSignalAndJoin:
    def test_a_finished_thread_is_joined(self):
        stop = threading.Event()
        thread = threading.Thread(target=stop.wait)
        thread.start()

        assert signal_and_join(thread, stop) is True
        assert not thread.is_alive()

    def test_no_thread_only_sets_the_event(self):
        stop = threading.Event()

        assert signal_and_join(None, stop) is True
        assert stop.is_set()

    def test_a_thread_that_will_not_stop_is_reported(self):
        release = threading.Event()
        thread = threading.Thread(target=release.wait, daemon=True)
        thread.start()
        try:
            assert signal_and_join(thread, threading.Event(), timeout=0.01) is False
        finally:
            release.set()
            thread.join()

    def test_stopping_from_the_thread_being_stopped_does_not_join_itself(self):
        """A handler running on the thread may stop its own worker: the loop ends once the handler returns."""
        stop = threading.Event()
        outcome: list[object] = []

        def handler():
            try:
                outcome.append(signal_and_join(threading.current_thread(), stop))
            except Exception as error:  # the bug being pinned: RuntimeError("cannot join current thread")
                outcome.append(error)

        thread = threading.Thread(target=handler)
        thread.start()
        thread.join()

        assert outcome == [True]
        assert stop.is_set()


class TestBackoffDelay:
    def test_the_delay_grows_and_is_capped_with_jitter(self):
        delays = [backoff_delay(failures, base=1.0, maximum=8.0) for failures in range(100)]

        assert all(0.5 <= delay <= 8.0 for delay in delays)
        assert max(delays[10:]) > 4.0
        assert backoff_delay(0, base=1.0, maximum=8.0) <= 1.0

    @pytest.mark.parametrize("failures", [0, 1, 2, 3])
    def test_each_failure_waits_up_to_twice_as_long(self, failures):
        delay = backoff_delay(failures, base=1.0, maximum=100.0)

        assert 0.5 * 2**failures <= delay <= 2**failures
