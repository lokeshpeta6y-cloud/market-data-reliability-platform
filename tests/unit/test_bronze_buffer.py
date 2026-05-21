"""Unit tests for bronze_writer.buffer.EventBuffer."""

import threading
import time

import pytest

from bronze_writer.buffer import EventBuffer


def _event(provider: str = "test", n: int = 0) -> dict:
    return {"provider": provider, "event_id": f"id-{n}", "payload": {}}


class TestAdd:
    def test_size_increments(self) -> None:
        buf = EventBuffer(batch_size=10)
        buf.add(_event())
        assert buf.size() == 1

    def test_multiple_adds(self) -> None:
        buf = EventBuffer(batch_size=10)
        for i in range(5):
            buf.add(_event(n=i))
        assert buf.size() == 5


class TestShouldFlush:
    def test_empty_buffer_never_flushes(self) -> None:
        buf = EventBuffer(batch_size=2, flush_interval_seconds=60)
        assert buf.should_flush() is False

    def test_flushes_at_batch_size(self) -> None:
        buf = EventBuffer(batch_size=3, flush_interval_seconds=60)
        for i in range(3):
            buf.add(_event(n=i))
        assert buf.should_flush() is True

    def test_does_not_flush_below_batch_size(self) -> None:
        buf = EventBuffer(batch_size=10, flush_interval_seconds=60)
        buf.add(_event())
        assert buf.should_flush() is False

    def test_flushes_on_interval(self) -> None:
        buf = EventBuffer(batch_size=1000, flush_interval_seconds=0.05)
        buf.add(_event())
        time.sleep(0.1)
        assert buf.should_flush() is True


class TestDrain:
    def test_drain_returns_all_events(self) -> None:
        buf = EventBuffer()
        events = [_event(n=i) for i in range(5)]
        for e in events:
            buf.add(e)
        result = buf.drain()
        assert len(result) == 5

    def test_drain_empties_buffer(self) -> None:
        buf = EventBuffer()
        buf.add(_event())
        buf.drain()
        assert buf.size() == 0

    def test_drain_empty_buffer_returns_empty_list(self) -> None:
        buf = EventBuffer()
        result = buf.drain()
        assert result == []

    def test_drain_resets_flush_timer(self) -> None:
        buf = EventBuffer(batch_size=1000, flush_interval_seconds=0.05)
        buf.add(_event())
        time.sleep(0.1)
        buf.drain()
        # After drain the timer resets; a fresh event should not trigger flush
        buf.add(_event())
        assert buf.should_flush() is False


class TestThreadSafety:
    def test_concurrent_adds_are_safe(self) -> None:
        buf = EventBuffer(batch_size=10_000)
        errors: list[Exception] = []

        def add_many() -> None:
            try:
                for i in range(500):
                    buf.add(_event(n=i))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=add_many) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert buf.size() == 5000
