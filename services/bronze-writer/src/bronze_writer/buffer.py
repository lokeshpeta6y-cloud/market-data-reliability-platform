"""Thread-safe in-memory event buffer with size and time-based flush triggers."""

from __future__ import annotations

import threading
import time
from typing import Any


class EventBuffer:
    """Thread-safe accumulator for raw event dicts; flushes on size or elapsed time."""

    def __init__(self, batch_size: int = 500, flush_interval_seconds: float = 30.0) -> None:
        self._batch_size = batch_size
        self._flush_interval = flush_interval_seconds
        self._lock = threading.Lock()
        self._events: list[dict[str, Any]] = []
        self._last_flush_time: float = time.monotonic()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, event: dict[str, Any]) -> None:
        """Append a serialised event dict (from model_dump) to the buffer."""
        with self._lock:
            self._events.append(event)

    def should_flush(self) -> bool:
        """Return True if the buffer has reached the size or age threshold."""
        with self._lock:
            if len(self._events) == 0:
                return False
            if len(self._events) >= self._batch_size:
                return True
            elapsed = time.monotonic() - self._last_flush_time
            return elapsed >= self._flush_interval

    def drain(self) -> list[dict[str, Any]]:
        """Atomically remove and return all buffered events, resetting the flush timer."""
        with self._lock:
            batch = self._events
            self._events = []
            self._last_flush_time = time.monotonic()
            return batch

    def size(self) -> int:
        """Return the current number of buffered events."""
        with self._lock:
            return len(self._events)

    def age_seconds(self) -> float:
        """Return seconds elapsed since the last flush (or since creation)."""
        with self._lock:
            return time.monotonic() - self._last_flush_time
