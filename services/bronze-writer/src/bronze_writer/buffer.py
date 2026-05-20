"""
Thread-safe in-memory event buffer for the bronze-writer service.

EventBuffer accumulates RawMarketEvent dicts until either:
  - ``batch_size`` events have been added, OR
  - ``flush_interval_seconds`` have elapsed since the last flush.

The buffer is protected by a threading.Lock so that the consumer thread
and any background flush thread can both call it without races.

Design notes
------------
- Events are stored as plain dicts (model_dump output) rather than Pydantic
  models so they can be serialised directly to Parquet via pandas/pyarrow
  without a second round-trip through Pydantic.
- ``drain()`` atomically replaces the internal list with an empty list and
  resets the flush timer, so the caller owns the drained batch exclusively.
- ``should_flush()`` is intentionally a pure predicate — it does not mutate
  state.  The caller decides when to act on it.
"""

from __future__ import annotations

import threading
import time
from typing import Any


class EventBuffer:
    """
    Thread-safe accumulator for raw event dicts.

    Parameters
    ----------
    batch_size:
        Maximum number of events before ``should_flush()`` returns True.
    flush_interval_seconds:
        Maximum age of the oldest buffered event (in seconds) before
        ``should_flush()`` returns True.
    """

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
        """
        Add a serialised event dict to the buffer.

        Parameters
        ----------
        event:
            Plain dict representation of a RawMarketEvent (from model_dump).
        """
        with self._lock:
            self._events.append(event)

    def should_flush(self) -> bool:
        """
        Return True if the buffer should be flushed.

        Thread-safe read — acquires the lock briefly to inspect state.
        """
        with self._lock:
            if len(self._events) == 0:
                return False
            if len(self._events) >= self._batch_size:
                return True
            elapsed = time.monotonic() - self._last_flush_time
            return elapsed >= self._flush_interval

    def drain(self) -> list[dict[str, Any]]:
        """
        Atomically remove and return all buffered events.

        Resets the flush timer so the interval starts fresh from this point.
        Returns an empty list if the buffer is currently empty.

        Returns
        -------
        list[dict[str, Any]]
            All events that were buffered at the moment of the call.
            The internal buffer is empty after this returns.
        """
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
