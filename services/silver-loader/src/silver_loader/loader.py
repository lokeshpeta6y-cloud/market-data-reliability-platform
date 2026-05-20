"""
SilverLoader — owns the in-memory event buffer and coordinates flushes to Snowflake.

Buffering strategy
------------------
- Events are buffered until either:
    a) ``batch_size`` is reached (size-triggered flush), or
    b) ``flush_interval_seconds`` has elapsed since the last flush (time-triggered,
       driven externally by the background thread in main.py).
- ``process(event)`` adds to the buffer and returns the flush count (0 if not
  flushed, >0 if a size-triggered flush occurred).
- ``flush()`` drains the current buffer regardless of size.
- Thread safety: a ``threading.Lock`` guards the buffer so that the main consume
  loop and the background flush timer do not race.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from mdrp_common.logging import get_logger
from mdrp_common.models import CurveEvent

from .snowflake_client import SnowflakeClient, SnowflakeLoadError

logger = get_logger(__name__)


def _event_to_dict(event: CurveEvent) -> dict[str, Any]:
    """
    Convert a CurveEvent to a plain dict suitable for JSON serialisation and
    Snowflake ingestion.  Decimal prices are converted to strings to preserve
    precision; datetimes are formatted as ISO-8601 with timezone.
    """
    return {
        "event_id": event.event_id,
        "source_event_id": event.source_event_id,
        "curve_name": event.curve_name,
        "instrument": event.instrument,
        "tenor": event.tenor,
        "delivery_period": event.delivery_period.value,
        "price": str(event.price),  # Decimal → string preserves precision
        "currency": event.currency,
        "unit": event.unit,
        "provider": event.provider,
        "version": event.version,
        "event_timestamp": event.event_timestamp.isoformat(),
        "ingestion_timestamp": event.ingestion_timestamp.isoformat(),
        "quality_score": event.quality_score,
        "is_replay": event.is_replay,
        "replay_source": event.replay_source.value if event.replay_source else None,
        "trace_id": event.trace_id,
    }


class SilverLoader:
    """
    Buffers CurveEvent objects and bulk-loads them to Snowflake Silver.

    Parameters
    ----------
    snowflake_client:
        A configured SnowflakeClient.  Pass ``None`` when Snowflake is not
        configured — flushes will be no-ops (logged as warnings).
    batch_size:
        Maximum number of events to buffer before a size-triggered flush.
    flush_interval_seconds:
        Used only for logging/metrics context; the actual timer is driven
        externally by the background thread in ``main.py``.
    """

    def __init__(
        self,
        snowflake_client: SnowflakeClient | None,
        batch_size: int = 1000,
        flush_interval_seconds: float = 60.0,
    ) -> None:
        self._client = snowflake_client
        self._batch_size = batch_size
        self._flush_interval = flush_interval_seconds
        self._buffer: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush_at: float = time.monotonic()
        self._total_loaded: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def buffer_size(self) -> int:
        """Current number of buffered events (thread-safe read)."""
        with self._lock:
            return len(self._buffer)

    def process(self, event: CurveEvent) -> int:
        """
        Add *event* to the buffer.

        If the buffer has reached ``batch_size`` after this addition, a
        size-triggered flush is executed inline.

        Returns the number of rows loaded (0 if no flush occurred).
        """
        record = _event_to_dict(event)
        with self._lock:
            self._buffer.append(record)
            should_flush = len(self._buffer) >= self._batch_size

        if should_flush:
            return self.flush()
        return 0

    def flush(self) -> int:
        """
        Drain the current buffer to Snowflake.

        Returns the number of rows loaded.  Returns 0 without error when:
        - the buffer is empty, or
        - Snowflake is not configured (client is None).
        """
        with self._lock:
            if not self._buffer:
                return 0
            batch = self._buffer.copy()
            self._buffer.clear()
            self._last_flush_at = time.monotonic()

        if self._client is None:
            logger.warning(
                "snowflake_not_configured_skipping_flush",
                batch_size=len(batch),
                layer="silver",
            )
            return 0

        try:
            rows_loaded = self._client.load_batch(batch)
        except SnowflakeLoadError as exc:
            # Put the records back so they can be retried on the next flush.
            # We log the error but do NOT crash — the caller (main.py) will
            # decide whether to skip the Kafka offset commit.
            logger.error(
                "silver_flush_failed_restoring_buffer",
                batch_size=len(batch),
                error=str(exc),
            )
            with self._lock:
                # Prepend so ordering is preserved
                self._buffer = batch + self._buffer
            raise

        self._total_loaded += rows_loaded
        logger.info(
            "silver_flush_complete",
            rows_loaded=rows_loaded,
            batch_size=len(batch),
            total_loaded=self._total_loaded,
        )
        return rows_loaded

    def close(self) -> None:
        """Flush remaining buffer and close the Snowflake connection."""
        remaining = self.buffer_size
        if remaining > 0:
            logger.info("silver_loader_draining_buffer", remaining=remaining)
            try:
                self.flush()
            except SnowflakeLoadError as exc:
                logger.error(
                    "silver_loader_drain_failed",
                    error=str(exc),
                    records_lost=remaining,
                )
        if self._client is not None:
            self._client.close()
