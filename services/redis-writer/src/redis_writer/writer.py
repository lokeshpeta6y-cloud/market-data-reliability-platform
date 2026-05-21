"""
RedisWriter — orchestrates CurveStore operations and emits observability signals.

Responsibilities
----------------
* Call CurveStore.update_tenor() for each incoming CurveEvent.
* After every write, check for stale instruments and emit structured warnings.
* Record Prometheus metrics for every processed event.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

from mdrp_common.logging import get_logger
from mdrp_common.metrics import (
    EVENT_PROCESSING_LATENCY_SECONDS,
    EVENTS_NORMALIZED_TOTAL,
    QUALITY_SCORE,
)
from mdrp_common.models import CurveEvent
from redis_writer.curve_store import CurveStore

log = get_logger(__name__)

_SERVICE_NAME = "redis-writer"

# How many events to process between staleness checks (avoid Redis SCAN on every message)
_STALENESS_CHECK_INTERVAL = 100


class RedisWriter:
    """
    Orchestrates persistence of CurveEvents into Redis.

    Parameters
    ----------
    curve_store:
        CurveStore instance that owns all Redis I/O.
    """

    def __init__(self, curve_store: CurveStore) -> None:
        self._store = curve_store
        self._events_since_staleness_check: int = 0

    def handle_event(self, event: CurveEvent) -> None:
        """
        Persist *event* to Redis and update observability signals.

        Steps:
        1. Write tenor data to Redis via CurveStore.
        2. Emit Prometheus metrics.
        3. Periodically check for stale instruments.
        """
        start = time.perf_counter()

        try:
            self._store.update_tenor(event)
        except Exception as exc:
            log.error(
                "redis_write_failed",
                curve_name=event.curve_name,
                tenor=event.tenor,
                provider=event.provider,
                instrument=event.instrument,
                error=str(exc),
            )
            raise

        elapsed = time.perf_counter() - start

        # Latency from event origin to now
        processing_latency = (
            datetime.now(UTC) - event.event_timestamp
        ).total_seconds()

        # Prometheus
        EVENTS_NORMALIZED_TOTAL.labels(
            provider=event.provider,
            instrument=event.instrument,
        ).inc()

        QUALITY_SCORE.labels(provider=event.provider).observe(event.quality_score)

        EVENT_PROCESSING_LATENCY_SECONDS.labels(
            service=_SERVICE_NAME,
            provider=event.provider,
        ).observe(processing_latency)

        log.info(
            "curve_event_written",
            curve_name=event.curve_name,
            tenor=event.tenor,
            instrument=event.instrument,
            provider=event.provider,
            quality_score=event.quality_score,
            version=event.version,
            write_duration_ms=round(elapsed * 1000, 2),
        )

        # Periodic staleness check
        self._events_since_staleness_check += 1
        if self._events_since_staleness_check >= _STALENESS_CHECK_INTERVAL:
            self._check_staleness()
            self._events_since_staleness_check = 0

    def check_staleness_now(self) -> None:
        """Force an immediate staleness check (called on shutdown, etc.)."""
        self._check_staleness()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _check_staleness(self) -> None:
        """
        Scan for stale instruments and emit structured WARNING log records.

        These warnings are the primary signal consumed by the ops API for
        alerting purposes.
        """
        try:
            stale_instruments = self._store.get_stale_instruments()
        except Exception as exc:
            log.warning("staleness_check_failed", error=str(exc))
            return

        now_ts = time.time()
        for instrument in stale_instruments:
            # Re-fetch last event timestamp for the structured log fields
            snapshot = self._store.get_snapshot(instrument)
            last_event_at: str | None = None
            staleness_seconds: float | None = None

            if snapshot is not None:
                last_event_at = snapshot.as_of.isoformat()
                staleness_seconds = round(now_ts - snapshot.as_of.timestamp(), 1)

            log.warning(
                "instrument_data_stale",
                instrument=instrument,
                last_event_at=last_event_at,
                staleness_seconds=staleness_seconds,
            )
