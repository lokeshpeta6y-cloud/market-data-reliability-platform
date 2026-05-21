"""
Databento Historical Replayer.

Pulls historical market data from the Databento API for a specified time window
and re-publishes it to the REPLAY_EVENTS topic.

This module is a no-op when:
  - The ``databento`` Python package is not installed, OR
  - ``DATABENTO_API_KEY`` is not set in the environment.

Databento records are converted to RawMarketEvent with
``is_replay=True`` and ``replay_source=ReplaySource.DATABENTO_HISTORICAL``.

Databento SDK note
------------------
Databento's Python SDK (``databento``) uses synchronous I/O.  We run the
download in a thread-pool executor to keep the engine's main loop non-blocking,
but BronzeReplayer and DLQReplayer are also synchronous so the engine calls them
directly in the worker thread anyway — consistency is more important here.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from typing import Any

from mdrp_common.kafka_client import MdrpProducer, Topics
from mdrp_common.logging import get_logger
from mdrp_common.metrics import REPLAY_EVENTS_TOTAL
from mdrp_common.models import RawMarketEvent, ReplayJob, ReplaySource

log = get_logger(__name__)

# Lazily import databento so the service starts even without the package.
try:
    import databento as _db  # type: ignore[import-untyped]

    _DATABENTO_AVAILABLE = True
except ImportError:
    _db = None  # type: ignore[assignment]
    _DATABENTO_AVAILABLE = False


class DatabentoReplayer:
    """
    Historical market data replay via the Databento REST/streaming API.

    Parameters
    ----------
    api_key:
        Databento API key.  Must not be None.
    dataset:
        Databento dataset identifier (e.g. ``"DBEQ.BASIC"``).
    rate_limit_per_second:
        Maximum events per second to publish to Kafka.
    """

    def __init__(
        self,
        api_key: str,
        dataset: str = "DBEQ.BASIC",
        rate_limit_per_second: int = 1000,
    ) -> None:
        if not _DATABENTO_AVAILABLE:
            raise RuntimeError(
                "databento package is not installed. "
                "Install it with: pip install databento"
            )
        self._api_key = api_key
        self._dataset = dataset
        self._rate_limit = rate_limit_per_second
        self._client = _db.Historical(api_key=api_key)

    @classmethod
    def is_available(cls) -> bool:
        """Return True if the Databento SDK is installed."""
        return _DATABENTO_AVAILABLE

    def replay(self, job: ReplayJob, producer: MdrpProducer) -> int:
        """
        Fetch historical data from Databento and publish to REPLAY_EVENTS.

        Parameters
        ----------
        job:
            Replay job; ``provider`` is treated as a Databento publisher
            (e.g. ``"XNAS"``), ``instrument`` as the symbol.
        producer:
            Kafka producer for REPLAY_EVENTS.

        Returns
        -------
        int
            Total events published.
        """
        symbols = [job.instrument] if job.instrument else None
        log.info(
            "databento_replay_start",
            job_id=job.job_id,
            dataset=self._dataset,
            symbols=symbols,
            start_time=job.start_time.isoformat(),
            end_time=job.end_time.isoformat(),
        )

        try:
            data = self._fetch_data(
                symbols=symbols,
                start=job.start_time,
                end=job.end_time,
            )
        except Exception as exc:
            log.error(
                "databento_fetch_failed",
                job_id=job.job_id,
                error=str(exc),
            )
            raise

        total_events = 0
        rate_start = time.monotonic()
        events_in_window = 0

        for record in data:
            event = self._convert_record(record, job)
            producer.produce(
                topic=Topics.REPLAY_EVENTS,
                value=event,
                key=f"{event.provider}:{event.instrument}",
            )

            total_events += 1
            events_in_window += 1
            REPLAY_EVENTS_TOTAL.labels(
                source=ReplaySource.DATABENTO_HISTORICAL.value
            ).inc()

            if events_in_window >= 100:
                elapsed = time.monotonic() - rate_start
                expected = events_in_window / self._rate_limit
                if expected > elapsed:
                    time.sleep(expected - elapsed)
                events_in_window = 0
                rate_start = time.monotonic()

        producer.flush()
        log.info(
            "databento_replay_complete",
            job_id=job.job_id,
            total_events=total_events,
        )
        return total_events

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _fetch_data(
        self,
        symbols: list[str] | None,
        start: datetime,
        end: datetime,
    ) -> Any:
        """
        Call the Databento timeseries API and return an iterable of records.

        We use ``schema="trades"`` as a sensible default.  The dataset and
        schema can be made configurable via settings if needed.
        """
        params: dict[str, Any] = {
            "dataset": self._dataset,
            "schema": "trades",
            "start": start.isoformat(),
            "end": end.isoformat(),
        }
        if symbols:
            params["symbols"] = ",".join(symbols)

        # get_range returns a DBNStore which is iterable
        return self._client.timeseries.get_range(**params)

    def _convert_record(self, record: Any, job: ReplayJob) -> RawMarketEvent:
        """
        Convert a Databento record to a RawMarketEvent.

        Databento records have a ``ts_event`` nanosecond timestamp and
        various schema-specific fields.  We store all record attributes
        in ``payload`` for downstream consumers.
        """
        # ts_event is nanoseconds since epoch
        ts_ns: int = getattr(record, "ts_event", 0)
        event_ts = datetime.fromtimestamp(ts_ns / 1e9, tz=UTC)

        provider = job.provider or self._dataset
        instrument = (
            getattr(record, "symbol", None)
            or getattr(record, "instrument_id", None)
            or job.instrument
            or "unknown"
        )

        # Build a payload dict from all public attributes of the record
        payload: dict[str, Any] = {}
        for attr in dir(record):
            if attr.startswith("_"):
                continue
            try:
                val = getattr(record, attr)
                if callable(val):
                    continue
                # Ensure JSON-serialisable
                if isinstance(val, (int, float, str, bool, type(None))):
                    payload[attr] = val
                else:
                    payload[attr] = str(val)
            except Exception:
                pass

        return RawMarketEvent(
            event_id=str(uuid.uuid4()),
            provider=str(provider),
            instrument=str(instrument),
            received_at=datetime.now(UTC),
            event_timestamp=event_ts,
            payload=payload,
            is_replay=True,
            replay_source=ReplaySource.DATABENTO_HISTORICAL,
            trace_id=str(uuid.uuid4()),
        )
