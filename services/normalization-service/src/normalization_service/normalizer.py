"""
Event normaliser for the Market Data Reliability Platform.

Orchestrates tenor parsing, instrument mapping, quality scoring, and Redis-backed
version counters to produce a canonical CurveEvent from a ValidatedMarketEvent.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import redis

from mdrp_common.logging import get_logger
from mdrp_common.models import (
    CurveEvent,
    DeliveryPeriod,
    FaultType,
    ValidatedMarketEvent,
)

from normalization_service.instrument_mapper import InstrumentMapper
from normalization_service.tenor_mapper import TenorMapper

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Quality-score fault penalties
# ---------------------------------------------------------------------------

_FAULT_PENALTIES: dict[FaultType, float] = {
    FaultType.DELAYED: 0.05,
    FaultType.SCHEMA_DRIFT: 0.20,
    FaultType.STALE: 0.30,
    FaultType.PARTIAL_CURVE: 0.25,
    FaultType.OUT_OF_ORDER: 0.10,
}

# Redis key prefix for curve version counters
_VERSION_KEY_PREFIX = "norm:version:"


# ---------------------------------------------------------------------------
# Normalizer
# ---------------------------------------------------------------------------


class Normalizer:
    """
    Transforms a :class:`ValidatedMarketEvent` into a :class:`CurveEvent`.

    All heavy lifting is delegated to :class:`TenorMapper` and
    :class:`InstrumentMapper`.  Version numbers are sourced from a Redis INCR
    counter keyed on ``norm:version:{curve_name}``.

    Parameters
    ----------
    redis_client:
        A connected ``redis.Redis`` instance.  The caller owns the connection
        lifecycle.
    version_counter_ttl:
        Optional TTL (seconds) applied to Redis version keys on first creation.
        ``0`` means no expiry (default).
    """

    def __init__(
        self,
        redis_client: redis.Redis[Any],
        version_counter_ttl: int = 0,
    ) -> None:
        self._redis = redis_client
        self._ttl = version_counter_ttl
        self._tenor_mapper = TenorMapper()
        self._instrument_mapper = InstrumentMapper()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def normalise(self, event: ValidatedMarketEvent) -> CurveEvent | None:
        """
        Normalise *event* into a :class:`CurveEvent`.

        Returns ``None`` when the event cannot be normalised (unrecognised
        instrument or missing price).  The caller should skip such events.
        """
        # --- Instrument ---
        try:
            canonical_instrument, currency, unit = self._instrument_mapper.normalise(
                event.instrument
            )
        except ValueError:
            log.warning(
                "normalisation_skipped_unknown_instrument",
                event_id=event.event_id,
                provider=event.provider,
                raw_instrument=event.instrument,
            )
            return None

        # --- Price ---
        raw_price = event.payload.get("price")
        if raw_price is None:
            log.warning(
                "normalisation_skipped_missing_price",
                event_id=event.event_id,
                provider=event.provider,
                instrument=canonical_instrument,
            )
            return None

        try:
            price = Decimal(str(raw_price))
        except Exception:
            log.warning(
                "normalisation_skipped_invalid_price",
                event_id=event.event_id,
                provider=event.provider,
                instrument=canonical_instrument,
                raw_price=raw_price,
            )
            return None

        # --- Tenor ---
        raw_tenor: str = str(event.payload.get("tenor", ""))
        if not raw_tenor:
            # Fall back to the instrument field — some providers omit tenor
            raw_tenor = event.instrument

        try:
            canonical_tenor, delivery_period = self._tenor_mapper.normalise(raw_tenor)
        except ValueError:
            log.warning(
                "normalisation_skipped_unknown_tenor",
                event_id=event.event_id,
                provider=event.provider,
                instrument=canonical_instrument,
                raw_tenor=raw_tenor,
            )
            return None

        # --- Curve name ---
        curve_name = _build_curve_name(canonical_instrument, delivery_period)

        # --- Quality score ---
        quality_score = _compute_quality_score(event.injected_faults)

        # --- Version (Redis INCR, atomic) ---
        version = self._next_version(curve_name)

        log.debug(
            "event_normalised",
            source_event_id=event.event_id,
            curve_name=curve_name,
            tenor=canonical_tenor,
            delivery_period=delivery_period.value,
            quality_score=quality_score,
            version=version,
        )

        return CurveEvent(
            source_event_id=event.event_id,
            curve_name=curve_name,
            instrument=canonical_instrument,
            tenor=canonical_tenor,
            delivery_period=delivery_period,
            price=price,
            currency=currency,
            unit=unit,
            provider=event.provider,
            version=version,
            event_timestamp=event.event_timestamp,
            ingestion_timestamp=datetime.now(UTC),
            quality_score=quality_score,
            is_replay=event.is_replay,
            replay_source=event.replay_source,
            trace_id=event.trace_id,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _next_version(self, curve_name: str) -> int:
        """
        Atomically increment and return the Redis version counter for
        *curve_name*.  On first use, the key is created with value 1.
        If *version_counter_ttl* > 0, TTL is set only when the counter
        is first created (INCR returns 1).
        """
        key = f"{_VERSION_KEY_PREFIX}{curve_name}"
        version: int = self._redis.incr(key)  # type: ignore[assignment]
        if version == 1 and self._ttl > 0:
            self._redis.expire(key, self._ttl)
        return version


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _build_curve_name(instrument: str, delivery_period: DeliveryPeriod) -> str:
    """Return the canonical curve name, e.g. ``TTF_MONTHLY_FWD``."""
    return f"{instrument}_{delivery_period.value.upper()}_FWD"


def _compute_quality_score(faults: list[FaultType]) -> float:
    """
    Compute the quality score for an event given its injected faults.

    Starts at 1.0 and subtracts a fixed penalty per fault type (each fault
    type is counted once regardless of how many times it appears).
    The result is clamped to [0.0, 1.0].
    """
    if not faults:
        return 1.0

    # Deduplicate so a repeated fault is penalised only once
    unique_faults = set(faults)
    score = 1.0
    for fault in unique_faults:
        score -= _FAULT_PENALTIES.get(fault, 0.0)

    return max(0.0, round(score, 10))
