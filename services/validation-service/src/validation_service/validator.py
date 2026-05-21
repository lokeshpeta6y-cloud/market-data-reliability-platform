"""
Core validation logic for the validation-service.

ValidationService.validate() accepts a RawMarketEvent and runs all validation
rules in order.  It returns a two-element tuple:

    (ValidatedMarketEvent, None)  — event passed all rules
    (None, DLQEvent)              — event failed a rule and must be dead-lettered
    (None, None)                  — event was a duplicate (silent discard)

Rules are applied in this order:
    1. Schema validation       — required fields present
    2. Type validation         — payload is dict, price is numeric
    3. Duplicate detection     — Redis SETNX
    4. Timestamp bounds        — within [-24h, +5min] of now
    5. Price sanity            — 0 < price < 1_000_000
    6. Provider quality score  — computed and stored in Redis

On DLQ, the full original event dict is embedded in DLQEvent.raw_payload so
that forensic replay can reconstruct the original message exactly.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from mdrp_common.logging import get_logger, set_trace_id
from mdrp_common.metrics import (
    DLQ_EVENTS_TOTAL,
    EVENT_PROCESSING_LATENCY_SECONDS,
    EVENTS_DEDUPLICATED_TOTAL,
    EVENTS_VALIDATED_TOTAL,
    QUALITY_SCORE,
    VALIDATION_ERRORS_TOTAL,
)
from mdrp_common.models import (
    DLQEvent,
    DLQFailureCategory,
    RawMarketEvent,
    ValidatedMarketEvent,
)

from .deduplicator import Deduplicator
from .quality_scorer import QualityScorer
from .settings import ValidationServiceSettings

logger = get_logger(__name__)

# Type alias for clarity
_ValidationResult = tuple[ValidatedMarketEvent | None, DLQEvent | None]


class ValidationService:
    """
    Stateful validation service.  Holds references to Redis-backed helpers.

    Parameters
    ----------
    settings:
        Fully populated ValidationServiceSettings instance.
    deduplicator:
        Redis-backed Deduplicator instance.
    quality_scorer:
        Redis-backed QualityScorer instance.
    """

    def __init__(
        self,
        settings: ValidationServiceSettings,
        deduplicator: Deduplicator,
        quality_scorer: QualityScorer,
    ) -> None:
        self._settings = settings
        self._deduplicator = deduplicator
        self._quality_scorer = quality_scorer

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(self, event: RawMarketEvent) -> _ValidationResult:
        """
        Run all validation rules against *event* and return the outcome.

        Returns
        -------
        (ValidatedMarketEvent, None)
            Event passed validation; caller should produce to VALIDATED_EVENTS.
        (None, DLQEvent)
            Event failed a rule; caller should produce to DLQ_EVENTS.
        (None, None)
            Event is a duplicate; caller should silently discard.
        """
        # Propagate trace_id through the log context for this call
        set_trace_id(event.trace_id)

        # Serialise the raw event once for DLQ embedding
        raw_payload = event.model_dump(mode="python")

        # ------------------------------------------------------------------
        # Rule 1: Schema validation — required top-level fields
        # ------------------------------------------------------------------
        required_fields = ("event_id", "provider", "instrument", "event_timestamp", "payload")
        missing = [f for f in required_fields if not getattr(event, f, None)]
        if missing:
            reason = f"Missing required fields: {', '.join(missing)}"
            logger.warning(
                "validation_failed_missing_fields",
                provider=event.provider,
                event_id=event.event_id,
                missing_fields=missing,
            )
            VALIDATION_ERRORS_TOTAL.labels(
                provider=event.provider, error_type="missing_required_field"
            ).inc()
            return None, self._make_dlq(
                event, raw_payload, reason, DLQFailureCategory.MISSING_REQUIRED_FIELD
            )

        # ------------------------------------------------------------------
        # Rule 2: Type validation — payload dict, price numeric
        # ------------------------------------------------------------------
        type_error = self._check_types(event)
        if type_error:
            logger.warning(
                "validation_failed_type_error",
                provider=event.provider,
                event_id=event.event_id,
                error=type_error,
            )
            VALIDATION_ERRORS_TOTAL.labels(
                provider=event.provider, error_type="schema_violation"
            ).inc()
            return None, self._make_dlq(
                event, raw_payload, type_error, DLQFailureCategory.SCHEMA_VIOLATION
            )

        # ------------------------------------------------------------------
        # Rule 3: Duplicate detection
        # ------------------------------------------------------------------
        if self._deduplicator.is_duplicate(event.event_id):
            logger.info(
                "event_deduplicated",
                provider=event.provider,
                event_id=event.event_id,
            )
            EVENTS_DEDUPLICATED_TOTAL.labels(provider=event.provider).inc()
            EVENTS_VALIDATED_TOTAL.labels(provider=event.provider, outcome="deduplicated").inc()
            return None, None  # Silent discard

        # ------------------------------------------------------------------
        # Rule 4: Timestamp bounds
        # ------------------------------------------------------------------
        ts_error, ts_category = self._check_timestamp(event.event_timestamp)
        if ts_error:
            logger.warning(
                "validation_failed_timestamp",
                provider=event.provider,
                event_id=event.event_id,
                category=ts_category,
                event_timestamp=event.event_timestamp.isoformat(),
            )
            VALIDATION_ERRORS_TOTAL.labels(
                provider=event.provider, error_type=ts_category.value
            ).inc()
            return None, self._make_dlq(event, raw_payload, ts_error, ts_category)

        # ------------------------------------------------------------------
        # Rule 5: Price sanity
        # ------------------------------------------------------------------
        price_error = self._check_price(event.payload)
        if price_error:
            logger.warning(
                "validation_failed_price",
                provider=event.provider,
                event_id=event.event_id,
                error=price_error,
            )
            VALIDATION_ERRORS_TOTAL.labels(provider=event.provider, error_type="malformed").inc()
            return None, self._make_dlq(
                event, raw_payload, price_error, DLQFailureCategory.MALFORMED
            )

        # ------------------------------------------------------------------
        # Rule 6: Provider quality scoring
        # ------------------------------------------------------------------
        quality_score = self._quality_scorer.score_event(event.provider, event.injected_faults)
        QUALITY_SCORE.labels(provider=event.provider).observe(quality_score)

        # ------------------------------------------------------------------
        # All rules passed — build ValidatedMarketEvent
        # ------------------------------------------------------------------
        validated = ValidatedMarketEvent(
            event_id=str(uuid.uuid4()),
            original_event_id=event.event_id,
            provider=event.provider,
            instrument=event.instrument,
            received_at=event.received_at,
            event_timestamp=event.event_timestamp,
            corrected_timestamp=False,
            payload=event.payload,
            injected_faults=event.injected_faults,
            is_replay=event.is_replay,
            replay_source=event.replay_source,
            trace_id=event.trace_id,
        )

        # Record end-to-end processing latency
        now = datetime.now(UTC)
        latency = (now - event.event_timestamp).total_seconds()
        EVENT_PROCESSING_LATENCY_SECONDS.labels(
            service="validation-service", provider=event.provider
        ).observe(abs(latency))

        EVENTS_VALIDATED_TOTAL.labels(provider=event.provider, outcome="passed").inc()

        logger.info(
            "event_validated",
            provider=event.provider,
            event_id=event.event_id,
            validated_event_id=validated.event_id,
            quality_score=quality_score,
        )

        return validated, None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _check_types(self, event: RawMarketEvent) -> str | None:
        """Return an error message if type constraints are violated, else None."""
        if not isinstance(event.payload, dict):
            return f"payload must be a dict, got {type(event.payload).__name__}"

        price = event.payload.get("price")
        if price is not None and not isinstance(price, (int, float)):
            return f"payload.price must be numeric, got {type(price).__name__}"

        return None

    def _check_timestamp(self, event_timestamp: datetime) -> tuple[str | None, DLQFailureCategory]:
        """
        Check event_timestamp is within the allowed window.

        Returns (error_message, category) — error_message is None on success.
        """
        now = datetime.now(UTC)
        oldest_allowed = now - timedelta(hours=self._settings.max_event_age_hours)
        latest_allowed = now + timedelta(minutes=self._settings.max_future_minutes)

        if event_timestamp < oldest_allowed:
            age_hours = (now - event_timestamp).total_seconds() / 3600
            return (
                f"event_timestamp is {age_hours:.1f}h old, exceeds max age "
                f"of {self._settings.max_event_age_hours}h",
                DLQFailureCategory.STALE,
            )

        if event_timestamp > latest_allowed:
            drift_minutes = (event_timestamp - now).total_seconds() / 60
            return (
                f"event_timestamp is {drift_minutes:.1f}min in the future, "
                f"exceeds max of {self._settings.max_future_minutes}min",
                DLQFailureCategory.OUT_OF_ORDER,
            )

        return None, DLQFailureCategory.UNKNOWN  # category unused when no error

    def _check_price(self, payload: dict[str, Any]) -> str | None:
        """Return an error message if price fails sanity check, else None."""
        price = payload.get("price")
        if price is None:
            # Price field is optional — not every instrument has a scalar price
            return None

        if not isinstance(price, (int, float)):
            return f"payload.price is not numeric: {price!r}"

        if price <= self._settings.min_price:
            return f"payload.price {price} must be > {self._settings.min_price}"

        if price >= self._settings.max_price:
            return f"payload.price {price} must be < {self._settings.max_price}"

        return None

    @staticmethod
    def _make_dlq(
        event: RawMarketEvent,
        raw_payload: dict[str, Any],
        reason: str,
        category: DLQFailureCategory,
    ) -> DLQEvent:
        """Construct a DLQEvent, preserving the original raw payload verbatim."""
        DLQ_EVENTS_TOTAL.labels(provider=event.provider, failure_category=category.value).inc()
        EVENTS_VALIDATED_TOTAL.labels(provider=event.provider, outcome="failed").inc()

        return DLQEvent(
            original_event_id=event.event_id,
            provider=event.provider,
            instrument=event.instrument,
            failure_reason=reason,
            failure_category=category,
            raw_payload=raw_payload,
            original_received_at=event.received_at,
            trace_id=event.trace_id,
        )
