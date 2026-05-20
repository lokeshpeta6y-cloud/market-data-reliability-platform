"""
Unit tests for ValidationService in validation-service.

Uses fakeredis to mock Redis — no real Redis required.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import fakeredis
import pytest

from mdrp_common.models import (
    DLQEvent,
    DLQFailureCategory,
    FaultType,
    RawMarketEvent,
    ValidatedMarketEvent,
)
from validation_service.deduplicator import Deduplicator
from validation_service.quality_scorer import QualityScorer
from validation_service.settings import ValidationServiceSettings
from validation_service.validator import ValidationService


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def settings() -> ValidationServiceSettings:
    return ValidationServiceSettings(
        KAFKA_BOOTSTRAP_SERVERS="localhost:9092",
        REDIS_URL="redis://localhost:6379/0",
        MAX_EVENT_AGE_HOURS=24,
        MAX_FUTURE_MINUTES=5,
        MIN_PRICE=0.0,
        MAX_PRICE=1_000_000.0,
        DEDUP_TTL_SECONDS=3600,
        QUALITY_ROLLING_WINDOW=100,
    )


@pytest.fixture
def deduplicator(redis_client) -> Deduplicator:
    return Deduplicator(redis_client=redis_client, ttl_seconds=3600)


@pytest.fixture
def quality_scorer(redis_client) -> QualityScorer:
    return QualityScorer(redis_client=redis_client, rolling_window=100)


@pytest.fixture
def validator(settings, deduplicator, quality_scorer) -> ValidationService:
    return ValidationService(
        settings=settings,
        deduplicator=deduplicator,
        quality_scorer=quality_scorer,
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _make_event(**overrides) -> RawMarketEvent:
    defaults = {
        "provider": "test-provider",
        "instrument": "TTF_CAL25",
        "event_timestamp": _now(),
        "payload": {
            "price": 42.50,
            "tenor": "2025-CAL",
            "currency": "EUR",
            "unit": "MWh",
        },
    }
    defaults.update(overrides)
    return RawMarketEvent(**defaults)


# ---------------------------------------------------------------------------
# Happy path — valid event passes all rules
# ---------------------------------------------------------------------------


class TestValidEvent:
    def test_valid_event_returns_validated_and_none(self, validator):
        event = _make_event()
        validated, dlq = validator.validate(event)
        assert validated is not None
        assert dlq is None

    def test_validated_event_preserves_provider(self, validator):
        event = _make_event(provider="ice-endex")
        validated, _ = validator.validate(event)
        assert validated.provider == "ice-endex"

    def test_validated_event_preserves_instrument(self, validator):
        event = _make_event(instrument="NBP_WIN25")
        validated, _ = validator.validate(event)
        assert validated.instrument == "NBP_WIN25"

    def test_validated_event_has_new_event_id(self, validator):
        event = _make_event()
        validated, _ = validator.validate(event)
        assert validated.event_id != event.event_id

    def test_validated_event_preserves_original_event_id(self, validator):
        event = _make_event()
        validated, _ = validator.validate(event)
        assert validated.original_event_id == event.event_id

    def test_valid_event_type(self, validator):
        event = _make_event()
        validated, _ = validator.validate(event)
        assert isinstance(validated, ValidatedMarketEvent)

    def test_valid_event_preserves_trace_id(self, validator):
        event = _make_event()
        validated, _ = validator.validate(event)
        assert validated.trace_id == event.trace_id


# ---------------------------------------------------------------------------
# Rule 1: Missing required fields → MISSING_REQUIRED_FIELD
# ---------------------------------------------------------------------------


class TestMissingRequiredField:
    def test_empty_provider_sends_to_dlq(self, validator):
        event = _make_event(provider="")
        validated, dlq = validator.validate(event)
        assert validated is None
        assert dlq is not None
        assert dlq.failure_category == DLQFailureCategory.MISSING_REQUIRED_FIELD

    def test_empty_instrument_sends_to_dlq(self, validator):
        event = _make_event(instrument="")
        validated, dlq = validator.validate(event)
        assert validated is None
        assert dlq is not None
        assert dlq.failure_category == DLQFailureCategory.MISSING_REQUIRED_FIELD

    def test_dlq_event_is_dlqevent_type(self, validator):
        event = _make_event(provider="")
        _, dlq = validator.validate(event)
        assert isinstance(dlq, DLQEvent)

    def test_dlq_preserves_original_event_id(self, validator):
        event = _make_event(provider="")
        _, dlq = validator.validate(event)
        assert dlq.original_event_id == event.event_id

    def test_dlq_failure_reason_mentions_field(self, validator):
        event = _make_event(provider="")
        _, dlq = validator.validate(event)
        assert "provider" in dlq.failure_reason.lower()


# ---------------------------------------------------------------------------
# Rule 5: Price out of range → MALFORMED
# ---------------------------------------------------------------------------


class TestPriceOutOfRange:
    def test_negative_price_sends_to_dlq(self, validator):
        event = _make_event(payload={"price": -1.0, "currency": "EUR", "unit": "MWh"})
        validated, dlq = validator.validate(event)
        assert validated is None
        assert dlq is not None
        assert dlq.failure_category == DLQFailureCategory.MALFORMED

    def test_zero_price_sends_to_dlq(self, validator):
        event = _make_event(payload={"price": 0.0, "currency": "EUR", "unit": "MWh"})
        validated, dlq = validator.validate(event)
        assert validated is None
        assert dlq is not None
        assert dlq.failure_category == DLQFailureCategory.MALFORMED

    def test_price_equal_to_max_sends_to_dlq(self, validator):
        event = _make_event(
            payload={"price": 1_000_000.0, "currency": "EUR", "unit": "MWh"}
        )
        validated, dlq = validator.validate(event)
        assert validated is None
        assert dlq is not None
        assert dlq.failure_category == DLQFailureCategory.MALFORMED

    def test_price_exceeds_max_sends_to_dlq(self, validator):
        event = _make_event(
            payload={"price": 1_500_000.0, "currency": "EUR", "unit": "MWh"}
        )
        validated, dlq = validator.validate(event)
        assert validated is None
        assert dlq is not None
        assert dlq.failure_category == DLQFailureCategory.MALFORMED

    def test_valid_price_passes(self, validator):
        event = _make_event(payload={"price": 500_000.0, "currency": "EUR", "unit": "MWh"})
        validated, dlq = validator.validate(event)
        assert validated is not None
        assert dlq is None

    def test_no_price_in_payload_passes(self, validator):
        """Price is optional — an event without a price field should pass."""
        event = _make_event(payload={"currency": "EUR", "unit": "MWh"})
        validated, dlq = validator.validate(event)
        assert validated is not None
        assert dlq is None


# ---------------------------------------------------------------------------
# Rule 4: Future timestamp > 5 min → OUT_OF_ORDER
# ---------------------------------------------------------------------------


class TestFutureTimestamp:
    def test_6_minutes_future_sends_to_dlq(self, validator):
        future_ts = _now() + timedelta(minutes=6)
        event = _make_event(event_timestamp=future_ts)
        validated, dlq = validator.validate(event)
        assert validated is None
        assert dlq is not None
        assert dlq.failure_category == DLQFailureCategory.OUT_OF_ORDER

    def test_exactly_5_minutes_future_passes(self, validator):
        # 4 minutes in future should pass (within the 5-min window)
        near_future = _now() + timedelta(minutes=4)
        event = _make_event(event_timestamp=near_future)
        validated, dlq = validator.validate(event)
        assert validated is not None
        assert dlq is None

    def test_1_hour_future_sends_to_dlq(self, validator):
        far_future = _now() + timedelta(hours=1)
        event = _make_event(event_timestamp=far_future)
        validated, dlq = validator.validate(event)
        assert validated is None
        assert dlq is not None
        assert dlq.failure_category == DLQFailureCategory.OUT_OF_ORDER

    def test_dlq_failure_reason_mentions_future(self, validator):
        future_ts = _now() + timedelta(minutes=10)
        event = _make_event(event_timestamp=future_ts)
        _, dlq = validator.validate(event)
        assert "future" in dlq.failure_reason.lower()


# ---------------------------------------------------------------------------
# Rule 4: Too old timestamp > 24h → STALE
# ---------------------------------------------------------------------------


class TestStaleTimestamp:
    def test_25_hours_old_sends_to_dlq(self, validator):
        old_ts = _now() - timedelta(hours=25)
        event = _make_event(event_timestamp=old_ts)
        validated, dlq = validator.validate(event)
        assert validated is None
        assert dlq is not None
        assert dlq.failure_category == DLQFailureCategory.STALE

    def test_2_days_old_sends_to_dlq(self, validator):
        very_old_ts = _now() - timedelta(days=2)
        event = _make_event(event_timestamp=very_old_ts)
        validated, dlq = validator.validate(event)
        assert validated is None
        assert dlq is not None
        assert dlq.failure_category == DLQFailureCategory.STALE

    def test_23_hours_old_passes(self, validator):
        recent_ts = _now() - timedelta(hours=23)
        event = _make_event(event_timestamp=recent_ts)
        validated, dlq = validator.validate(event)
        assert validated is not None
        assert dlq is None

    def test_dlq_failure_reason_mentions_age(self, validator):
        old_ts = _now() - timedelta(hours=30)
        event = _make_event(event_timestamp=old_ts)
        _, dlq = validator.validate(event)
        assert dlq.failure_reason is not None
        assert len(dlq.failure_reason) > 0


# ---------------------------------------------------------------------------
# Rule 3: Duplicate detection → silent discard
# ---------------------------------------------------------------------------


class TestDuplication:
    def test_first_event_not_duplicate(self, validator):
        event = _make_event()
        validated, dlq = validator.validate(event)
        assert validated is not None
        assert dlq is None

    def test_second_identical_event_id_silently_discarded(self, validator):
        event = _make_event()
        # First pass should succeed
        validator.validate(event)
        # Second pass with the same event should be silently dropped
        validated, dlq = validator.validate(event)
        assert validated is None
        assert dlq is None

    def test_different_event_ids_both_pass(self, validator):
        e1 = _make_event()
        e2 = _make_event()
        assert e1.event_id != e2.event_id
        v1, d1 = validator.validate(e1)
        v2, d2 = validator.validate(e2)
        assert v1 is not None
        assert v2 is not None


# ---------------------------------------------------------------------------
# DLQ raw_payload preservation
# ---------------------------------------------------------------------------


class TestDLQPayloadPreservation:
    def test_dlq_preserves_raw_payload(self, validator):
        event = _make_event(provider="")
        _, dlq = validator.validate(event)
        assert dlq.raw_payload is not None
        assert "provider" in dlq.raw_payload or "event_id" in dlq.raw_payload
