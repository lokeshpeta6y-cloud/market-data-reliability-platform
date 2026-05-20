"""
Unit tests for mdrp_common.models.

Covers RawMarketEvent, CurveEvent, DLQEvent, ForwardCurveSnapshot and all
related enumerations.  No external dependencies — no Redis, no Kafka.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from mdrp_common.models import (
    CurveEvent,
    DeliveryPeriod,
    DLQEvent,
    DLQFailureCategory,
    FaultType,
    ForwardCurveSnapshot,
    RawMarketEvent,
    ReplaySource,
    TenorPrice,
    ValidatedMarketEvent,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _make_raw_event(**overrides) -> dict:
    base = {
        "provider": "test-provider",
        "instrument": "TTF_CAL25",
        "event_timestamp": _utc_now(),
        "payload": {"price": 42.50, "tenor": "2025-CAL", "currency": "EUR", "unit": "MWh"},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# RawMarketEvent
# ---------------------------------------------------------------------------


class TestRawMarketEvent:
    def test_valid_construction_sets_defaults(self):
        evt = RawMarketEvent(**_make_raw_event())
        assert evt.event_id is not None
        assert evt.trace_id is not None
        assert evt.is_replay is False
        assert evt.replay_source is None
        assert evt.injected_faults == []

    def test_event_id_is_unique(self):
        e1 = RawMarketEvent(**_make_raw_event())
        e2 = RawMarketEvent(**_make_raw_event())
        assert e1.event_id != e2.event_id

    def test_explicit_event_id_preserved(self):
        eid = str(uuid.uuid4())
        evt = RawMarketEvent(**_make_raw_event(event_id=eid))
        assert evt.event_id == eid

    def test_missing_provider_raises(self):
        data = _make_raw_event()
        del data["provider"]
        with pytest.raises(ValidationError) as exc_info:
            RawMarketEvent(**data)
        errors = exc_info.value.errors()
        assert any(e["loc"] == ("provider",) for e in errors)

    def test_missing_instrument_raises(self):
        data = _make_raw_event()
        del data["instrument"]
        with pytest.raises(ValidationError):
            RawMarketEvent(**data)

    def test_missing_event_timestamp_raises(self):
        data = _make_raw_event()
        del data["event_timestamp"]
        with pytest.raises(ValidationError):
            RawMarketEvent(**data)

    def test_missing_payload_raises(self):
        data = _make_raw_event()
        del data["payload"]
        with pytest.raises(ValidationError):
            RawMarketEvent(**data)

    def test_naive_event_timestamp_gets_utc(self):
        naive = datetime(2025, 6, 1, 12, 0, 0)
        evt = RawMarketEvent(**_make_raw_event(event_timestamp=naive))
        assert evt.event_timestamp.tzinfo is not None
        assert evt.event_timestamp.tzinfo == timezone.utc

    def test_naive_received_at_gets_utc(self):
        naive = datetime(2025, 6, 1, 12, 0, 0)
        evt = RawMarketEvent(**_make_raw_event(received_at=naive))
        assert evt.received_at.tzinfo == timezone.utc

    def test_iso_string_event_timestamp_parsed(self):
        ts_str = "2025-03-15T14:30:00+00:00"
        evt = RawMarketEvent(**_make_raw_event(event_timestamp=ts_str))
        assert evt.event_timestamp.year == 2025
        assert evt.event_timestamp.month == 3

    def test_injected_faults_stored(self):
        faults = [FaultType.DUPLICATE, FaultType.STALE]
        evt = RawMarketEvent(**_make_raw_event(injected_faults=faults))
        assert FaultType.DUPLICATE in evt.injected_faults
        assert FaultType.STALE in evt.injected_faults

    def test_replay_fields(self):
        evt = RawMarketEvent(
            **_make_raw_event(is_replay=True, replay_source=ReplaySource.BRONZE_S3)
        )
        assert evt.is_replay is True
        assert evt.replay_source == ReplaySource.BRONZE_S3

    def test_all_fault_types_valid(self):
        for fault in FaultType:
            evt = RawMarketEvent(**_make_raw_event(injected_faults=[fault]))
            assert fault in evt.injected_faults


# ---------------------------------------------------------------------------
# CurveEvent
# ---------------------------------------------------------------------------


def _make_curve_event(**overrides) -> dict:
    base = {
        "source_event_id": str(uuid.uuid4()),
        "curve_name": "TTF_FORWARD",
        "instrument": "TTF",
        "tenor": "2025-03",
        "delivery_period": DeliveryPeriod.MONTHLY,
        "price": Decimal("42.500"),
        "currency": "EUR",
        "unit": "MWh",
        "provider": "test-provider",
        "version": 1,
        "event_timestamp": _utc_now(),
        "quality_score": 1.0,
        "trace_id": str(uuid.uuid4()),
    }
    base.update(overrides)
    return base


class TestCurveEvent:
    def test_valid_construction(self):
        evt = CurveEvent(**_make_curve_event())
        assert evt.curve_name == "TTF_FORWARD"
        assert evt.quality_score == 1.0

    def test_quality_score_zero_valid(self):
        evt = CurveEvent(**_make_curve_event(quality_score=0.0))
        assert evt.quality_score == 0.0

    def test_quality_score_one_valid(self):
        evt = CurveEvent(**_make_curve_event(quality_score=1.0))
        assert evt.quality_score == 1.0

    def test_quality_score_above_one_raises(self):
        with pytest.raises(ValidationError):
            CurveEvent(**_make_curve_event(quality_score=1.01))

    def test_quality_score_below_zero_raises(self):
        with pytest.raises(ValidationError):
            CurveEvent(**_make_curve_event(quality_score=-0.01))

    def test_quality_score_mid_range(self):
        evt = CurveEvent(**_make_curve_event(quality_score=0.75))
        assert evt.quality_score == 0.75

    def test_decimal_price_preserved(self):
        price = Decimal("123.456789")
        evt = CurveEvent(**_make_curve_event(price=price))
        assert evt.price == price

    def test_price_as_string_decimal(self):
        evt = CurveEvent(**_make_curve_event(price="99.99"))
        assert evt.price == Decimal("99.99")

    def test_price_serialisation_round_trip(self):
        price = Decimal("55.123")
        evt = CurveEvent(**_make_curve_event(price=price))
        dumped = evt.model_dump()
        assert dumped["price"] == price

    def test_delivery_period_enum_values(self):
        for period in DeliveryPeriod:
            evt = CurveEvent(**_make_curve_event(delivery_period=period))
            assert evt.delivery_period == period

    def test_missing_trace_id_raises(self):
        data = _make_curve_event()
        del data["trace_id"]
        with pytest.raises(ValidationError):
            CurveEvent(**data)

    def test_ingestion_timestamp_auto_set(self):
        evt = CurveEvent(**_make_curve_event())
        assert evt.ingestion_timestamp is not None
        assert evt.ingestion_timestamp.tzinfo is not None


# ---------------------------------------------------------------------------
# DLQEvent
# ---------------------------------------------------------------------------


def _make_dlq_event(**overrides) -> dict:
    base = {
        "original_event_id": str(uuid.uuid4()),
        "provider": "test-provider",
        "instrument": "TTF_CAL25",
        "failure_reason": "Test failure",
        "failure_category": DLQFailureCategory.UNKNOWN,
        "raw_payload": {"price": 42.5},
        "original_received_at": _utc_now(),
        "trace_id": str(uuid.uuid4()),
    }
    base.update(overrides)
    return base


class TestDLQEvent:
    def test_valid_construction(self):
        evt = DLQEvent(**_make_dlq_event())
        assert evt.dlq_event_id is not None
        assert evt.retry_count == 0

    def test_all_failure_categories(self):
        for category in DLQFailureCategory:
            evt = DLQEvent(**_make_dlq_event(failure_category=category))
            assert evt.failure_category == category

    def test_schema_violation_category(self):
        evt = DLQEvent(
            **_make_dlq_event(failure_category=DLQFailureCategory.SCHEMA_VIOLATION)
        )
        assert evt.failure_category == DLQFailureCategory.SCHEMA_VIOLATION

    def test_duplicate_category(self):
        evt = DLQEvent(**_make_dlq_event(failure_category=DLQFailureCategory.DUPLICATE))
        assert evt.failure_category == DLQFailureCategory.DUPLICATE

    def test_malformed_category(self):
        evt = DLQEvent(**_make_dlq_event(failure_category=DLQFailureCategory.MALFORMED))
        assert evt.failure_category == DLQFailureCategory.MALFORMED

    def test_stale_category(self):
        evt = DLQEvent(**_make_dlq_event(failure_category=DLQFailureCategory.STALE))
        assert evt.failure_category == DLQFailureCategory.STALE

    def test_out_of_order_category(self):
        evt = DLQEvent(
            **_make_dlq_event(failure_category=DLQFailureCategory.OUT_OF_ORDER)
        )
        assert evt.failure_category == DLQFailureCategory.OUT_OF_ORDER

    def test_missing_required_field_category(self):
        evt = DLQEvent(
            **_make_dlq_event(
                failure_category=DLQFailureCategory.MISSING_REQUIRED_FIELD
            )
        )
        assert evt.failure_category == DLQFailureCategory.MISSING_REQUIRED_FIELD

    def test_instrument_optional(self):
        data = _make_dlq_event()
        del data["instrument"]
        evt = DLQEvent(**data)
        assert evt.instrument is None

    def test_raw_payload_preserved(self):
        payload = {"price": 99.0, "custom_field": "abc", "nested": {"x": 1}}
        evt = DLQEvent(**_make_dlq_event(raw_payload=payload))
        assert evt.raw_payload == payload

    def test_dlq_timestamp_auto_set(self):
        evt = DLQEvent(**_make_dlq_event())
        assert evt.dlq_timestamp is not None
        assert evt.dlq_timestamp.tzinfo is not None

    def test_missing_original_event_id_raises(self):
        data = _make_dlq_event()
        del data["original_event_id"]
        with pytest.raises(ValidationError):
            DLQEvent(**data)


# ---------------------------------------------------------------------------
# ForwardCurveSnapshot
# ---------------------------------------------------------------------------


def _make_tenor_price(tenor: str, price: str = "50.0") -> TenorPrice:
    return TenorPrice(
        tenor=tenor,
        price=Decimal(price),
        quality_score=1.0,
        last_updated=_utc_now(),
    )


def _make_snapshot(**overrides) -> dict:
    tenors = {
        "2025-03": _make_tenor_price("2025-03", "42.5"),
        "2025-04": _make_tenor_price("2025-04", "43.0"),
        "2025-05": _make_tenor_price("2025-05", "43.5"),
    }
    base = {
        "curve_name": "TTF_FORWARD",
        "instrument": "TTF",
        "as_of": _utc_now(),
        "tenors": tenors,
        "completeness": 1.0,
        "version": 1,
        "provider": "test-provider",
    }
    base.update(overrides)
    return base


class TestForwardCurveSnapshot:
    def test_valid_construction(self):
        snap = ForwardCurveSnapshot(**_make_snapshot())
        assert snap.curve_name == "TTF_FORWARD"
        assert len(snap.tenors) == 3

    def test_completeness_one_full_curve(self):
        snap = ForwardCurveSnapshot(**_make_snapshot(completeness=1.0))
        assert snap.completeness == 1.0

    def test_completeness_partial_curve(self):
        snap = ForwardCurveSnapshot(**_make_snapshot(completeness=0.6))
        assert snap.completeness == 0.6

    def test_completeness_zero_valid(self):
        snap = ForwardCurveSnapshot(**_make_snapshot(completeness=0.0))
        assert snap.completeness == 0.0

    def test_completeness_above_one_raises(self):
        with pytest.raises(ValidationError):
            ForwardCurveSnapshot(**_make_snapshot(completeness=1.01))

    def test_completeness_below_zero_raises(self):
        with pytest.raises(ValidationError):
            ForwardCurveSnapshot(**_make_snapshot(completeness=-0.01))

    def test_snapshot_id_auto_generated(self):
        snap = ForwardCurveSnapshot(**_make_snapshot())
        assert snap.snapshot_id is not None
        # Should be a valid UUID
        uuid.UUID(snap.snapshot_id)

    def test_unique_snapshot_ids(self):
        s1 = ForwardCurveSnapshot(**_make_snapshot())
        s2 = ForwardCurveSnapshot(**_make_snapshot())
        assert s1.snapshot_id != s2.snapshot_id

    def test_tenor_prices_decimal(self):
        snap = ForwardCurveSnapshot(**_make_snapshot())
        for tenor_price in snap.tenors.values():
            assert isinstance(tenor_price.price, Decimal)

    def test_is_authoritative_defaults_false(self):
        snap = ForwardCurveSnapshot(**_make_snapshot())
        assert snap.is_authoritative is False

    def test_empty_tenors_allowed(self):
        snap = ForwardCurveSnapshot(**_make_snapshot(tenors={}, completeness=0.0))
        assert snap.tenors == {}

    def test_created_at_auto_set(self):
        snap = ForwardCurveSnapshot(**_make_snapshot())
        assert snap.created_at is not None
        assert snap.created_at.tzinfo is not None
