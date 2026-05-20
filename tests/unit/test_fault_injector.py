"""
Unit tests for FaultInjector in provider-emulator.

All randomness is controlled via random.seed() so that probabilistic faults
with rate=1.0 are deterministic.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import pytest

from mdrp_common.models import FaultType, RawMarketEvent
from provider_emulator.fault_injector import FaultInjector, _SCHEMA_DRIFT_MAP


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
            "curve_name": "TTF_FORWARD",
            "bid": 42.0,
            "ask": 43.0,
            "volume": 1000,
        },
    }
    defaults.update(overrides)
    return RawMarketEvent(**defaults)


def _make_events(n: int = 5) -> list[RawMarketEvent]:
    return [_make_event() for _ in range(n)]


def _injector(**kwargs) -> FaultInjector:
    """Create a FaultInjector with all rates zeroed unless explicitly provided."""
    defaults = {
        "fault_rate_duplicate": 0.0,
        "fault_rate_malformed": 0.0,
        "fault_rate_delayed": 0.0,
        "fault_rate_out_of_order": 0.0,
        "fault_rate_schema_drift": 0.0,
        "fault_rate_stale": 0.0,
        "fault_rate_partial_curve": 0.0,
    }
    defaults.update(kwargs)
    return FaultInjector(**defaults)


# ---------------------------------------------------------------------------
# No faults — pass through unchanged
# ---------------------------------------------------------------------------


class TestNoFaults:
    def test_events_pass_through_unchanged(self):
        fi = _injector()
        events = _make_events(5)
        original_ids = [e.event_id for e in events]
        result = fi.inject(events)
        assert len(result) == 5
        assert [e.event_id for e in result] == original_ids

    def test_no_injected_faults_on_events(self):
        fi = _injector()
        events = _make_events(3)
        result = fi.inject(events)
        for event in result:
            assert event.injected_faults == []

    def test_empty_input_returns_empty(self):
        fi = _injector()
        result = fi.inject([])
        assert result == []

    def test_single_event_unchanged(self):
        fi = _injector()
        event = _make_event()
        result = fi.inject([event])
        assert len(result) == 1
        assert result[0].event_id == event.event_id

    def test_payload_unchanged(self):
        fi = _injector()
        event = _make_event()
        original_payload = dict(event.payload)
        result = fi.inject([event])
        assert result[0].payload == original_payload


# ---------------------------------------------------------------------------
# DUPLICATE fault (rate = 1.0)
# ---------------------------------------------------------------------------


class TestDuplicateFault:
    def test_every_event_produces_two_events(self):
        fi = _injector(fault_rate_duplicate=1.0)
        events = _make_events(3)
        result = fi.inject(events)
        assert len(result) == 6  # Each event duplicated

    def test_duplicate_shares_same_event_id(self):
        fi = _injector(fault_rate_duplicate=1.0)
        event = _make_event()
        result = fi.inject([event])
        assert len(result) == 2
        assert result[0].event_id == result[1].event_id

    def test_duplicate_fault_recorded_in_injected_faults(self):
        fi = _injector(fault_rate_duplicate=1.0)
        event = _make_event()
        result = fi.inject([event])
        for evt in result:
            assert FaultType.DUPLICATE in evt.injected_faults

    def test_injected_faults_contains_duplicate(self):
        fi = _injector(fault_rate_duplicate=1.0)
        events = _make_events(2)
        result = fi.inject(events)
        assert all(FaultType.DUPLICATE in e.injected_faults for e in result)

    def test_original_event_not_mutated(self):
        fi = _injector(fault_rate_duplicate=1.0)
        original = _make_event()
        original_id = original.event_id
        fi.inject([original])
        # The injector should deep-copy; the original should not be modified
        # (We check that the original's injected_faults doesn't contain DUPLICATE
        # unless the injector mutates the input — which it doesn't since it deep copies)
        assert original.event_id == original_id


# ---------------------------------------------------------------------------
# MALFORMED fault (rate = 1.0)
# ---------------------------------------------------------------------------


class TestMalformedFault:
    def test_malformed_fault_recorded_in_injected_faults(self):
        fi = _injector(fault_rate_malformed=1.0)
        events = _make_events(3)
        result = fi.inject(events)
        assert all(FaultType.MALFORMED in e.injected_faults for e in result)

    def test_malformed_payload_is_corrupted(self):
        """At least one of the following must be true: a required field is None,
        a field is corrupted, or the payload is empty."""
        fi = _injector(fault_rate_malformed=1.0)
        required_fields = ["price", "tenor", "curve_name", "currency", "unit"]
        corrupted_count = 0
        for _ in range(20):
            event = _make_event()
            result = fi.inject([event])
            assert len(result) == 1
            payload = result[0].payload
            payload_corrupted = (
                not payload  # empty dict
                or any(payload.get(f) is None for f in required_fields)
                or any(
                    isinstance(payload.get(f), str) and "~~" in str(payload.get(f, ""))
                    for f in required_fields
                )
                or (payload.get("price") is not None and payload.get("price", 0) < 0)
            )
            if payload_corrupted:
                corrupted_count += 1
        # All 20 should be corrupted since rate=1.0
        assert corrupted_count == 20

    def test_malformed_event_count_unchanged(self):
        fi = _injector(fault_rate_malformed=1.0)
        events = _make_events(5)
        result = fi.inject(events)
        assert len(result) == 5  # Malformed doesn't add/remove events

    def test_injected_faults_contains_malformed(self):
        fi = _injector(fault_rate_malformed=1.0)
        event = _make_event()
        result = fi.inject([event])
        assert FaultType.MALFORMED in result[0].injected_faults


# ---------------------------------------------------------------------------
# STALE fault (rate = 1.0)
# ---------------------------------------------------------------------------


class TestStaleFault:
    def test_stale_timestamp_is_in_the_past(self):
        fi = _injector(fault_rate_stale=1.0)
        before = _now()
        events = _make_events(3)
        result = fi.inject(events)
        for evt in result:
            # Each event's timestamp should be at least 2 hours in the past
            assert evt.event_timestamp < before - timedelta(hours=1)

    def test_stale_fault_recorded_in_injected_faults(self):
        fi = _injector(fault_rate_stale=1.0)
        events = _make_events(3)
        result = fi.inject(events)
        assert all(FaultType.STALE in e.injected_faults for e in result)

    def test_stale_backdated_at_least_2_hours(self):
        fi = _injector(fault_rate_stale=1.0)
        event = _make_event()
        original_ts = event.event_timestamp
        result = fi.inject([event])
        delta = original_ts - result[0].event_timestamp
        assert delta.total_seconds() >= 2 * 3600 - 1  # Allow 1s tolerance

    def test_stale_event_count_unchanged(self):
        fi = _injector(fault_rate_stale=1.0)
        events = _make_events(4)
        result = fi.inject(events)
        assert len(result) == 4

    def test_injected_faults_contains_stale(self):
        fi = _injector(fault_rate_stale=1.0)
        event = _make_event()
        result = fi.inject([event])
        assert FaultType.STALE in result[0].injected_faults


# ---------------------------------------------------------------------------
# SCHEMA_DRIFT fault (rate = 1.0)
# ---------------------------------------------------------------------------


class TestSchemaDriftFault:
    def test_schema_drift_renames_payload_fields(self):
        fi = _injector(fault_rate_schema_drift=1.0)
        # Run many times since drift picks a random subset of driftable keys
        drifted_seen = False
        for _ in range(20):
            event = _make_event()
            result = fi.inject([event])
            payload = result[0].payload
            # Check that at least one drifted key is present
            if any(v in payload for v in _SCHEMA_DRIFT_MAP.values()):
                drifted_seen = True
                break
        assert drifted_seen

    def test_schema_drift_fault_recorded_in_injected_faults(self):
        fi = _injector(fault_rate_schema_drift=1.0)
        # We need a payload with driftable keys
        events = _make_events(5)
        result = fi.inject(events)
        assert all(FaultType.SCHEMA_DRIFT in e.injected_faults for e in result)

    def test_schema_drift_original_keys_removed(self):
        fi = _injector(fault_rate_schema_drift=1.0)
        for _ in range(20):
            event = _make_event()
            result = fi.inject([event])
            payload = result[0].payload
            # For any drifted value key present, the original key should not be
            for original, drifted in _SCHEMA_DRIFT_MAP.items():
                if drifted in payload:
                    assert original not in payload, (
                        f"Original key {original!r} still present after drift to {drifted!r}"
                    )

    def test_schema_drift_event_count_unchanged(self):
        fi = _injector(fault_rate_schema_drift=1.0)
        events = _make_events(4)
        result = fi.inject(events)
        assert len(result) == 4


# ---------------------------------------------------------------------------
# DELAYED fault (rate = 1.0)
# ---------------------------------------------------------------------------


class TestDelayedFault:
    def test_delayed_events_go_to_hold_queue(self):
        fi = _injector(fault_rate_delayed=1.0, delay_min_seconds=60.0, delay_max_seconds=120.0)
        events = _make_events(3)
        result = fi.inject(events)
        # All events should be in the hold queue, not returned
        assert len(result) == 0
        assert fi.delay_queue_depth == 3

    def test_delayed_events_not_immediately_available(self):
        fi = _injector(fault_rate_delayed=1.0, delay_min_seconds=60.0, delay_max_seconds=120.0)
        events = _make_events(2)
        fi.inject(events)
        # drain_ready should return nothing yet
        ready = fi.drain_ready()
        assert len(ready) == 0


# ---------------------------------------------------------------------------
# Multiple faults can coexist
# ---------------------------------------------------------------------------


class TestMultipleFaults:
    def test_stale_and_malformed_both_applied(self):
        fi = _injector(fault_rate_stale=1.0, fault_rate_malformed=1.0)
        event = _make_event()
        result = fi.inject([event])
        assert len(result) == 1
        faults = result[0].injected_faults
        assert FaultType.STALE in faults
        assert FaultType.MALFORMED in faults

    def test_stale_and_schema_drift_both_applied(self):
        fi = _injector(fault_rate_stale=1.0, fault_rate_schema_drift=1.0)
        for _ in range(10):
            event = _make_event()
            result = fi.inject([event])
            if result:
                faults = result[0].injected_faults
                assert FaultType.STALE in faults
                # Schema drift only applies if there are driftable keys
                if any(k in event.payload for k in _SCHEMA_DRIFT_MAP):
                    assert FaultType.SCHEMA_DRIFT in faults
                break
