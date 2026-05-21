"""Unit tests for gold_loader.snapshot_assembler.SnapshotAssembler."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from gold_loader.snapshot_assembler import SnapshotAssembler
from mdrp_common.models import CurveEvent, DeliveryPeriod


def _ts(minutes_ago: int = 0) -> datetime:
    """Return a UTC datetime offset by *minutes_ago* from now."""
    return datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)


def _event(
    curve_name: str = "TTF_MONTHLY_FWD",
    tenor: str = "2024-03",
    price: float = 30.0,
    quality_score: float = 1.0,
    event_timestamp: datetime | None = None,
    version: int = 1,
) -> CurveEvent:
    return CurveEvent(
        source_event_id=str(uuid4()),
        curve_name=curve_name,
        instrument="TTF",
        tenor=tenor,
        delivery_period=DeliveryPeriod.MONTHLY,
        price=Decimal(str(price)),
        currency="EUR",
        unit="MWh",
        provider="provider-emulator",
        version=version,
        event_timestamp=event_timestamp or _ts(0),
        ingestion_timestamp=datetime.now(timezone.utc),
        quality_score=quality_score,
        is_replay=False,
        replay_source=None,
        trace_id=str(uuid4()),
    )


class TestAdd:
    def test_adds_event_to_window(self) -> None:
        assembler = SnapshotAssembler(snapshot_window_minutes=5)
        assembler.add(_event())
        assert assembler.pending_window_count() == 1

    def test_same_window_collapses(self) -> None:
        assembler = SnapshotAssembler(snapshot_window_minutes=5)
        ts = _ts(0)
        assembler.add(_event(tenor="2024-03", event_timestamp=ts))
        assembler.add(_event(tenor="2024-04", event_timestamp=ts))
        assert assembler.pending_window_count() == 1

    def test_different_curves_separate_windows(self) -> None:
        assembler = SnapshotAssembler(snapshot_window_minutes=5)
        ts = _ts(0)
        assembler.add(_event(curve_name="TTF_MONTHLY_FWD", event_timestamp=ts))
        assembler.add(_event(curve_name="NBP_MONTHLY_FWD", event_timestamp=ts))
        assert assembler.pending_window_count() == 2

    def test_last_write_wins_for_same_tenor(self) -> None:
        assembler = SnapshotAssembler(
            snapshot_window_minutes=5,
            expected_tenors_per_curve=1,
        )
        ts = _ts(0)
        assembler.add(_event(tenor="2024-03", price=10.0, event_timestamp=ts))
        assembler.add(_event(tenor="2024-03", price=99.0, version=2, event_timestamp=ts))

        # Make the window expire and collect the snapshot
        old_ts = _ts(20)
        assembler._windows = {
            k: v for k, v in assembler._windows.items()
        }
        # Manually age the window start
        for buf in assembler._windows.values():
            buf.window_start = _ts(20)

        snaps = assembler.get_ready_snapshots()
        assert len(snaps) == 1
        assert snaps[0].tenors["2024-03"].price == Decimal("99.0")


class TestGetReadySnapshots:
    def test_fresh_window_not_ready(self) -> None:
        assembler = SnapshotAssembler(snapshot_window_minutes=5)
        assembler.add(_event(event_timestamp=_ts(0)))
        assert assembler.get_ready_snapshots() == []

    def test_expired_window_returned(self) -> None:
        assembler = SnapshotAssembler(
            snapshot_window_minutes=5,
            min_completeness=0.0,
            expected_tenors_per_curve=1,
        )
        # Add event and manually age the window
        assembler.add(_event(event_timestamp=_ts(0)))
        for buf in assembler._windows.values():
            buf.window_start = _ts(20)

        snaps = assembler.get_ready_snapshots()
        assert len(snaps) == 1
        assert assembler.pending_window_count() == 0

    def test_below_min_completeness_discarded(self) -> None:
        assembler = SnapshotAssembler(
            snapshot_window_minutes=5,
            min_completeness=0.90,
            expected_tenors_per_curve=10,
        )
        # Only 1 of 10 expected tenors → completeness 0.10
        assembler.add(_event(tenor="2024-03", event_timestamp=_ts(0)))
        for buf in assembler._windows.values():
            buf.window_start = _ts(20)

        snaps = assembler.get_ready_snapshots()
        assert snaps == []

    def test_full_completeness_is_authoritative(self) -> None:
        assembler = SnapshotAssembler(
            snapshot_window_minutes=5,
            min_completeness=0.80,
            min_quality_score=0.70,
            expected_tenors_per_curve=2,
        )
        ts = _ts(0)
        assembler.add(_event(tenor="2024-03", quality_score=1.0, event_timestamp=ts))
        assembler.add(_event(tenor="2024-04", quality_score=1.0, event_timestamp=ts))
        for buf in assembler._windows.values():
            buf.window_start = _ts(20)

        snaps = assembler.get_ready_snapshots()
        assert len(snaps) == 1
        assert snaps[0].completeness == pytest.approx(1.0)
        assert snaps[0].is_authoritative is True

    def test_low_quality_not_authoritative(self) -> None:
        assembler = SnapshotAssembler(
            snapshot_window_minutes=5,
            min_completeness=0.80,
            min_quality_score=0.70,
            expected_tenors_per_curve=1,
        )
        assembler.add(_event(tenor="2024-03", quality_score=0.50, event_timestamp=_ts(0)))
        for buf in assembler._windows.values():
            buf.window_start = _ts(20)

        snaps = assembler.get_ready_snapshots()
        assert len(snaps) == 1
        assert snaps[0].is_authoritative is False
