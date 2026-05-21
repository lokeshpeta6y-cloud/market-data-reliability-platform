"""Unit tests for quality score computation in normalization_service.normalizer."""

import pytest

from normalization_service.normalizer import _compute_quality_score
from mdrp_common.models import FaultType


class TestQualityScore:
    def test_no_faults_returns_one(self) -> None:
        assert _compute_quality_score([]) == 1.0

    def test_delayed_penalty(self) -> None:
        score = _compute_quality_score([FaultType.DELAYED])
        assert score == pytest.approx(0.95)

    def test_schema_drift_penalty(self) -> None:
        score = _compute_quality_score([FaultType.SCHEMA_DRIFT])
        assert score == pytest.approx(0.80)

    def test_stale_penalty(self) -> None:
        score = _compute_quality_score([FaultType.STALE])
        assert score == pytest.approx(0.70)

    def test_partial_curve_penalty(self) -> None:
        score = _compute_quality_score([FaultType.PARTIAL_CURVE])
        assert score == pytest.approx(0.75)

    def test_out_of_order_penalty(self) -> None:
        score = _compute_quality_score([FaultType.OUT_OF_ORDER])
        assert score == pytest.approx(0.90)

    def test_duplicate_fault_counted_once(self) -> None:
        score_once = _compute_quality_score([FaultType.STALE])
        score_twice = _compute_quality_score([FaultType.STALE, FaultType.STALE])
        assert score_once == score_twice

    def test_multiple_faults_cumulative(self) -> None:
        score = _compute_quality_score([FaultType.STALE, FaultType.DELAYED])
        # 1.0 - 0.30 - 0.05 = 0.65
        assert score == pytest.approx(0.65)

    def test_score_clamped_to_zero(self) -> None:
        # Stack all penalties — should never go below 0
        all_faults = list(FaultType)
        score = _compute_quality_score(all_faults)
        assert score >= 0.0

    def test_malformed_fault_no_penalty(self) -> None:
        # MALFORMED is not in _FAULT_PENALTIES so it contributes 0 penalty
        score = _compute_quality_score([FaultType.MALFORMED])
        assert score == 1.0

    def test_duplicate_fault_no_penalty(self) -> None:
        score = _compute_quality_score([FaultType.DUPLICATE])
        assert score == 1.0
