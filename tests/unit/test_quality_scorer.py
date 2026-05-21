"""
Unit tests for QualityScorer in validation-service.

Uses fakeredis — no real Redis required.
"""

from __future__ import annotations

import pytest
import fakeredis

from mdrp_common.models import FaultType
from validation_service.quality_scorer import FAULT_PENALTIES, QualityScorer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def redis_client():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def scorer(redis_client) -> QualityScorer:
    return QualityScorer(redis_client=redis_client, rolling_window=100)


# ---------------------------------------------------------------------------
# Score computation — no Redis interaction needed for pure computation
# ---------------------------------------------------------------------------


class TestScoreComputation:
    def test_no_faults_score_is_one(self, scorer):
        score = scorer.score_event("provider-A", [])
        assert score == 1.0

    def test_malformed_fault_score(self, scorer):
        score = scorer.score_event("provider-A", [FaultType.MALFORMED])
        expected = 1.0 - FAULT_PENALTIES[FaultType.MALFORMED]
        assert score == pytest.approx(expected)

    def test_malformed_penalty_is_0_5(self):
        assert FAULT_PENALTIES[FaultType.MALFORMED] == 0.50

    def test_duplicate_penalty_is_0_3(self):
        assert FAULT_PENALTIES[FaultType.DUPLICATE] == 0.30

    def test_stale_penalty_is_0_25(self):
        assert FAULT_PENALTIES[FaultType.STALE] == 0.25

    def test_schema_drift_penalty_is_0_2(self):
        assert FAULT_PENALTIES[FaultType.SCHEMA_DRIFT] == 0.20

    def test_duplicate_fault_score(self, scorer):
        score = scorer.score_event("provider-A", [FaultType.DUPLICATE])
        expected = 1.0 - FAULT_PENALTIES[FaultType.DUPLICATE]
        assert score == pytest.approx(expected)

    def test_two_faults_compound(self, scorer):
        faults = [FaultType.MALFORMED, FaultType.STALE]
        score = scorer.score_event("provider-A", faults)
        expected = max(
            0.0,
            1.0 - FAULT_PENALTIES[FaultType.MALFORMED] - FAULT_PENALTIES[FaultType.STALE],
        )
        assert score == pytest.approx(expected)

    def test_multiple_faults_clamped_to_zero(self, scorer):
        """Enough faults should drive the score to exactly 0.0, never negative."""
        all_faults = list(FaultType)
        score = scorer.score_event("provider-A", all_faults)
        assert score == 0.0

    def test_three_faults_compound(self, scorer):
        faults = [FaultType.MALFORMED, FaultType.DUPLICATE, FaultType.STALE]
        score = scorer.score_event("provider-A", faults)
        total_penalty = sum(FAULT_PENALTIES[f] for f in faults)
        expected = max(0.0, 1.0 - total_penalty)
        assert score == pytest.approx(expected)

    def test_score_never_exceeds_one(self, scorer):
        # Even with an empty fault list, score should be exactly 1.0
        score = scorer.score_event("provider-A", [])
        assert score <= 1.0

    def test_score_never_below_zero(self, scorer):
        all_faults = list(FaultType)
        score = scorer.score_event("provider-A", all_faults)
        assert score >= 0.0

    def test_score_returned_as_float(self, scorer):
        score = scorer.score_event("provider-A", [])
        assert isinstance(score, float)


# ---------------------------------------------------------------------------
# Redis storage — rolling average persisted and retrieved
# ---------------------------------------------------------------------------


class TestRollingAverageRedisStorage:
    def test_score_stored_in_redis(self, scorer, redis_client):
        scorer.score_event("provider-B", [])
        avg = scorer.get_rolling_average("provider-B")
        assert avg is not None

    def test_no_scores_returns_none(self, scorer):
        avg = scorer.get_rolling_average("provider-never-seen")
        assert avg is None

    def test_single_perfect_score_rolling_avg_is_one(self, scorer):
        scorer.score_event("provider-C", [])
        avg = scorer.get_rolling_average("provider-C")
        assert avg == pytest.approx(1.0)

    def test_single_malformed_score_rolling_avg(self, scorer):
        scorer.score_event("provider-D", [FaultType.MALFORMED])
        avg = scorer.get_rolling_average("provider-D")
        expected = 1.0 - FAULT_PENALTIES[FaultType.MALFORMED]
        assert avg == pytest.approx(expected)

    def test_rolling_avg_updates_over_multiple_events(self, scorer):
        # 2 perfect + 2 malformed
        scorer.score_event("provider-E", [])
        scorer.score_event("provider-E", [])
        scorer.score_event("provider-E", [FaultType.MALFORMED])
        scorer.score_event("provider-E", [FaultType.MALFORMED])
        avg = scorer.get_rolling_average("provider-E")
        perfect = 1.0
        malformed = 1.0 - FAULT_PENALTIES[FaultType.MALFORMED]
        expected = (perfect + perfect + malformed + malformed) / 4
        assert avg == pytest.approx(expected, rel=1e-3)

    def test_providers_isolated_in_redis(self, scorer):
        scorer.score_event("provider-F", [])
        scorer.score_event("provider-G", [FaultType.MALFORMED])
        avg_f = scorer.get_rolling_average("provider-F")
        avg_g = scorer.get_rolling_average("provider-G")
        assert avg_f != avg_g
        assert avg_f == pytest.approx(1.0)

    def test_rolling_window_decay_applied(self, redis_client):
        """When count reaches window, both sum and count are halved (decay)."""
        window = 10
        scorer = QualityScorer(redis_client=redis_client, rolling_window=window)
        # Score window events at 1.0 each
        for _ in range(window):
            scorer.score_event("provider-H", [])
        # After decay, average should still be approximately 1.0 (all perfect scores)
        avg = scorer.get_rolling_average("provider-H")
        assert avg == pytest.approx(1.0, rel=1e-2)

    def test_rolling_average_reflects_recent_bad_scores(self, redis_client):
        """Introducing malformed scores should pull the average down."""
        scorer = QualityScorer(redis_client=redis_client, rolling_window=100)
        # Start with 10 perfect scores
        for _ in range(10):
            scorer.score_event("provider-I", [])
        avg_before = scorer.get_rolling_average("provider-I")
        # Now introduce 10 malformed scores
        for _ in range(10):
            scorer.score_event("provider-I", [FaultType.MALFORMED])
        avg_after = scorer.get_rolling_average("provider-I")
        assert avg_after < avg_before

    def test_score_stored_with_correct_key_prefix(self, scorer, redis_client):
        scorer.score_event("my-provider", [])
        keys = redis_client.keys("provider:quality:*")
        assert any("my-provider" in (k.decode() if isinstance(k, bytes) else k) for k in keys)
