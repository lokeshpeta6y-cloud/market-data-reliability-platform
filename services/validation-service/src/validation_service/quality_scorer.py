"""Quality scoring for market events — fault penalties, per-event scores, rolling provider averages in Redis."""

from __future__ import annotations

from typing import Final

import redis

from mdrp_common.logging import get_logger
from mdrp_common.models import FaultType

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Penalty table — penalty subtracted from 1.0 per fault
# ---------------------------------------------------------------------------

FAULT_PENALTIES: Final[dict[FaultType, float]] = {
    FaultType.DUPLICATE: 0.30,
    FaultType.MALFORMED: 0.50,
    FaultType.SCHEMA_DRIFT: 0.20,
    FaultType.STALE: 0.25,
    FaultType.OUT_OF_ORDER: 0.25,
    FaultType.DELAYED: 0.10,
    FaultType.MISSING_FIELD: 0.40,
    FaultType.PARTIAL_CURVE: 0.15,
}

_KEY_PREFIX = "provider:quality:"


class QualityScorer:
    """Computes per-event quality scores and maintains rolling provider averages in Redis."""

    def __init__(self, redis_client: redis.Redis, rolling_window: int = 100) -> None:
        self._redis = redis_client
        self._window = rolling_window

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def score_event(self, provider: str, injected_faults: list[FaultType]) -> float:
        """Compute quality score in [0.0, 1.0] for one event and update the provider rolling average."""
        score = self._compute_score(injected_faults)
        self._update_rolling_average(provider, score)
        logger.debug(
            "quality_score_computed",
            provider=provider,
            score=score,
            faults=[f.value for f in injected_faults],
        )
        return score

    def get_rolling_average(self, provider: str) -> float | None:
        """
        Return the current rolling average for a provider, or None if no data.
        """
        key = f"{_KEY_PREFIX}{provider}"
        data = self._redis.hmget(key, "sum", "count")
        total_str, count_str = data[0], data[1]

        if total_str is None or count_str is None:
            return None

        total = float(total_str)
        count = float(count_str)
        if count == 0:
            return None

        return total / count

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _compute_score(injected_faults: list[FaultType]) -> float:
        """Subtract penalties for each fault; clamp to [0.0, 1.0]."""
        penalty = sum(FAULT_PENALTIES.get(fault, 0.0) for fault in injected_faults)
        return max(0.0, min(1.0, 1.0 - penalty))

    def _update_rolling_average(self, provider: str, score: float) -> None:
        """Atomically update the running sum/count in Redis; halve both when the window is full."""
        key = f"{_KEY_PREFIX}{provider}"

        # Use a pipeline for atomicity across the two HINCRBYFLOAT calls
        pipe = self._redis.pipeline(transaction=True)
        pipe.hincrbyfloat(key, "sum", score)
        pipe.hincrbyfloat(key, "count", 1.0)
        results = pipe.execute()

        new_sum: float = float(results[0])
        new_count: float = float(results[1])

        # Decay when the window is exceeded — keep proportional shape
        if new_count >= self._window:
            decayed_sum = new_sum / 2.0
            decayed_count = new_count / 2.0
            pipe2 = self._redis.pipeline(transaction=True)
            pipe2.hset(key, "sum", decayed_sum)
            pipe2.hset(key, "count", decayed_count)
            pipe2.execute()
            logger.debug(
                "quality_rolling_average_decayed",
                provider=provider,
                old_count=new_count,
                new_count=decayed_count,
            )
