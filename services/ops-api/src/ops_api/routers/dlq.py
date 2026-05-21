"""
DLQ inspection and replay endpoints.

GET  /api/v1/dlq              — DLQ stats: depth, failure categories, recent entries
POST /api/v1/dlq/replay       — submit a DLQ replay job for a time window
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, model_validator

from mdrp_common.logging import get_logger
from mdrp_common.models import DLQEvent, DLQFailureCategory, ReplayJob, ReplaySource

from ..dependencies import RedisDep, SettingsDep
from .replay import SubmitReplayResponse, _get_job_store

log = get_logger(__name__)

router = APIRouter(prefix="/api/v1/dlq", tags=["dlq"])

# Redis keys written by the validation service
_DLQ_RECENT_KEY = "dlq:recent"          # Redis list of serialised DLQEvent JSON (most recent first)
_DLQ_COUNTER_KEY = "dlq:category_counts"  # Redis hash: failure_category -> count


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class FailureCategoryCount(BaseModel):
    category: str
    count: int


class DLQEntry(BaseModel):
    dlq_event_id: str
    original_event_id: str
    provider: str
    instrument: str | None
    failure_reason: str
    failure_category: str
    original_received_at: datetime
    dlq_timestamp: datetime
    retry_count: int
    trace_id: str


class DLQStatsResponse(BaseModel):
    depth_estimate: int
    top_failure_categories: list[FailureCategoryCount]
    recent_entries: list[DLQEntry]
    as_of: datetime = Field(default_factory=lambda: datetime.now(UTC))


class DLQReplayRequest(BaseModel):
    start_time: datetime = Field(
        ...,
        description="Start of the DLQ time window to replay (UTC).",
    )
    end_time: datetime = Field(
        ...,
        description="End of the DLQ time window to replay (UTC).",
    )
    provider: str | None = Field(
        default=None,
        description="Optional provider filter.",
    )
    instrument: str | None = Field(
        default=None,
        description="Optional instrument filter.",
    )
    requested_by: str = Field(default="ops-api")

    @model_validator(mode="after")
    def validate_window(self) -> "DLQReplayRequest":
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        return self


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=DLQStatsResponse,
    summary="DLQ statistics and recent entries",
)
async def get_dlq_stats(
    redis: RedisDep,
    settings: SettingsDep,
    recent_limit: int = Query(
        default=20,
        ge=1,
        le=200,
        alias="limit",
        description="Number of recent DLQ entries to include.",
    ),
) -> DLQStatsResponse:
    """
    Return DLQ depth estimate, top failure categories, and most recent entries.

    Depth is estimated from the Redis list length (``dlq:recent``).
    For an authoritative count, query Kafka topic offsets directly.
    """
    # Depth estimate from recent list length
    depth = await redis.llen(_DLQ_RECENT_KEY)

    # Category counts from the counter hash
    raw_counts = await redis.hgetall(_DLQ_COUNTER_KEY)
    category_counts: list[FailureCategoryCount] = []
    for cat, cnt in raw_counts.items():
        cat_str = cat.decode() if isinstance(cat, bytes) else cat
        cnt_int = int(cnt.decode() if isinstance(cnt, bytes) else cnt)
        category_counts.append(FailureCategoryCount(category=cat_str, count=cnt_int))
    category_counts.sort(key=lambda x: x.count, reverse=True)

    # Recent entries
    raw_entries = await redis.lrange(_DLQ_RECENT_KEY, 0, recent_limit - 1)
    entries: list[DLQEntry] = []
    for raw in raw_entries:
        raw_str = raw.decode() if isinstance(raw, bytes) else raw
        try:
            event = DLQEvent.model_validate_json(raw_str)
            entries.append(
                DLQEntry(
                    dlq_event_id=event.dlq_event_id,
                    original_event_id=event.original_event_id,
                    provider=event.provider,
                    instrument=event.instrument,
                    failure_reason=event.failure_reason,
                    failure_category=event.failure_category.value,
                    original_received_at=event.original_received_at,
                    dlq_timestamp=event.dlq_timestamp,
                    retry_count=event.retry_count,
                    trace_id=event.trace_id,
                )
            )
        except Exception as exc:
            log.warning("dlq_entry_parse_error", error=str(exc))

    return DLQStatsResponse(
        depth_estimate=depth,
        top_failure_categories=category_counts,
        recent_entries=entries,
    )


@router.post(
    "/replay",
    response_model=SubmitReplayResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a DLQ replay job",
    description="Re-process all DLQ events within the specified time window through the full pipeline.",
)
async def submit_dlq_replay(
    body: DLQReplayRequest,
    request: Request,
    redis: RedisDep,
) -> SubmitReplayResponse:
    """
    Submit a DLQ replay job.

    Creates a ReplayJob with source=DLQ and enqueues it for the replay-engine.
    Events matching the time window (and optional provider/instrument filters)
    will be re-published to ``market.events.raw``.
    """
    job = ReplayJob(
        source=ReplaySource.DLQ,
        provider=body.provider,
        instrument=body.instrument,
        start_time=body.start_time,
        end_time=body.end_time,
        requested_by=body.requested_by,
        status="pending",
    )

    job_store = _get_job_store(request)
    job_store.save_job(job)

    log.info(
        "dlq_replay_job_submitted",
        job_id=job.job_id,
        start_time=job.start_time.isoformat(),
        end_time=job.end_time.isoformat(),
        provider=job.provider,
        instrument=job.instrument,
    )

    return SubmitReplayResponse(
        job_id=job.job_id,
        status="pending",
        message=(
            f"DLQ replay job {job.job_id} accepted. "
            f"Poll /api/v1/replay/{job.job_id} for status."
        ),
    )
