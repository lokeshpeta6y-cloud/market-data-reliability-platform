"""
Replay job endpoints.

POST /api/v1/replay              — submit a replay job
GET  /api/v1/replay              — list recent replay jobs
GET  /api/v1/replay/{job_id}     — get job status and progress
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, model_validator

from mdrp_common.logging import get_logger
from mdrp_common.models import ReplayJob, ReplaySource

from ..dependencies import RedisDep, SettingsDep

log = get_logger(__name__)

router = APIRouter(prefix="/api/v1/replay", tags=["replay"])


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class ReplayRequest(BaseModel):
    source: ReplaySource = Field(
        ...,
        description="Replay data source: bronze_s3 | databento_historical | dlq",
    )
    provider: str | None = Field(
        default=None,
        description="Provider name. Required for bronze_s3 and databento_historical.",
    )
    instrument: str | None = Field(
        default=None,
        description="Instrument symbol. Optional filter.",
    )
    start_time: datetime = Field(
        ...,
        description="Start of the replay window (UTC).",
    )
    end_time: datetime = Field(
        ...,
        description="End of the replay window (UTC).",
    )
    requested_by: str = Field(
        default="ops-api",
        description="Identifier of the submitter for audit purposes.",
    )

    @model_validator(mode="after")
    def validate_window(self) -> ReplayRequest:
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")
        if (
            self.source
            in (
                ReplaySource.BRONZE_S3,
                ReplaySource.DATABENTO_HISTORICAL,
            )
            and not self.provider
        ):
            raise ValueError(f"'provider' is required for source '{self.source.value}'")
        return self


class ReplayJobResponse(BaseModel):
    job_id: str
    source: ReplaySource
    provider: str | None
    instrument: str | None
    start_time: datetime
    end_time: datetime
    requested_at: datetime
    requested_by: str
    status: str
    events_replayed: int
    error: str | None


class SubmitReplayResponse(BaseModel):
    job_id: str
    status: str = "pending"
    message: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_job_store(request: Request):  # type: ignore[return]
    """Retrieve the JobStore from app state."""
    return request.app.state.job_store


def _job_to_response(job: ReplayJob) -> ReplayJobResponse:
    return ReplayJobResponse(
        job_id=job.job_id,
        source=job.source,
        provider=job.provider,
        instrument=job.instrument,
        start_time=job.start_time,
        end_time=job.end_time,
        requested_at=job.requested_at,
        requested_by=job.requested_by,
        status=job.status,
        events_replayed=job.events_replayed,
        error=job.error,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post(
    "",
    response_model=SubmitReplayResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a replay job",
)
async def submit_replay(
    body: ReplayRequest,
    request: Request,
    redis: RedisDep,
) -> SubmitReplayResponse:
    """
    Submit a new replay job.

    The job is persisted in Redis and picked up by the replay-engine service.
    Returns the job_id immediately; poll ``GET /api/v1/replay/{job_id}`` for status.
    """
    job = ReplayJob(
        source=body.source,
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
        "replay_job_submitted",
        job_id=job.job_id,
        source=job.source,
        provider=job.provider,
        instrument=job.instrument,
    )

    return SubmitReplayResponse(
        job_id=job.job_id,
        status="pending",
        message=f"Replay job {job.job_id} accepted. Poll /api/v1/replay/{job.job_id} for status.",
    )


@router.get(
    "",
    response_model=list[ReplayJobResponse],
    summary="List recent replay jobs",
)
async def list_replay_jobs(
    request: Request,
    redis: RedisDep,
    settings: SettingsDep,
    limit: int = Query(
        default=50,
        ge=1,
        le=500,
        description="Maximum number of jobs to return.",
    ),
) -> list[ReplayJobResponse]:
    """Return recent replay jobs (pending, running, completed, failed)."""
    job_store = _get_job_store(request)
    jobs = job_store.list_all_jobs(limit=min(limit, settings.replay_list_limit))
    return [_job_to_response(j) for j in jobs]


@router.get(
    "/{job_id}",
    response_model=ReplayJobResponse,
    summary="Get replay job status",
)
async def get_replay_job(
    job_id: str,
    request: Request,
    redis: RedisDep,
) -> ReplayJobResponse:
    """Return the current status and progress of a replay job."""
    job_store = _get_job_store(request)
    job = job_store.get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Replay job '{job_id}' not found.",
        )
    return _job_to_response(job)
