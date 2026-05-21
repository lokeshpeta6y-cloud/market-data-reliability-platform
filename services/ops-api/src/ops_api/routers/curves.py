"""
Forward curve endpoints.

GET /api/v1/curves                        — list all instruments with snapshot summary
GET /api/v1/curves/{instrument}           — full ForwardCurveSnapshot for instrument
GET /api/v1/curves/{instrument}/history   — last N curve events from Redis sorted set
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel, Field

from mdrp_common.logging import get_logger
from mdrp_common.models import ForwardCurveSnapshot

from ..dependencies import RedisDep

log = get_logger(__name__)

router = APIRouter(prefix="/api/v1/curves", tags=["curves"])

# Redis key patterns (written by the normalisation/gold loader service)
_SNAPSHOT_KEY = "curve:snapshot:{instrument}"
_SNAPSHOT_INDEX_KEY = "curve:snapshots"  # Redis set of instrument names
_HISTORY_KEY = "curve:history:{instrument}"  # Redis sorted set, score = epoch


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class CurveSummary(BaseModel):
    instrument: str
    curve_name: str
    provider: str
    as_of: datetime
    completeness: float
    tenor_count: int
    version: int
    is_authoritative: bool


class CurveHistoryEntry(BaseModel):
    score: float  # Unix epoch used as sort key
    snapshot: dict[str, Any]


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=list[CurveSummary],
    summary="List all instruments with latest snapshot summary",
)
async def list_curves(redis: RedisDep) -> list[CurveSummary]:
    """
    Return a lightweight summary for every instrument that has a snapshot in Redis.
    """
    # Discover all instrument names from the index set
    members = await redis.smembers(_SNAPSHOT_INDEX_KEY)
    if not members:
        # Fallback: scan for snapshot keys directly
        keys = await redis.keys("curve:snapshot:*")
        members = {k.decode().replace("curve:snapshot:", "") if isinstance(k, bytes) else k.replace("curve:snapshot:", "") for k in keys}

    summaries: list[CurveSummary] = []
    for member in members:
        instrument = member.decode() if isinstance(member, bytes) else member
        key = _SNAPSHOT_KEY.format(instrument=instrument)
        raw = await redis.get(key)
        if not raw:
            continue
        try:
            snap = ForwardCurveSnapshot.model_validate_json(raw)
            summaries.append(
                CurveSummary(
                    instrument=snap.instrument,
                    curve_name=snap.curve_name,
                    provider=snap.provider,
                    as_of=snap.as_of,
                    completeness=snap.completeness,
                    tenor_count=len(snap.tenors),
                    version=snap.version,
                    is_authoritative=snap.is_authoritative,
                )
            )
        except Exception as exc:
            log.warning("invalid_curve_snapshot", instrument=instrument, error=str(exc))

    summaries.sort(key=lambda s: s.instrument)
    return summaries


@router.get(
    "/{instrument}",
    response_model=ForwardCurveSnapshot,
    summary="Get full ForwardCurveSnapshot for an instrument",
)
async def get_curve(instrument: str, redis: RedisDep) -> ForwardCurveSnapshot:
    """Return the latest ForwardCurveSnapshot for the given instrument."""
    key = _SNAPSHOT_KEY.format(instrument=instrument)
    raw = await redis.get(key)
    if not raw:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No curve snapshot found for instrument '{instrument}'.",
        )
    try:
        return ForwardCurveSnapshot.model_validate_json(raw)
    except Exception as exc:
        log.error("curve_snapshot_parse_error", instrument=instrument, error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Stored curve snapshot could not be parsed.",
        ) from exc


@router.get(
    "/{instrument}/history",
    response_model=list[dict[str, Any]],
    summary="Get last N curve events for an instrument",
)
async def get_curve_history(
    instrument: str,
    redis: RedisDep,
    limit: int = Query(default=20, ge=1, le=200, description="Number of historical curve events to return"),
) -> list[dict[str, Any]]:
    """
    Return the last *limit* curve events from the Redis sorted set.

    The sorted set ``curve:history:{instrument}`` (written by the redis-writer
    service) stores serialised CurveEvent JSON with ``event_timestamp`` Unix-ms
    as the score.  We return entries newest-first, augmented with an
    ``event_timestamp_ms`` field carrying the sort score.
    """
    key = _HISTORY_KEY.format(instrument=instrument)

    # ZREVRANGEBYSCORE with scores — newest first
    raw_entries = await redis.zrevrange(key, 0, limit - 1, withscores=True)
    if not raw_entries:
        return []

    results: list[dict[str, Any]] = []
    for raw_value, score in raw_entries:
        value = raw_value.decode() if isinstance(raw_value, bytes) else raw_value
        try:
            data = json.loads(value)
            results.append({"event_timestamp_ms": score, **data})
        except json.JSONDecodeError as exc:
            log.warning(
                "curve_history_parse_error",
                instrument=instrument,
                error=str(exc),
            )

    return results
