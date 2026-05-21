"""
Health and pipeline status endpoints.

GET /health          — liveness probe (no external checks)
GET /api/v1/status   — full pipeline connectivity check
GET /api/v1/providers — list all provider health snapshots
GET /api/v1/providers/{provider} — single provider detail
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from mdrp_common.logging import get_logger
from mdrp_common.models import ProviderHealthSnapshot, ProviderStatus

from ..dependencies import RedisDep, SettingsDep, StorageDep

log = get_logger(__name__)

router = APIRouter(tags=["health"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class LivenessResponse(BaseModel):
    status: str = "ok"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ComponentStatus(BaseModel):
    name: str
    status: str  # "healthy" | "degraded" | "outage"
    latency_ms: float | None = None
    error: str | None = None


class PipelineStatusResponse(BaseModel):
    overall: str  # "healthy" | "degraded" | "outage"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    components: list[ComponentStatus]


# ---------------------------------------------------------------------------
# Liveness probe
# ---------------------------------------------------------------------------


@router.get(
    "/health",
    response_model=LivenessResponse,
    summary="Liveness probe",
    description="Returns 200 if the ops-api process is running. No external dependency checks.",
)
async def health() -> LivenessResponse:
    return LivenessResponse()


# ---------------------------------------------------------------------------
# Full pipeline status
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/status",
    response_model=PipelineStatusResponse,
    summary="Pipeline status",
    description="Check connectivity to Kafka, Redis, and MinIO. Returns per-component status.",
)
async def pipeline_status(
    redis: RedisDep,
    storage: StorageDep,
    settings: SettingsDep,
) -> PipelineStatusResponse:
    components: list[ComponentStatus] = []

    # Run all checks concurrently
    redis_status, kafka_status, minio_status = await asyncio.gather(
        _check_redis(redis),
        _check_kafka(settings.kafka_bootstrap_servers),
        _check_minio(storage),
        return_exceptions=False,
    )

    components.extend([redis_status, kafka_status, minio_status])

    # Overall status: outage if any component is outage, degraded if any degraded
    statuses = {c.status for c in components}
    if "outage" in statuses:
        overall = "outage"
    elif "degraded" in statuses:
        overall = "degraded"
    else:
        overall = "healthy"

    return PipelineStatusResponse(overall=overall, components=components)


# ---------------------------------------------------------------------------
# Provider health
# ---------------------------------------------------------------------------


@router.get(
    "/api/v1/providers",
    response_model=list[ProviderHealthSnapshot],
    summary="List all provider health snapshots",
)
async def list_providers(redis: RedisDep) -> list[ProviderHealthSnapshot]:
    """
    Return all provider health snapshots stored in Redis.

    The redis-writer stores provider health as a Redis Hash at
    ``provider:health:{provider}`` with fields: ``last_event_at``,
    ``events_per_minute``.  We reconstruct a ProviderHealthSnapshot
    from those fields.

    We skip minute-counter sub-keys (``provider:health:{p}:minute:{n}``)
    by filtering to keys that do NOT contain ``:minute:``.
    """
    pattern = "provider:health:*"
    all_keys: list[bytes] = await redis.keys(pattern)
    # Filter out the rolling minute counter sub-keys
    health_keys = [
        k for k in all_keys
        if b":minute:" not in k
    ]
    if not health_keys:
        return []

    snapshots: list[ProviderHealthSnapshot] = []
    for key in health_keys:
        key_str = key.decode() if isinstance(key, bytes) else key
        provider_name = key_str.replace("provider:health:", "")
        snap = await _read_provider_snapshot(redis, provider_name)
        if snap is not None:
            snapshots.append(snap)

    snapshots.sort(key=lambda s: s.provider)
    return snapshots


@router.get(
    "/api/v1/providers/{provider}",
    response_model=ProviderHealthSnapshot,
    summary="Get provider health detail",
)
async def get_provider(provider: str, redis: RedisDep) -> ProviderHealthSnapshot:
    """Return the health snapshot for a single provider."""
    snap = await _read_provider_snapshot(redis, provider)
    if snap is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No health snapshot found for provider '{provider}'.",
        )
    return snap


async def _read_provider_snapshot(
    redis: RedisDep,
    provider: str,
) -> ProviderHealthSnapshot | None:
    """
    Read provider health from the Redis Hash written by the redis-writer service.

    Hash fields: ``last_event_at`` (ISO datetime str), ``events_per_minute`` (int str).
    Returns None when the key does not exist.
    """
    key = f"provider:health:{provider}"
    raw: dict[bytes, bytes] = await redis.hgetall(key)
    if not raw:
        return None

    def _decode(v: bytes | str) -> str:
        return v.decode() if isinstance(v, bytes) else v

    fields = {_decode(k): _decode(v) for k, v in raw.items()}

    last_event_at: datetime | None = None
    raw_last = fields.get("last_event_at")
    if raw_last:
        try:
            last_event_at = datetime.fromisoformat(raw_last)
        except ValueError:
            pass

    events_per_minute = 0
    try:
        events_per_minute = int(fields.get("events_per_minute", "0"))
    except ValueError:
        pass

    # Estimate events in last 60 s from events_per_minute (same granularity)
    events_last_60s = events_per_minute

    # Derive status: if no event in the last 5 minutes → degraded
    status_val = ProviderStatus.UNKNOWN
    if last_event_at is not None:
        import time as _time
        age_s = _time.time() - last_event_at.timestamp()
        if age_s < 300:
            status_val = ProviderStatus.HEALTHY
        elif age_s < 900:
            status_val = ProviderStatus.DEGRADED
        else:
            status_val = ProviderStatus.OUTAGE

    return ProviderHealthSnapshot(
        provider=provider,
        status=status_val,
        last_event_at=last_event_at,
        events_last_60s=events_last_60s,
        updated_at=last_event_at or datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# Connectivity check helpers
# ---------------------------------------------------------------------------


async def _check_redis(redis: RedisDep) -> ComponentStatus:
    import time

    t0 = time.monotonic()
    try:
        await redis.ping()
        latency_ms = (time.monotonic() - t0) * 1000
        return ComponentStatus(name="redis", status="healthy", latency_ms=round(latency_ms, 2))
    except Exception as exc:
        return ComponentStatus(name="redis", status="outage", error=str(exc))


async def _check_kafka(bootstrap_servers: str) -> ComponentStatus:
    """
    Check Kafka reachability by attempting to list topics via the admin client.
    Runs synchronous confluent-kafka call in a thread executor.
    """
    import asyncio
    import time

    def _sync_check() -> tuple[str, float | None, str | None]:
        t0 = time.monotonic()
        try:
            from confluent_kafka.admin import AdminClient

            admin = AdminClient(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "socket.timeout.ms": 3000,
                    "request.timeout.ms": 3000,
                }
            )
            meta = admin.list_topics(timeout=3)
            latency_ms = (time.monotonic() - t0) * 1000
            _ = meta.topics  # access to confirm no error
            return "healthy", round(latency_ms, 2), None
        except Exception as exc:
            return "outage", None, str(exc)

    loop = asyncio.get_event_loop()
    s, lat, err = await loop.run_in_executor(None, _sync_check)
    return ComponentStatus(name="kafka", status=s, latency_ms=lat, error=err)


async def _check_minio(storage: StorageDep) -> ComponentStatus:
    """
    Check MinIO/S3 reachability by listing objects (limited to 1).
    Runs synchronous boto3 call in a thread executor.
    """
    import asyncio
    import time

    def _sync_check() -> tuple[str, float | None, str | None]:
        t0 = time.monotonic()
        try:
            storage._s3.head_bucket(Bucket=storage._bucket)
            latency_ms = (time.monotonic() - t0) * 1000
            return "healthy", round(latency_ms, 2), None
        except Exception as exc:
            err = str(exc)
            # A 404 means bucket doesn't exist yet but MinIO is reachable
            if "404" in err or "NoSuchBucket" in err:
                latency_ms = (time.monotonic() - t0) * 1000
                return "degraded", round(latency_ms, 2), "bucket not found"
            return "outage", None, err

    loop = asyncio.get_event_loop()
    s, lat, err = await loop.run_in_executor(None, _sync_check)
    return ComponentStatus(name="minio", status=s, latency_ms=lat, error=err)
