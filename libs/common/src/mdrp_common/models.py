"""
Core domain models for the Market Data Reliability Platform.

All inter-service communication uses these Pydantic v2 models serialised as JSON.
The canonical types here enforce a single source of truth across every service —
producers serialise to these, consumers deserialise from these, nothing else crosses
a topic boundary.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class DeliveryPeriod(str, Enum):
    SPOT = "spot"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    SEASONAL = "seasonal"
    ANNUAL = "annual"


class FaultType(str, Enum):
    DUPLICATE = "duplicate"
    DELAYED = "delayed"
    MALFORMED = "malformed"
    MISSING_FIELD = "missing_field"
    SCHEMA_DRIFT = "schema_drift"
    STALE = "stale"
    OUT_OF_ORDER = "out_of_order"
    PARTIAL_CURVE = "partial_curve"


class DLQFailureCategory(str, Enum):
    SCHEMA_VIOLATION = "schema_violation"
    DUPLICATE = "duplicate"
    MALFORMED = "malformed"
    STALE = "stale"
    OUT_OF_ORDER = "out_of_order"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    UNKNOWN = "unknown"


class ReplaySource(str, Enum):
    BRONZE_S3 = "bronze_s3"
    DATABENTO_HISTORICAL = "databento_historical"
    DLQ = "dlq"


class ProviderStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OUTAGE = "outage"
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Raw market event — emitted by the provider emulator onto market.events.raw
# This is the untouched, pre-validation representation of what we received.
# ---------------------------------------------------------------------------


class RawMarketEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    provider: str
    instrument: str
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    event_timestamp: datetime
    payload: dict[str, Any]
    injected_faults: list[FaultType] = Field(default_factory=list)
    is_replay: bool = False
    replay_source: ReplaySource | None = None
    trace_id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    @field_validator("received_at", "event_timestamp", mode="before")
    @classmethod
    def ensure_utc(cls, v: Any) -> datetime:
        if isinstance(v, str):
            v = datetime.fromisoformat(v)
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v


# ---------------------------------------------------------------------------
# Validated event — emitted by validation-service onto market.events.validated
# Structurally sound, deduplicated, timestamp-corrected.
# ---------------------------------------------------------------------------


class ValidatedMarketEvent(BaseModel):
    event_id: str
    original_event_id: str
    provider: str
    instrument: str
    received_at: datetime
    event_timestamp: datetime
    corrected_timestamp: bool = False
    payload: dict[str, Any]
    injected_faults: list[FaultType]
    is_replay: bool = False
    replay_source: ReplaySource | None = None
    trace_id: str
    validated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Normalised curve event — emitted by normalisation-service onto
# market.events.normalized.  This is the canonical domain representation.
# ---------------------------------------------------------------------------


class CurveEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source_event_id: str
    curve_name: str
    instrument: str
    tenor: str
    delivery_period: DeliveryPeriod
    price: Decimal
    currency: str
    unit: str
    provider: str
    version: int
    event_timestamp: datetime
    ingestion_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    quality_score: float = Field(ge=0.0, le=1.0)
    is_replay: bool = False
    replay_source: ReplaySource | None = None
    trace_id: str

    @model_validator(mode="after")
    def validate_quality_score(self) -> "CurveEvent":
        if self.quality_score < 0.0 or self.quality_score > 1.0:
            raise ValueError("quality_score must be between 0.0 and 1.0")
        return self


# ---------------------------------------------------------------------------
# Dead-letter queue event — emitted onto market.events.dlq
# Preserves full original payload for forensic inspection and replay.
# ---------------------------------------------------------------------------


class DLQEvent(BaseModel):
    dlq_event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    original_event_id: str
    provider: str
    instrument: str | None = None
    failure_reason: str
    failure_category: DLQFailureCategory
    raw_payload: dict[str, Any]
    original_received_at: datetime
    dlq_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    retry_count: int = 0
    trace_id: str


# ---------------------------------------------------------------------------
# Provider health snapshot — written to Redis, exposed via the ops API
# ---------------------------------------------------------------------------


class ProviderHealthSnapshot(BaseModel):
    provider: str
    status: ProviderStatus
    last_event_at: datetime | None = None
    events_last_60s: int = 0
    dlq_rate_last_60s: float = 0.0
    quality_score_p50: float = 0.0
    quality_score_p95: float = 0.0
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Replay job — created by the ops API, consumed by the replay engine
# ---------------------------------------------------------------------------


class ReplayJob(BaseModel):
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    source: ReplaySource
    provider: str | None = None
    instrument: str | None = None
    start_time: datetime
    end_time: datetime
    requested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    requested_by: str = "ops-api"
    status: str = "pending"
    events_replayed: int = 0
    error: str | None = None


# ---------------------------------------------------------------------------
# Forward curve snapshot — written to Snowflake Gold and Redis
# ---------------------------------------------------------------------------


class TenorPrice(BaseModel):
    tenor: str
    price: Decimal
    quality_score: float
    last_updated: datetime


class ForwardCurveSnapshot(BaseModel):
    snapshot_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    curve_name: str
    instrument: str
    as_of: datetime
    tenors: dict[str, TenorPrice]
    completeness: float = Field(ge=0.0, le=1.0)
    is_authoritative: bool = False
    version: int
    provider: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
