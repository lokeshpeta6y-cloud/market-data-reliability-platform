r"""
MDRP Demo API — standalone, zero external dependencies.

Runs the full ops-api surface with realistic mock data so you can
explore the platform without Docker, Redis, Kafka, or S3.

  python -m venv .venv && .venv\Scripts\activate
  pip install fastapi uvicorn
  python demo/api.py

Then open: http://localhost:8007/docs
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(
    title="MDRP Ops API",
    description=(
        "**Market Data Reliability Platform** — Operational control plane.\n\n"
        "Exposes pipeline health, provider quality scores, forward curve snapshots, "
        "replay job management, and DLQ inspection.\n\n"
        "> _Demo mode: realistic mock data, no external dependencies._"
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

_PROVIDERS = ["databento-emulated", "ice-emulated", "refinitiv-emulated"]
_INSTRUMENTS = ["TTF", "NBP", "BRENT", "WTI", "EU_ETS"]
_TENORS = [
    "2025-06", "2025-07", "2025-08", "2025-09",
    "2025-Q3", "2025-Q4", "2026-Q1", "2026-Q2",
    "2026-CAL", "2027-CAL",
]

_replay_jobs: dict[str, dict[str, Any]] = {}


def _failure_reason(category: str) -> str:
    reasons = {
        "schema_violation": "Required field 'price' is null",
        "duplicate": "Event ID already processed within 300s dedup window",
        "stale_data": "Event timestamp is 18 minutes behind wall clock",
        "malformed_payload": "JSON parse error: unexpected token at position 142",
        "out_of_order": "Sequence gap detected: expected 10045, got 10048",
    }
    return reasons.get(category, "Validation failed")


def _seed_dlq() -> list[dict[str, Any]]:
    categories = ["schema_violation", "duplicate", "stale_data", "malformed_payload", "out_of_order"]
    events = []
    for i in range(47):
        received = datetime.now(timezone.utc) - timedelta(minutes=random.randint(1, 480))
        events.append({
            "event_id": str(uuid.uuid4()),
            "provider": random.choice(_PROVIDERS),
            "failure_category": random.choice(categories),
            "failure_reason": _failure_reason(categories[i % len(categories)]),
            "received_at": received.isoformat(),
            "payload_preview": {"symbol": random.choice(_INSTRUMENTS), "price": round(random.uniform(20, 120), 4)},
            "retry_count": random.randint(0, 3),
            "replayed": random.random() < 0.3,
        })
    return events


_dlq_events: list[dict[str, Any]] = _seed_dlq()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _quality_score(provider: str) -> float:
    base = {"databento-emulated": 0.961, "ice-emulated": 0.934, "refinitiv-emulated": 0.978}
    return round(base.get(provider, 0.95) + random.uniform(-0.01, 0.01), 4)


# ---------------------------------------------------------------------------
# Health & pipeline status
# ---------------------------------------------------------------------------

@app.get("/health", tags=["Health"], summary="Service liveness probe")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "ops-api", "version": "0.1.0"}


@app.get("/health/pipeline", tags=["Health"], summary="Full pipeline health summary")
async def pipeline_health() -> dict[str, Any]:
    return {
        "overall": "healthy",
        "checked_at": _now(),
        "services": {
            "provider-emulator":      {"status": "healthy", "events_per_second": round(random.uniform(38, 52), 1)},
            "validation-service":     {"status": "healthy", "validation_rate_pct": round(random.uniform(97.5, 99.2), 2)},
            "bronze-writer":          {"status": "healthy", "last_write_latency_ms": round(random.uniform(12, 45), 1)},
            "normalization-service":  {"status": "healthy", "throughput_eps": round(random.uniform(35, 50), 1)},
            "redis-writer":           {"status": "healthy", "cache_hit_rate_pct": 99.1},
            "silver-loader":          {"status": "healthy", "last_snowflake_load": (datetime.now(timezone.utc) - timedelta(seconds=38)).isoformat()},
            "gold-loader":            {"status": "healthy", "snapshots_today": random.randint(180, 220)},
            "replay-engine":          {"status": "idle",    "active_jobs": 0},
        },
        "kafka_topics": {
            "market.events.raw":        {"lag": random.randint(0, 5),   "throughput_eps": round(random.uniform(40, 55), 1)},
            "market.events.validated":  {"lag": random.randint(0, 3),   "throughput_eps": round(random.uniform(38, 52), 1)},
            "market.events.normalized": {"lag": random.randint(0, 3),   "throughput_eps": round(random.uniform(36, 50), 1)},
            "market.events.dlq":        {"lag": 0,                       "throughput_eps": round(random.uniform(0.1, 0.8), 2)},
            "market.events.replay":     {"lag": 0,                       "throughput_eps": 0.0},
        },
        "dlq_last_1h": random.randint(3, 12),
        "stale_curves": [],
    }


@app.get("/health/providers", tags=["Health"], summary="Provider quality scores and status")
async def provider_health() -> list[dict[str, Any]]:
    results = []
    for provider in _PROVIDERS:
        last_event = datetime.now(timezone.utc) - timedelta(seconds=random.randint(1, 8))
        results.append({
            "provider": provider,
            "status": "active",
            "quality_score": _quality_score(provider),
            "events_last_minute": random.randint(280, 380),
            "dlq_rate_pct": round(random.uniform(0.5, 2.5), 2),
            "duplicate_rate_pct": round(random.uniform(1.5, 2.8), 2),
            "last_event_received": last_event.isoformat(),
            "uptime_pct_today": round(random.uniform(99.1, 99.9), 2),
        })
    return results


# ---------------------------------------------------------------------------
# Curve snapshots (Redis serving cache)
# ---------------------------------------------------------------------------

@app.get("/curves", tags=["Curves"], summary="List all instruments in the serving cache")
async def list_curves() -> list[str]:
    return [f"{instr}_FORWARD" for instr in _INSTRUMENTS]


@app.get("/curves/{curve_name}", tags=["Curves"], summary="Latest forward curve snapshot")
async def get_curve(curve_name: str) -> dict[str, Any]:
    instrument = curve_name.replace("_FORWARD", "")
    if instrument not in _INSTRUMENTS:
        raise HTTPException(status_code=404, detail=f"Curve '{curve_name}' not found in serving cache")

    base_prices = {"TTF": 28.4, "NBP": 26.1, "BRENT": 84.2, "WTI": 80.5, "EU_ETS": 65.3}
    base = base_prices.get(instrument, 50.0)
    currency = "EUR" if instrument in ("TTF", "NBP", "EU_ETS") else "USD"
    unit = "t CO2" if instrument == "EU_ETS" else ("MWh" if instrument in ("TTF", "NBP") else "bbl")

    tenors = {}
    for i, tenor in enumerate(_TENORS):
        price = base + (i * 0.35) + random.uniform(-0.5, 0.5)
        tenors[tenor] = {
            "price": round(price, 4),
            "currency": currency,
            "unit": unit,
            "quality_score": round(random.uniform(0.94, 0.99), 4),
            "provider": "databento-emulated",
            "version": random.randint(40, 60),
        }

    return {
        "curve_name": curve_name,
        "instrument": instrument,
        "as_of": _now(),
        "tenors": tenors,
        "completeness": round(len(tenors) / len(_TENORS), 3),
        "is_authoritative": True,
        "last_updated": (datetime.now(timezone.utc) - timedelta(seconds=random.randint(5, 30))).isoformat(),
        "source": "redis-serving-cache",
    }


# ---------------------------------------------------------------------------
# Replay jobs
# ---------------------------------------------------------------------------

@app.get("/replay/jobs", tags=["Replay"], summary="List all replay jobs")
async def list_replay_jobs() -> list[dict[str, Any]]:
    return list(_replay_jobs.values())


@app.post("/replay/jobs", tags=["Replay"], summary="Trigger a Bronze S3 replay", status_code=202)
async def trigger_replay(body: dict[str, Any]) -> dict[str, Any]:
    job_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    job: dict[str, Any] = {
        "job_id": job_id,
        "source": body.get("source", "bronze_s3"),
        "provider": body.get("provider", "databento-emulated"),
        "start_time": body.get("start_time", (now - timedelta(hours=2)).isoformat()),
        "end_time": body.get("end_time", now.isoformat()),
        "status": "queued",
        "created_at": now.isoformat(),
        "events_replayed": 0,
        "events_failed": 0,
        "rate_limit_eps": body.get("rate_limit_eps", 500),
    }
    _replay_jobs[job_id] = job
    return {"job_id": job_id, "status": "queued", "message": "Replay job queued successfully"}


@app.get("/replay/jobs/{job_id}", tags=["Replay"], summary="Get replay job status")
async def get_replay_job(job_id: str) -> dict[str, Any]:
    if job_id not in _replay_jobs:
        raise HTTPException(status_code=404, detail=f"Replay job '{job_id}' not found")
    job = _replay_jobs[job_id]
    # Simulate progress
    if job["status"] == "queued":
        job["status"] = "running"
        job["started_at"] = _now()
        job["events_replayed"] = random.randint(100, 800)
    elif job["status"] == "running":
        job["events_replayed"] = job.get("events_replayed", 0) + random.randint(200, 600)
        if job["events_replayed"] > 5000:
            job["status"] = "completed"
            job["completed_at"] = _now()
    return job


@app.delete("/replay/jobs/{job_id}", tags=["Replay"], summary="Cancel a running replay job", status_code=202)
async def cancel_replay_job(job_id: str) -> dict[str, str]:
    if job_id not in _replay_jobs:
        raise HTTPException(status_code=404, detail=f"Replay job '{job_id}' not found")
    _replay_jobs[job_id]["status"] = "cancelled"
    _replay_jobs[job_id]["cancelled_at"] = _now()
    return {"job_id": job_id, "status": "cancelled"}


# ---------------------------------------------------------------------------
# DLQ inspection
# ---------------------------------------------------------------------------

@app.get("/dlq/events", tags=["DLQ"], summary="List DLQ events (failed validation)")
async def list_dlq_events(
    limit: int = 20,
    provider: str | None = None,
    category: str | None = None,
    replayed: bool | None = None,
) -> dict[str, Any]:
    events = _dlq_events
    if provider:
        events = [e for e in events if e["provider"] == provider]
    if category:
        events = [e for e in events if e["failure_category"] == category]
    if replayed is not None:
        events = [e for e in events if e["replayed"] == replayed]
    return {
        "total": len(events),
        "returned": min(limit, len(events)),
        "events": events[:limit],
    }


@app.get("/dlq/summary", tags=["DLQ"], summary="DLQ failure breakdown by category and provider")
async def dlq_summary() -> dict[str, Any]:
    by_category: dict[str, int] = {}
    by_provider: dict[str, int] = {}
    for e in _dlq_events:
        by_category[e["failure_category"]] = by_category.get(e["failure_category"], 0) + 1
        by_provider[e["provider"]] = by_provider.get(e["provider"], 0) + 1
    return {
        "total_events": len(_dlq_events),
        "unreplayed": sum(1 for e in _dlq_events if not e["replayed"]),
        "by_category": by_category,
        "by_provider": by_provider,
        "oldest_event": min(e["received_at"] for e in _dlq_events),
        "newest_event": max(e["received_at"] for e in _dlq_events),
    }


@app.post("/dlq/replay", tags=["DLQ"], summary="Replay all unreplayed DLQ events", status_code=202)
async def replay_dlq(body: dict[str, Any] | None = None) -> dict[str, Any]:
    unreplayed = [e for e in _dlq_events if not e["replayed"]]
    job_id = str(uuid.uuid4())
    job: dict[str, Any] = {
        "job_id": job_id,
        "source": "dlq",
        "status": "queued",
        "created_at": _now(),
        "events_queued": len(unreplayed),
        "events_replayed": 0,
        "events_failed": 0,
    }
    _replay_jobs[job_id] = job
    for e in unreplayed:
        e["replayed"] = True
    return {"job_id": job_id, "events_queued": len(unreplayed), "status": "queued"}


# ---------------------------------------------------------------------------
# Alert webhook (AlertManager → Teams/Email)
# ---------------------------------------------------------------------------

_received_alerts: list[dict[str, Any]] = []


@app.post("/alerts/webhook", tags=["Alerts"], summary="AlertManager webhook receiver", status_code=200)
async def receive_alert(payload: dict[str, Any]) -> dict[str, str]:
    for alert in payload.get("alerts", []):
        _received_alerts.append({
            "received_at": _now(),
            "name": alert.get("labels", {}).get("alertname", "unknown"),
            "severity": alert.get("labels", {}).get("severity", "warning"),
            "status": alert.get("status", "firing"),
            "summary": alert.get("annotations", {}).get("summary", ""),
            "routed_to": ["teams", "email"],
        })
    return {"status": "accepted", "count": len(payload.get("alerts", []))}


@app.get("/alerts/history", tags=["Alerts"], summary="Recently received and routed alerts")
async def alert_history(limit: int = 20) -> list[dict[str, Any]]:
    return _received_alerts[-limit:]


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("\n  MDRP Ops API -- Demo Mode")
    print("  -----------------------------------------")
    print("  API docs  ->  http://localhost:8007/docs")
    print("  ReDoc     ->  http://localhost:8007/redoc")
    print("  Health    ->  http://localhost:8007/health/pipeline")
    print("  Curves    ->  http://localhost:8007/curves/TTF_FORWARD")
    print("  DLQ       ->  http://localhost:8007/dlq/summary")
    print("  -----------------------------------------\n")
    uvicorn.run(app, host="0.0.0.0", port=8007, reload=False)
