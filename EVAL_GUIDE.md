# MDRP — Evaluator Access Guide

This document covers everything needed to verify the live system. For architecture and design rationale see [SHOWCASE.md](SHOWCASE.md).

> **Before you start:** Replace `<HOST>` throughout this document with the EC2 public IP provided to you.
> Snowflake account, user, and all credentials are provided alongside this file.

---

## Access Credentials

| Resource | Value |
|----------|-------|
| EC2 Public IP | `<HOST>` — provided separately |
| Ops API | `http://<HOST>:8000` |
| Grafana | `http://<HOST>:3000` → admin / _provided separately_ |
| Prometheus | `http://<HOST>:9090` |
| Jaeger | `http://<HOST>:16686` |
| Snowflake account | _provided separately_ |
| Snowflake user | _provided separately_ |
| Snowflake PAT token | _provided separately_ |
| AWS eval credentials | _provided separately_ (read-only: S3 + Snowflake secret) |

---

## 1. Ops API

The API reads from the Redis hot cache and responds in sub-millisecond latency.

**System health**
```bash
curl http://<HOST>:8000/health
```
Expected: all 9 services `"status": "healthy"`.

**Live forward curves (all instruments)**
```bash
curl http://<HOST>:8000/api/v1/curves
```

**Single instrument with all tenors**
```bash
curl http://<HOST>:8000/api/v1/curves/TTF
curl http://<HOST>:8000/api/v1/curves/NBP
curl http://<HOST>:8000/api/v1/curves/BRENT
```

**Provider health and quality KPIs**
```bash
curl http://<HOST>:8000/api/v1/providers
```

**Dead-letter queue status**
```bash
curl http://<HOST>:8000/api/v1/dlq
```

**Trigger a Bronze S3 replay** (adjust the time window to match today's date)
```bash
curl -X POST http://<HOST>:8000/api/v1/replay \
  -H "Content-Type: application/json" \
  -d '{
    "source": "bronze_s3",
    "provider": "provider-emulator",
    "start_time": "2026-05-21T10:00:00Z",
    "end_time": "2026-05-21T11:00:00Z"
  }'
```

---

## 2. Grafana Dashboards

Navigate to `http://<HOST>:3000` and log in with `admin` / _password provided separately_.

**What to look for:**
- **MDRP Overview** dashboard — event throughput, validation pass rate, DLQ depth, quality scores per instrument
- **Bronze Writer** panel — Parquet flush rate, bytes written, S3 write latency
- **Provider Emulator** panel — events/cycle, fault injection counters per fault type
- All panels should show live data updating every 15 seconds

---

## 3. Prometheus

Navigate to `http://<HOST>:9090`.

Useful queries to run directly in the Prometheus expression browser:

```promql
# Total events published by the provider emulator
mdrp_events_published_total

# Validation pass vs DLQ rate
mdrp_events_validated_total
mdrp_events_dlq_total

# Bronze write success / failure
mdrp_bronze_writes_total

# Per-instrument quality score rolling average
mdrp_provider_quality_score
```

---

## 4. Jaeger Traces

Navigate to `http://<HOST>:16686`.

Select service **`ops-api`** and search for recent traces. Each API request is traced end-to-end with span attributes including instrument, provider, and Redis read latency.

To generate a trace, make any API call (e.g. `curl http://<HOST>:8000/api/v1/curves`) then refresh Jaeger.

---

## 5. Snowflake — Silver and Gold Layers

Connect using:
- **Account:** _provided separately_
- **User:** _provided separately_
- **Authenticator:** `programmatic_access_token`
- **PAT token:** _provided separately_
- **Warehouse:** `MDRP_LOAD_WH`
- **Database:** `MARKET_DATA`

**Silver layer — validated events**
```sql
-- Row count and latest load time
SELECT COUNT(*) AS total_rows, MAX(received_at) AS latest_event
FROM MARKET_DATA.SILVER_EVENTS.CURVE_EVENTS;

-- Quality distribution across instruments
SELECT instrument, validation_status,
       ROUND(AVG(quality_score), 3) AS avg_quality,
       COUNT(*) AS event_count
FROM MARKET_DATA.SILVER_EVENTS.CURVE_EVENTS
GROUP BY instrument, validation_status
ORDER BY instrument, validation_status;

-- Latest price per tenor for TTF
SELECT tenor, price, quality_score, event_timestamp
FROM MARKET_DATA.SILVER_EVENTS.CURVE_EVENTS
WHERE instrument = 'TTF'
ORDER BY event_timestamp DESC
LIMIT 24;
```

**Gold layer — reconciled forward curve snapshots**
```sql
-- Snapshot count and completeness
SELECT COUNT(*) AS total_snapshots,
       ROUND(AVG(completeness_pct), 3) AS avg_completeness,
       MIN(as_of) AS oldest_snapshot,
       MAX(as_of) AS newest_snapshot
FROM MARKET_DATA.GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS;

-- Latest authoritative snapshot per instrument
SELECT instrument, curve_name, as_of,
       front_price, num_tenors, completeness_pct, quality_score
FROM MARKET_DATA.GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS
WHERE is_authoritative = TRUE
ORDER BY as_of DESC
LIMIT 12;

-- Quality gate in action — completeness < 0.80 snapshots are rejected before load
SELECT completeness_pct, COUNT(*) AS count
FROM MARKET_DATA.GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS
GROUP BY completeness_pct
ORDER BY completeness_pct;
```

---

## 6. S3 Bronze Layer

With the provided AWS eval credentials (region `us-east-2`):

```bash
# List today's Parquet files
aws s3 ls s3://mdrp-bronze/bronze/provider-emulator/ --recursive --human-readable

# Download a file to inspect
aws s3 cp s3://mdrp-bronze/bronze/provider-emulator/2026-05-21/11/events_<uuid>.parquet .

# Read it with Python
python3 -c "import pandas as pd; df = pd.read_parquet('events_<uuid>.parquet'); print(df.head())"
```

The Bronze bucket preserves **all** events including fault-injected ones — the full immutable audit trail. Silver and Gold contain only quality-gated events.

---

## 7. Verification Checklist

| Check | How to verify | Expected result |
|-------|---------------|-----------------|
| All 9 services healthy | `GET /health` | `"status": "healthy"` for each |
| Live data flowing | `GET /api/v1/curves` | 6 instruments, `completeness: 1.0` |
| Fault injection active | `GET /api/v1/providers` | non-zero fault counters |
| DLQ routing working | `GET /api/v1/dlq` | malformed events present |
| Bronze S3 writing | AWS CLI `ls mdrp-bronze` | Parquet files updated within last 30s |
| Silver loaded | Snowflake Silver COUNT | growing row count, updated live |
| Gold quality gate | Snowflake Gold query | completeness_pct ≥ 0.80 for all rows |
| Grafana live | Open dashboard | panels updating every 15s |
| Replay functional | POST `/api/v1/replay` | returns job_id, events re-appear in topics |

---

## Repository

`https://github.com/lokeshpeta6y-cloud/market-data-reliability-platform`

Architecture overview, design decisions, and service-level documentation: [SHOWCASE.md](SHOWCASE.md)
