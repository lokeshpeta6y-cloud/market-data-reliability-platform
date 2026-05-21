# Market Data Reliability Platform — Submission

**Candidate:** Lokesh  
**Account:** YMAUZRZ-ME29964 (Snowflake) · 299582146389 (AWS)  
**Submitted:** 2026-05-21  
**Live stack:** `http://3.133.104.157:8000` (EC2 running, ~15 min warm-up after cold start)

---

## What was built

A production-grade, event-driven market data pipeline for energy forward curves. It ingests raw price events from providers, validates and normalises them, detects and quantifies data quality faults, stores the Bronze/Silver/Gold data lakehouse layers, and exposes a REST API for trading desk consumption — all observable end-to-end in real time.

**The problem it solves:** Energy market data arrives from multiple providers simultaneously. Events are frequently malformed, stale, duplicated, out-of-order, or structurally drifted (field renames). Without a reliability layer, downstream risk systems and trading desks consume corrupted data silently. This platform intercepts every event, scores its quality, routes failures to a dead-letter queue, and makes only authoritative, quality-gated curves available to consumers.

---

## Architecture

```
Databento API ──┐
                ├──► provider-emulator ──► market.events.raw (Kafka)
Synthetic data ─┘         │
                           │ 7 fault types injected:
                           │ DUPLICATE · MALFORMED · DELAYED ·
                           │ OUT_OF_ORDER · SCHEMA_DRIFT · STALE · PARTIAL_CURVE
                           ▼
                   validation-service ──► market.events.dlq  (bad events)
                           │
                           ▼
              market.events.validated
              ┌────────────┼───────────────┐
              ▼            ▼               ▼
        bronze-writer  normalization  (also silver-loader)
        (S3 Parquet)   -service            │
                            │         silver-loader ──► Snowflake SILVER_EVENTS
                            ▼         gold-loader   ──► Snowflake GOLD_CURVES
                     market.events.normalized
                            │
                            ▼
                      redis-writer ──► Redis hot cache
                            │         (sub-ms reads for trading desk)
                            ▼
                        ops-api ◄── replay-engine (Bronze S3 / DLQ / Databento)
                            │
                     Grafana · Prometheus · Jaeger · AlertManager
```

**9 services. 5 Kafka topics. 3 storage layers. 6 instruments. Fully containerised.**

---

## Instruments covered

| Code | Name | Exchange |
|---|---|---|
| `TTF` | Title Transfer Facility (gas) | ICE |
| `NBP` | National Balancing Point (gas) | ICE |
| `TTF_POWER` | TTF Power forward | EEX |
| `BRENT` | Brent Crude Oil | ICE |
| `WTI` | West Texas Intermediate | CME (Globex) |
| `EU_ETS` | EU Emissions Allowances | ICE |

---

## Live verification

### 1. REST API (EC2)

The stack is deployed on AWS EC2 (`m7i-flex.large`, us-east-2). Allow ~15 minutes from cold start for Docker builds, then:

```bash
# Platform health
curl http://3.133.104.157:8000/health

# Latest forward curves (all instruments, from Redis cache)
curl http://3.133.104.157:8000/api/v1/curves

# Single instrument
curl http://3.133.104.157:8000/api/v1/curves/TTF

# Provider health + quality scores
curl http://3.133.104.157:8000/api/v1/providers

# Dead-letter queue — live fault breakdown
curl http://3.133.104.157:8000/api/v1/dlq

# Trigger a Bronze S3 replay (last 1 hour)
curl -X POST http://3.133.104.157:8000/api/v1/replay \
  -H "Content-Type: application/json" \
  -d '{"source":"bronze_s3","provider":"provider-emulator",
       "start_time":"2026-05-21T00:00:00Z",
       "end_time":"2026-05-21T23:59:59Z",
       "requested_by":"evaluator"}'
```

**Expected `/api/v1/curves` response shape:**
```json
[
  {
    "instrument": "TTF",
    "curve_name": "TTF_MONTHLY_FWD",
    "tenors": {
      "1M": {"price": 32.41, "quality_score": 0.95, "updated_at": "2026-05-21T..."},
      "3M": {"price": 33.10, "quality_score": 0.95, "updated_at": "2026-05-21T..."},
      "Cal27": {"price": 35.80, "quality_score": 0.90, "updated_at": "2026-05-21T..."}
    },
    "provider": "provider-emulator",
    "is_authoritative": true,
    "completeness": 0.92
  }
]
```

**Expected `/api/v1/dlq` response shape:**
```json
{
  "depth_estimate": 47,
  "top_failure_categories": [
    {"category": "DELAYED", "count": 18},
    {"category": "OUT_OF_ORDER", "count": 12},
    {"category": "DUPLICATE", "count": 9},
    {"category": "SCHEMA_DRIFT", "count": 4},
    {"category": "MALFORMED", "count": 4}
  ],
  "recent_entries": [...],
  "as_of": "2026-05-21T..."
}
```

### 2. Grafana dashboards

URL: `http://3.133.104.157:3000`  
Login: `admin` / `mdrp_grafana`

**Pipeline Overview dashboard shows:**
- Event throughput per service (events/sec)
- Consumer lag per Kafka topic
- DLQ depth over time
- Provider quality scores (per instrument, per fault type)
- Bronze write success/failure rate
- Snowflake load latency (Silver + Gold)
- Redis cache hit rate

### 3. Prometheus metrics

URL: `http://3.133.104.157:9090`

Key metrics:
```promql
# Events processed per second (by service)
rate(mdrp_events_processed_total[1m])

# DLQ depth trend
mdrp_dlq_depth_total

# Provider quality score
sum by (provider) (
  rate(mdrp_event_quality_score_sum[5m])
) / sum by (provider) (
  rate(mdrp_event_quality_score_count[5m])
)

# Bronze write success rate
rate(mdrp_bronze_writes_total{outcome="success"}[5m])

# Consumer lag
kafka_consumer_group_lag
```

### 4. Jaeger distributed traces

URL: `http://3.133.104.157:16686`

Every event carries a `trace_id` propagated end-to-end. Search for service `validation-service` or `normalization-service` to see the full event journey from ingestion through to Redis write.

---

## Snowflake — Silver and Gold data

**Account:** `YMAUZRZ-ME29964`  
**User:** `Lokesh`  
**Auth:** PAT token (retrieve from AWS Secrets Manager — see below)  
**Database:** `MARKET_DATA`

### Connect via Python
```python
import snowflake.connector

conn = snowflake.connector.connect(
    account="YMAUZRZ-ME29964",
    user="Lokesh",
    authenticator="programmatic_access_token",
    token="<PAT from Secrets Manager>",
    database="MARKET_DATA",
    warehouse="MDRP_LOAD_WH",
    role="MDRP_WRITER",
)
```

### Retrieve PAT from AWS Secrets Manager
```bash
# Using eval credentials (see below)
aws secretsmanager get-secret-value \
  --secret-id mdrp/prod/snowflake-pat-token \
  --region us-east-2 \
  --query SecretString \
  --output text
```

### Verification queries

**Silver layer — validated curve events:**
```sql
-- Row count and recency
SELECT
    provider,
    instrument,
    COUNT(*)                          AS total_events,
    MIN(event_timestamp)              AS earliest,
    MAX(event_timestamp)              AS latest,
    AVG(quality_score)                AS avg_quality
FROM MARKET_DATA.SILVER_EVENTS.CURVE_EVENTS
GROUP BY provider, instrument
ORDER BY latest DESC;

-- Quality score distribution
SELECT
    validation_status,
    ROUND(quality_score, 1)           AS score_bucket,
    COUNT(*)                          AS events
FROM MARKET_DATA.SILVER_EVENTS.CURVE_EVENTS
GROUP BY 1, 2
ORDER BY 2 DESC;

-- Full lineage: S3 Bronze key → Snowflake row
SELECT
    event_id,
    provider,
    instrument,
    tenor,
    price,
    quality_score,
    bronze_s3_key,
    trace_id
FROM MARKET_DATA.SILVER_EVENTS.CURVE_EVENTS
LIMIT 10;
```

**Gold layer — reconciled forward curve snapshots:**
```sql
-- Latest complete curve per instrument
SELECT
    provider,
    instrument,
    curve_date,
    front_price,
    num_tenors,
    quality_score,
    completeness_pct,
    snapshot_time
FROM MARKET_DATA.GOLD_CURVES.V_LATEST_CURVES
ORDER BY instrument, curve_date DESC;

-- Provider quality trend (7-day rolling)
SELECT *
FROM MARKET_DATA.GOLD_CURVES.V_PROVIDER_QUALITY_SUMMARY
ORDER BY provider, quality_date DESC;

-- Curve completeness SLA tracking
SELECT
    provider,
    instrument,
    completeness_pct,
    avg_quality_score,
    rag_status,          -- GREEN / AMBER / RED
    filled_slots,
    expected_slots
FROM MARKET_DATA.GOLD_CURVES.V_CURVE_COMPLETENESS
ORDER BY completeness_pct ASC;
```

**DLQ analysis in Snowflake:**
```sql
-- Fault breakdown by category and provider
SELECT *
FROM MARKET_DATA.SILVER_EVENTS.V_DLQ_SUMMARY
ORDER BY events_last_24h DESC;

-- Unresolved events awaiting replay
SELECT
    provider,
    failure_category,
    failure_reason,
    dlq_timestamp,
    retry_count,
    raw_payload
FROM MARKET_DATA.SILVER_EVENTS.DLQ_EVENTS
WHERE retry_status = 'PENDING'
ORDER BY dlq_timestamp DESC
LIMIT 20;
```

---

## Bronze layer — raw Parquet on S3

**Bucket:** `mdrp-bronze` (us-east-2)  
**Partition scheme:** `events/provider={provider}/date={YYYY-MM-DD}/`

### Browse and read with eval AWS credentials
```python
import boto3
import pandas as pd

s3 = boto3.client(
    "s3",
    aws_access_key_id="<eval_access_key_id>",       # provided separately
    aws_secret_access_key="<eval_secret_access_key>", # provided separately
    region_name="us-east-2",
)

# List partitions
resp = s3.list_objects_v2(
    Bucket="mdrp-bronze",
    Prefix="events/provider=provider-emulator/",
    Delimiter="/"
)
for p in resp.get("CommonPrefixes", []):
    print(p["Prefix"])

# Read a Parquet file directly
import io
obj = s3.get_object(Bucket="mdrp-bronze", Key="<key from listing>")
df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
print(df.head())
print(df.dtypes)
```

---

## Data quality fault system

The platform injects 7 real-world fault types at configurable rates and measures the system's response to each:

| Fault | Rate | What happens |
|---|---|---|
| `DUPLICATE` | 2% | Same `event_id` published twice. Deduplicator (Redis SETNX) silently drops the copy. DLQ entry created. |
| `MALFORMED` | 1% | Required field (`price`, `tenor`) corrupted. Schema validator rejects it. Full raw payload preserved in DLQ. |
| `DELAYED` | 5% | Event held 2–30s before release. Arrives with correct timestamp but late. Quality score −0.05. |
| `OUT_OF_ORDER` | 3% | Events published in shuffled sequence. Quality score −0.10. |
| `SCHEMA_DRIFT` | 0.5% | Field renamed (`price` → `px`). Validator catches missing field. Quality score −0.20 on survivors. |
| `STALE` | 1% | `event_timestamp` backdated 2–24h. Timestamp bounds check fails. Quality score −0.30. |
| `PARTIAL_CURVE` | 2% | 20–50% of tenors dropped from batch. Curve completeness < 1.0. Quality score −0.25. |

The quality score starts at 1.0. Faults that survive to normalisation subtract their penalty. The score flows into:
- Redis cache (`is_authoritative` flag gates serving to trading desk)
- Snowflake Silver (`quality_score` column)
- Snowflake Gold (`quality_score`, `completeness_pct` per snapshot)
- Grafana Provider Quality Scores panel

---

## Replay engine

The platform can recover from provider outages by replaying from three sources:

```bash
# 1. Replay from Bronze S3 (reconstruct from Parquet)
curl -X POST http://3.133.104.157:8000/api/v1/replay \
  -H "Content-Type: application/json" \
  -d '{
    "source": "bronze_s3",
    "provider": "provider-emulator",
    "start_time": "2026-05-21T00:00:00Z",
    "end_time": "2026-05-21T12:00:00Z",
    "requested_by": "evaluator"
  }'

# 2. Replay DLQ events (retry failed events after fixing root cause)
curl -X POST http://3.133.104.157:8000/api/v1/dlq/replay \
  -H "Content-Type: application/json" \
  -d '{
    "start_time": "2026-05-21T00:00:00Z",
    "end_time": "2026-05-21T23:59:59Z",
    "requested_by": "evaluator"
  }'

# 3. Check replay job status
curl http://3.133.104.157:8000/api/v1/replay/{job_id}
```

Replay jobs are coordinated via Redis `ZPOPMIN` — multiple replay-engine replicas cannot claim the same job. Replayed events are stamped `is_replay=true` and flow through the full validation → normalisation → Bronze/Silver/Gold pipeline again. Snowflake COPY INTO is idempotent (file-level checksum deduplication), so replay never double-counts.

---

## Repository and CI/CD

**GitHub:** `https://github.com/lokeshpeta6y-cloud/market-data-reliability-platform`

### CI pipeline (GitHub Actions)

| Job | What it checks |
|---|---|
| **Lint & Type Check** | `ruff check` + `ruff format --check` + `mypy` — runs per service in a matrix |
| **Unit Tests** | `pytest tests/unit/` — validation logic, normalisation, Parquet serialisation, Bronze flush |
| **Docker Build** | Every service image builds from scratch — catches missing deps and broken Dockerfiles |
| **Integration Smoke Test** | Full stack via `docker compose up`; waits for health; asserts `/api/v1/curves` returns data (main branch only) |

### Run tests locally
```bash
pip install libs/common services/normalization-service \
            services/validation-service services/bronze-writer \
            services/gold-loader pytest pyarrow pandas

pytest tests/unit/ -v
```

### Code structure
```
libs/
  common/                  # Shared Pydantic models, settings, Kafka helpers
    src/mdrp_common/
      models.py            # RawMarketEvent, CurveEvent, DLQEvent, ForwardCurveSnapshot
      kafka.py             # Producer/consumer base classes with at-least-once delivery
      settings.py          # Typed pydantic-settings for all env vars

services/
  provider-emulator/       # Synthetic + Databento live feed; FaultInjector with 7 types
  validation-service/      # Schema check, Redis SETNX dedup, DLQ routing
  bronze-writer/           # Parquet buffer, S3 flush by size/age threshold
  normalization-service/   # Tenor mapping, quality scoring, Redis INCR version
  redis-writer/            # HSET latest curve, provider health snapshot
  silver-loader/           # COPY INTO Snowflake SILVER_EVENTS.CURVE_EVENTS
  gold-loader/             # COPY INTO Snowflake GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS
  replay-engine/           # ZPOPMIN job claim, Bronze/DLQ/Databento replay
  ops-api/                 # FastAPI: curves, providers, DLQ, replay, alert webhook

infra/
  snowflake/               # 001–006 DDL scripts (database, schemas, tables, roles, views)
  terraform/               # VPC, ECS, ECR, S3, Secrets Manager, EventBridge, eval-user
  deploy-ec2.sh            # One-command EC2 deployment for evaluation

tests/
  unit/                    # Pure-Python, no external deps
  integration/             # Requires running stack
  chaos/                   # Fault injection validation

config/
  prometheus/              # prometheus.yml + 5 alert rule files
  grafana/                 # Auto-provisioned dashboards + datasources
  alertmanager/            # Routing: AlertManager → ops-api → Teams / SMTP
```

---

## Key design decisions

**Why Kafka (Redpanda) and not SQS?**  
Both bronze-writer and normalization-service consume the same `market.events.validated` topic independently. Kafka's consumer group model enables this fan-out without message duplication. Kafka's log retention also means Bronze replay can re-read events without touching S3 for short windows.

**Why Redis SETNX for deduplication and not a Bloom filter?**  
A Bloom filter has false positives — valid events get discarded. In energy trading, losing a valid price tick is as damaging as accepting a duplicate. Redis SETNX is exact. At 10k events/hour with 1h TTL the key space is ~500 KB — trivially small.

**Why Parquet and not Delta Lake / Iceberg?**  
The replay requirement is time-windowed and provider-scoped, not transactional. Parquet is simpler to write (no transaction log), natively supported by Snowflake COPY INTO, and the partition scheme maps directly to replay time windows. The files can be registered as Iceberg later without rewriting data.

**Why Redis sorted set for replay job coordination?**  
`ZPOPMIN` is atomic — two replay-engine replicas cannot claim the same job without a distributed lock. Score = submission timestamp gives FIFO ordering. The ops-api can introspect the queue with `ZRANGE` without coordination overhead.

**Why quality score over a binary pass/fail?**  
A binary system loses information. A partial curve (20% of tenors missing) is not as bad as a fully malformed event. The quality score (0.0–1.0) lets downstream systems make nuanced decisions: the trading desk API only serves curves with `quality_score ≥ 0.85`, while analytics can query all data with the score as a filter.

---

## Infrastructure

| Component | Technology | Where |
|---|---|---|
| Streaming | Redpanda (Kafka-compatible) | Docker container (EC2) |
| Cache | Redis 7 (AOF + LRU) | Docker container (EC2) |
| Bronze storage | AWS S3 | `mdrp-bronze`, us-east-2 |
| Silver layer | Snowflake `SILVER_EVENTS.CURVE_EVENTS` | YMAUZRZ-ME29964 |
| Gold layer | Snowflake `GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS` | YMAUZRZ-ME29964 |
| Secrets | AWS Secrets Manager | `mdrp/prod/*`, us-east-2 |
| Metrics | Prometheus + Grafana | Docker containers (EC2) |
| Traces | Jaeger (OTLP gRPC) | Docker container (EC2) |
| Alerts | AlertManager → ops-api → Teams/SMTP | Docker container (EC2) |
| IaC | Terraform | `infra/terraform/` |
| CI/CD | GitHub Actions | `.github/workflows/ci.yml` |

---

## Evaluation credentials

**EC2 stack (live):**
- Ops API: `http://3.133.104.157:8000`
- Grafana: `http://3.133.104.157:3000` — `admin` / `mdrp_grafana`
- Prometheus: `http://3.133.104.157:9090`
- Jaeger: `http://3.133.104.157:16686`

**AWS (read-only — Bronze S3 + Snowflake PAT):**
```
AWS_ACCESS_KEY_ID:     <provided via secure channel>
AWS_SECRET_ACCESS_KEY: <provided via secure channel>
Region:                us-east-2
Bucket:                mdrp-bronze
Credentials expire:    2026-05-28
```

**Snowflake:**
```
Account:    YMAUZRZ-ME29964
User:       Lokesh
Auth:       PAT token (retrieve via AWS CLI above)
Database:   MARKET_DATA
Warehouse:  MDRP_LOAD_WH
```

---

## Run it yourself (2 modes)

### Mode 1 — Simulated (no cloud accounts needed)
```bash
git clone https://github.com/lokeshpeta6y-cloud/market-data-reliability-platform
cd market-data-reliability-platform
cp .env.example .env          # fill in values (see comments in file)
docker compose up -d
# Stack is ready in ~90 seconds
curl http://localhost:8000/health
open http://localhost:3000     # Grafana
```

### Mode 2 — Cloud (real S3 + Snowflake + Databento)
```bash
# Prerequisites: AWS CLI configured, key pair 'mdrp' in us-east-2
bash infra/deploy-ec2.sh
# Provisions EC2, pulls secrets from Secrets Manager, builds + starts stack
# URLs printed at the end (~15 min)
```
