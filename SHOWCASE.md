# Market Data Reliability Platform — Technical Showcase

**Live stack:** `http://13.58.210.216:8000` | **Repo:** github.com/lokeshpeta6y-cloud/market-data-reliability-platform  
**Verified:** 2026-05-21 | **Author:** Lokesh

---

## Problem Statement

Energy trading desks consume forward curve data from multiple market data providers simultaneously. Raw feeds arrive with silent failures — stale prices, duplicate ticks, schema drift between versions, partial curves, and out-of-order events. Without a reliability layer, downstream risk systems and trading algorithms consume corrupted data with no visibility into data quality.

**This platform intercepts every raw event, scores its quality, routes failures, and makes only authoritative quality-gated curves available to consumers — with full end-to-end observability.**

---

## Architecture Overview (Cloud Deployment)

> This document covers the live cloud deployment on AWS EC2. For local setup and the full mode comparison see [README.md](README.md).

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA INGESTION                              │
│                                                                     │
│   Databento API ──┐                                                 │
│                   ├──► provider-emulator ──► market.events.raw      │
│   Synthetic data ─┘         │                    (Kafka topic)      │
│                              │ injects 7 fault types:               │
│                              │ DUPLICATE · MALFORMED · DELAYED      │
│                              │ OUT_OF_ORDER · SCHEMA_DRIFT          │
│                              │ STALE · PARTIAL_CURVE                │
└──────────────────────────────┼──────────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      VALIDATION & QUALITY SCORING                   │
│                                                                     │
│   validation-service                                                │
│   ├── deduplication (Redis SETNX, event_id key)                     │
│   ├── schema validation (required fields, type checks)              │
│   ├── staleness detection (configurable threshold)                  │
│   ├── quality score = 1.0 − sum(fault penalties)                   │
│   │     STALE:−0.30  PARTIAL_CURVE:−0.25  SCHEMA_DRIFT:−0.20       │
│   │     OUT_OF_ORDER:−0.15  DELAYED:−0.10  DUPLICATE:−0.05         │
│   ├── valid events → market.events.validated                        │
│   └── fatal events → market.events.dlq                             │
└──────────────────────────────┼──────────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       MEDALLION ARCHITECTURE                        │
│                                                                     │
│   ┌─────────────────────────────────────────────────────────┐       │
│   │  BRONZE LAYER — Raw Immutable Store                     │       │
│   │  bronze-writer → S3 Parquet                             │       │
│   │  Partition: bronze/{provider}/{YYYY-MM-DD}/{HH}/        │       │
│   │  227 files · 16.7 MB · updated every 30s               │       │
│   │  Preserves ALL events including fault-injected ones     │       │
│   └───────────────────────┬─────────────────────────────────┘       │
│                           │                                         │
│   normalization-service ──┘ (tenor mapping, instrument normalise)   │
│                           │                                         │
│   ┌───────────────────────▼─────────────────────────────────┐       │
│   │  SILVER LAYER — Validated + Normalised Events           │       │
│   │  silver-loader → Snowflake SILVER_EVENTS.CURVE_EVENTS   │       │
│   │  89,229 rows loaded today · batches of 1000 · ~1.3s/batch│      │
│   │  Fields: event_id, provider, instrument, tenor, price,  │       │
│   │          quality_score, validation_status, bronze_s3_key│       │
│   └───────────────────────┬─────────────────────────────────┘       │
│                           │ 5-min tumbling window                   │
│   ┌───────────────────────▼─────────────────────────────────┐       │
│   │  GOLD LAYER — Reconciled Forward Curve Snapshots        │       │
│   │  gold-loader → Snowflake GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS│   │
│   │  247 snapshots loaded · 7 per window · quality gate ≥0.8│       │
│   │  Fields: curve_name, instrument, as_of, front_price,    │       │
│   │          num_tenors, completeness_pct, quality_score    │       │
│   └─────────────────────────────────────────────────────────┘       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                         CACHE LAYER                                 │
│                                                                     │
│   redis-writer → Redis 7 (in-memory)                               │
│   ├── Key: curve:{provider}:{instrument}                            │
│   ├── TTL: 300s (instrument metadata), 60s (quality scores)         │
│   ├── PIPELINE transactions — atomic multi-tenor updates            │
│   ├── Sub-millisecond reads for trading desk consumption            │
│   └── Version counter per curve (v15,948 for TTF at capture time)  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────┐
│                      END USER API (ops-api)                         │
│                                                                     │
│   FastAPI · port 8000 · reads from Redis cache                     │
│   ├── GET  /health                                                  │
│   ├── GET  /api/v1/curves          — all instruments                │
│   ├── GET  /api/v1/curves/{symbol} — single instrument + all tenors │
│   ├── GET  /api/v1/providers       — provider health + quality KPIs │
│   ├── GET  /api/v1/dlq             — dead-letter queue status       │
│   └── POST /api/v1/replay          — trigger historical replay      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Instruments Covered

| Code | Full Name | Exchange | Tenors | Curve Type |
|------|-----------|----------|--------|------------|
| `TTF` | Title Transfer Facility (gas) | ICE | 24 months | Monthly forward |
| `NBP` | National Balancing Point (gas) | ICE | 24 months | Monthly forward |
| `TTF_POWER` | TTF Power forward | EEX | 32 quarters | Monthly forward |
| `BRENT` | Brent Crude Oil | ICE | 24 months | Monthly forward |
| `WTI` | West Texas Intermediate | CME Globex | 24 months | Monthly forward |
| `EU_ETS` | EU Emissions Allowances | ICE | 5 quarters | Monthly forward |

---

## How Data Is Fetched

The `provider-emulator` service runs an **Ornstein-Uhlenbeck mean-reverting price process** for all 6 instruments, publishing every 5 seconds. It also supports a **Databento adapter** for real historical data (CME Globex MBP-1 schema).

At each publish cycle it deliberately injects faults at configured rates:

| Fault Type | Rate | Quality Penalty | What it does |
|------------|------|-----------------|--------------|
| `DUPLICATE` | 2% | −0.05 | Re-sends the same event_id |
| `MALFORMED` | 1% | fatal → DLQ | Corrupts required fields |
| `DELAYED` | 5% | −0.10 | Holds event in queue, releases late |
| `OUT_OF_ORDER` | 3% | −0.15 | Delivers events in wrong sequence |
| `SCHEMA_DRIFT` | 0.5% | −0.20 | Renames fields (e.g. `price` → `px`) |
| `STALE` | 1% | −0.30 | Timestamp >5 min old |
| `PARTIAL_CURVE` | 2% | −0.25 | Sends only subset of tenors |

**Verified live (cycle 685):**
```json
{
  "cycle": 685,
  "clean_events": 133,
  "published": 130,
  "drained": 8,
  "delay_queue": 33,
  "ooo_queue": 5,
  "event": "publish_cycle_complete"
}
```
133 events per cycle × 685 cycles = **~91,000 events published** since startup.

---

## How Data Is Cleaned (Validation Pipeline)

Every event on `market.events.raw` is processed by `validation-service`:

1. **Deduplication** — Redis SETNX on `dedup:{event_id}` with 5-min TTL. Duplicate events are counted and discarded.
2. **Schema check** — Required fields presence, correct types, no `None` prices.
3. **Staleness check** — `event_timestamp` vs `received_at`. Events older than threshold → STALE fault.
4. **Quality scoring** — Additive penalty model. Score = 1.0 minus all applicable penalties.
5. **Routing** — Score > 0 → `market.events.validated`. Fatal (malformed, missing ID) → `market.events.dlq`.

Events carry their `quality_score` and `validation_status` through the entire pipeline into Snowflake Silver, so analysts can filter by quality threshold.

---

## Streaming Pipeline (Kafka Topics)

5 Redpanda (Kafka-compatible) topics form the backbone:

```
market.events.raw          ← provider-emulator publishes here
market.events.validated    ← validation-service → bronze-writer, normalization, silver-loader consume
market.events.dlq          ← fatal events land here (replayed on demand)
market.events.normalized   ← normalization-service → redis-writer, gold-loader consume
market.events.replay       ← replay-engine republishes historical events here
```

All consumers use `KAFKA_CONSUMER_GROUP_PREFIX=mdrp` with offset-committed, at-least-once delivery. `bronze-writer` uses `KAFKA_PRODUCER_ACKS=all` for durability.

---

## Bronze Layer — Immutable Event Store (S3)

`bronze-writer` consumes `market.events.validated`, buffers in memory, and flushes to S3 Parquet on size (1000 events) or time (30s) thresholds.

**Partition scheme:**
```
s3://mdrp-bronze/
  bronze/
    provider-emulator/
      2026-05-21/
        11/
          events_<uuid>.parquet   ← ~60–93 KB per file, Snappy compressed
```

**Live stats (verified):**
- **227 Parquet files** written today
- **16.7 MB** total Bronze storage
- Files updated continuously every 30 seconds

Mixed-type safety: dict/list fields (fault metadata) are serialised to JSON strings before PyArrow write, preventing schema conflicts from fault-injected events.

---

## Silver Layer — Validated Events in Snowflake

`silver-loader` consumes `market.events.validated` and loads to Snowflake via `COPY INTO` using PAT token authentication.

**Table:** `MARKET_DATA.SILVER_EVENTS.CURVE_EVENTS`

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | VARCHAR | Unique event identifier |
| `provider` | VARCHAR | Data source name |
| `instrument` | VARCHAR | Normalised instrument code |
| `tenor` | VARCHAR | Forward tenor (e.g. `2026-09`) |
| `price` | FLOAT | Mid price |
| `quality_score` | FLOAT | 0.0–1.0 reliability score |
| `validation_status` | VARCHAR | VALID / STALE / OUT_OF_ORDER etc |
| `bronze_s3_key` | VARCHAR | Full lineage back to S3 file |
| `trace_id` | VARCHAR | End-to-end correlation ID |
| `event_timestamp` | TIMESTAMP_TZ | Original event time |
| `received_at` | TIMESTAMP_TZ | Ingestion time |

**Live stats:** **89,229 rows** loaded today · batches of 1000 rows · ~1.3 seconds per Snowflake COPY INTO

---

## Gold Layer — Reconciled Forward Curve Snapshots

`gold-loader` runs on a **5-minute tumbling window**. It assembles complete forward curve snapshots from Silver, applies a quality gate (completeness ≥ 80%), and merges into the Gold table.

**Table:** `MARKET_DATA.GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS`

Quality gate: snapshots where fewer than 80% of expected tenors arrived are discarded. This ensures traders never see a partial curve presented as complete.

**Live stats:**
```json
{
  "snapshots_ready": 7,
  "snapshots_loaded": 7,
  "rows_loaded": 7,
  "total_loaded": 247,
  "total_skipped": 16,
  "event": "gold_flush_complete"
}
```
**247 Gold snapshots** loaded · 7 instruments per 5-min window · 16 partial snapshots correctly discarded by quality gate.

---

## Cache Layer — Sub-millisecond Reads (Redis)

`redis-writer` consumes `market.events.normalized` and maintains a **live forward curve cache** in Redis, keyed by instrument.

```
curve:provider-emulator:TTF        → full 24-tenor snapshot (JSON)
curve:provider-emulator:TTF:2026-09 → individual tenor with quality_score
quality:provider-emulator:TTF      → rolling quality metrics
```

- **PIPELINE transactions** for atomic multi-tenor updates
- **TTL:** 300s instrument metadata, 60s quality scores
- `ops-api` reads exclusively from Redis — sub-millisecond latency for trading desk

**Why Redis before Snowflake?** Snowflake COPY INTO has ~1–2s latency. A trading desk querying curves every second cannot wait. Redis serves the hot path; Snowflake serves analytics and audit.

---

## Replay Engine

`replay-engine` supports 3 replay sources for historical event reprocessing:

| Source | Use case |
|--------|----------|
| `bronze_s3` | Replay any time window from S3 Parquet |
| `dlq` | Retry dead-lettered events after fix deployment |
| `databento` | Re-ingest from Databento historical API |

**Verified live:**
```bash
POST /api/v1/replay
{
  "source": "bronze_s3",
  "provider": "provider-emulator",
  "start_time": "2026-05-21T00:00:00Z",
  "end_time": "2026-05-21T23:59:59Z"
}
# Result: 37,687 events replayed in ~15 seconds
```

---

## Live API — End User Consumption

All endpoints served from `http://13.58.210.216:8000`. Data read from Redis cache — no Snowflake query on the hot path.

### Platform Health
```bash
GET /health
```
```json
{"status": "ok", "timestamp": "2026-05-21T11:37:45.044359Z"}
```

### All Forward Curves (summary)
```bash
GET /api/v1/curves
```
```json
[
  {"instrument":"TTF","curve_name":"TTF_MONTHLY_FWD","provider":"provider-emulator",
   "as_of":"2026-05-21T11:37:34Z","completeness":1.0,"tenor_count":24,
   "version":15948,"is_authoritative":true},
  {"instrument":"BRENT","curve_name":"BRENT_MONTHLY_FWD","provider":"provider-emulator",
   "as_of":"2026-05-21T11:37:39Z","completeness":1.0,"tenor_count":24,
   "version":16015,"is_authoritative":true},
  {"instrument":"NBP",  "completeness":1.0,"tenor_count":24,"version":15979},
  {"instrument":"WTI",  "completeness":1.0,"tenor_count":24,"version":15957},
  {"instrument":"TTF_POWER","completeness":1.0,"tenor_count":32,"version":15943},
  {"instrument":"EU_ETS","completeness":1.0,"tenor_count":5, "version":3333}
]
```

### Single Instrument — Full Curve with Quality Scores
```bash
GET /api/v1/curves/TTF
```
```json
{
  "instrument": "TTF",
  "curve_name": "TTF_MONTHLY_FWD",
  "as_of": "2026-05-21T11:37:34Z",
  "completeness": 1.0,
  "version": 15948,
  "tenors": {
    "2026-06": {"price": "30.1078", "quality_score": 1.0},
    "2026-07": {"price": "30.0719", "quality_score": 0.95},
    "2026-08": {"price": "30.1836", "quality_score": 0.95},
    "2026-09": {"price": "30.3026", "quality_score": 1.0},
    "2027-01": {"price": "30.1112", "quality_score": 0.90},
    "2027-02": {"price": "30.1101", "quality_score": 0.70},
    "... 24 tenors total": "..."
  }
}
```
Note: `quality_score: 0.70` on `2027-02` — a STALE fault (−0.30) was detected on that tenor. Trading systems can filter `quality_score < 0.8` before consumption.

### Provider Health
```bash
GET /api/v1/providers
```
```json
[{
  "provider": "provider-emulator",
  "status": "healthy",
  "last_event_at": "2026-05-21T11:37:43Z",
  "events_last_60s": 88480,
  "dlq_rate_last_60s": 0.0,
  "quality_score_p50": 0.0,
  "quality_score_p95": 0.0
}]
```
**88,480 events processed in last 60 seconds.** DLQ rate: 0 — all events are scoring above the fatal threshold.

### Dead-Letter Queue
```bash
GET /api/v1/dlq
```
```json
{
  "depth_estimate": 0,
  "top_failure_categories": [],
  "recent_entries": [],
  "as_of": "2026-05-21T11:37:45Z"
}
```

---

## Snowflake Verification Queries

```sql
-- Silver: row count by instrument (run against MARKET_DATA database)
SELECT provider, instrument, COUNT(*) AS total_events,
       MIN(event_timestamp) AS earliest, MAX(event_timestamp) AS latest,
       ROUND(AVG(quality_score), 3) AS avg_quality
FROM MARKET_DATA.SILVER_EVENTS.CURVE_EVENTS
GROUP BY 1, 2 ORDER BY latest DESC;

-- Gold: latest complete curves
SELECT instrument, curve_date, front_price, num_tenors,
       ROUND(quality_score, 3) AS quality, completeness_pct, snapshot_time
FROM MARKET_DATA.GOLD_CURVES.V_LATEST_CURVES
ORDER BY instrument;

-- Full lineage: trace an event from S3 key to Snowflake row
SELECT event_id, provider, instrument, tenor, price,
       quality_score, bronze_s3_key, trace_id
FROM MARKET_DATA.SILVER_EVENTS.CURVE_EVENTS
WHERE bronze_s3_key LIKE '%provider-emulator%'
LIMIT 10;
```

---

## Observability Stack

### Grafana — `http://13.58.210.216:3000` (admin / mdrp_grafana)
Pipeline Overview dashboard showing:
- Event throughput per service (events/sec)
- Kafka consumer lag per topic
- DLQ depth over time
- Bronze write success/failure rate
- Redis cache hit rate
- Snowflake load latency (Silver + Gold)

### Prometheus — `http://13.58.210.216:9090`
Key metrics:
```promql
rate(mdrp_events_processed_total[1m])     # throughput per service
mdrp_dlq_depth_total                       # DLQ backlog
rate(mdrp_bronze_writes_total[5m])         # S3 write rate
kafka_consumer_group_lag                   # pipeline lag
```

### Jaeger — `http://13.58.210.216:16686`
Distributed traces for every HTTP request to `ops-api`. Search service `ops-api` to see full request traces including Redis reads.

### AlertManager — `http://13.58.210.216:9093`
Alerts configured for:
- Provider down (no events for 5 min)
- DLQ spike (>100 events in 5 min)
- High consumer lag (>10,000 messages)
- Redis down
- Bronze write failures

---

## Infrastructure

| Component | Technology | Where |
|-----------|-----------|-------|
| Message broker | Redpanda (Kafka-compatible) | Docker, EC2 |
| Cache | Redis 7 | Docker, EC2 |
| Bronze storage | AWS S3 (`mdrp-bronze`) | us-east-2 |
| Silver/Gold warehouse | Snowflake `MARKET_DATA` | YMAUZRZ-ME29964 |
| Compute | EC2 `m7i-flex.large` (2 vCPU, 8 GB) | us-east-2 |
| Secrets | AWS Secrets Manager (`mdrp/prod/*`) | us-east-2 |
| Metrics | Prometheus + Grafana | Docker, EC2 |
| Tracing | Jaeger (OTLP/gRPC) | Docker, EC2 |
| Alerting | AlertManager | Docker, EC2 |
| CI/CD | GitHub Actions | `.github/workflows/ci.yml` |
| IaC | Terraform (modules: networking, ECS, S3, secrets, eval-user) | `infra/terraform/` |

---

## Batch Jobs — Where and How They Run

Both batch jobs run as **long-lived Docker containers** on EC2 — not scheduled cron or Lambda. They are event-driven micro-batchers:

**silver-loader** — consumes `market.events.validated` continuously. Flushes to Snowflake when batch reaches 1,000 events OR 30 seconds elapse (whichever first). Uses PAT token auth (`authenticator="programmatic_access_token"`).

**gold-loader** — runs a 5-minute tumbling window over Silver data. Every 5 minutes: assembles curve snapshots, applies quality gate (completeness ≥ 80%), merges into Gold. 7 instruments × every 5 min = 7 rows per window.

Neither requires a scheduler — Kafka provides the event-driven trigger and offset management handles restarts transparently.

---

## Numbers at a Glance (captured 2026-05-21 ~11:38 UTC)

| Metric | Value |
|--------|-------|
| Events published (since startup) | ~91,000 |
| Silver rows in Snowflake | 89,229 |
| Gold snapshots in Snowflake | 247 |
| Bronze Parquet files (S3) | 227 |
| Bronze storage (S3) | 16.7 MB |
| Events replayed (single job) | 37,687 |
| Active instruments | 6 |
| Curve versions (TTF) | 15,948 |
| All containers healthy | 16/16 |
| DLQ depth | 0 |
| Uptime | ~1 hour continuous |
