# gold-loader

Consumes normalised `CurveEvent` messages from `market.events.normalized`, assembles
them into reconciled **ForwardCurveSnapshot** objects, and upserts authoritative
snapshots into the **Snowflake Gold layer** (`GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS`).

## Architecture

```
market.events.normalized
        │
        ▼  (consumer group: gold-loader)
  ┌──────────────────────┐
  │  SnapshotAssembler   │  ← groups events by (curve_name, 5-min window)
  └──────────┬───────────┘
             │  get_ready_snapshots()  (windows > 2x width old)
             ▼
  ┌──────────────────────┐
  │     GoldLoader       │  ← filters: completeness >= min_completeness
  └──────────┬───────────┘              AND is_authoritative == True
             │  load_batch()
             ▼
  ┌──────────────────────┐
  │   SnowflakeClient    │
  │  PUT → stage         │
  │  MERGE INTO Gold     │  ← upsert on (curve_name, as_of)
  └──────────────────────┘
```

The consume loop and Snowflake writes are decoupled:
- **Main thread**: consumes Kafka messages and feeds `SnapshotAssembler`.
- **Background thread**: polls every `poll_interval_seconds` for expired windows
  and loads them to Snowflake.

## Snapshot assembly

Events arriving in a `snapshot_window_minutes`-wide tumbling window for the same
`curve_name` are merged (last-write-wins per tenor).  A window is finalised once it
is `2 × snapshot_window_minutes` old to tolerate late-arriving events.

A snapshot is **authoritative** when:
- `completeness = tenors_received / expected_tenors ≥ 0.95`
- `min(quality_score across tenors) ≥ min_quality_score`

Only authoritative snapshots are written to Gold by default.

## Snowflake DDL

```sql
CREATE SCHEMA IF NOT EXISTS GOLD_CURVES;

CREATE TABLE IF NOT EXISTS GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS (
    snapshot_id      VARCHAR(36)   NOT NULL,
    curve_name       VARCHAR(100),
    instrument       VARCHAR(50),
    as_of            TIMESTAMP_TZ,
    tenors           VARIANT,
    completeness     NUMBER(5, 4),
    is_authoritative BOOLEAN,
    version          INTEGER,
    provider         VARCHAR(50),
    created_at       TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (snapshot_id),
    UNIQUE (curve_name, as_of)
);

CREATE STAGE IF NOT EXISTS GOLD_CURVES.MDRP_STAGE
    FILE_FORMAT = (TYPE = 'JSON');
```

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka / Redpanda bootstrap address |
| `KAFKA_CONSUMER_GROUP` | `gold-loader` | Consumer group ID |
| `METRICS_PORT` | `8009` | Prometheus metrics HTTP port |
| `SNAPSHOT_WINDOW_MINUTES` | `5` | Tumbling window width for event grouping |
| `MIN_COMPLETENESS` | `0.80` | Minimum completeness to write a snapshot |
| `MIN_QUALITY_SCORE` | `0.70` | Minimum quality score for authoritative status |
| `EXPECTED_TENORS_PER_CURVE` | `0` | Set >0 to fix expected tenor count (0 = learn from data) |
| `POLL_INTERVAL_SECONDS` | `5` | How often the background thread checks for ready snapshots |
| `SNOWFLAKE_ACCOUNT` | *(unset)* | Snowflake account identifier |
| `SNOWFLAKE_USER` | *(unset)* | Snowflake username |
| `SNOWFLAKE_PASSWORD` | *(unset)* | Snowflake password |
| `SNOWFLAKE_DATABASE` | `MARKET_DATA_PLATFORM` | Snowflake database |
| `SNOWFLAKE_SCHEMA_GOLD` | `GOLD_CURVES` | Schema containing FORWARD_CURVE_SNAPSHOTS |
| `SNOWFLAKE_WAREHOUSE` | `INGESTION_WH` | Compute warehouse |
| `SNOWFLAKE_STAGE_NAME` | `MDRP_STAGE` | Internal stage for MERGE |
| `SNOWFLAKE_LOAD_RETRIES` | `3` | Reconnect attempts before giving up |

**Snowflake is optional.** The service runs normally without Snowflake credentials
(assembles snapshots but skips writes, logged at WARN level).

## Running locally

```bash
# Without Snowflake
docker compose up gold-loader

# With Snowflake
SNOWFLAKE_ACCOUNT=myorg-myaccount \
SNOWFLAKE_USER=loader_svc \
SNOWFLAKE_PASSWORD=s3cr3t \
docker compose up gold-loader
```

## Metrics

| Metric | Labels | Description |
|---|---|---|
| `mdrp_snowflake_loads_total` | `layer=gold, outcome` | MERGE operations |
| `mdrp_snowflake_load_duration_seconds` | `layer=gold` | Load latency histogram |
| `mdrp_snowflake_rows_loaded_total` | `layer=gold` | Cumulative rows upserted |
| `mdrp_consumer_lag_messages` | `topic, partition, consumer_group` | Kafka consumer lag |
