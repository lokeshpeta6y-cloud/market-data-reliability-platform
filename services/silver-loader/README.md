# silver-loader

Consumes normalised `CurveEvent` messages from `market.events.normalized` and
bulk-loads them into the **Snowflake Silver layer** (`SILVER_EVENTS.CURVE_EVENTS`).

## Architecture

```
market.events.normalized
        │
        ▼  (consumer group: silver-loader)
  ┌──────────────┐
  │ SilverLoader │  ← in-memory buffer (up to batch_size events)
  └──────┬───────┘
         │  flush (size-triggered OR time-triggered)
         ▼
  ┌────────────────────┐
  │  SnowflakeClient   │
  │  PUT → stage       │
  │  COPY INTO Silver  │
  └────────────────────┘
```

Two flush triggers run in parallel:
- **Size-triggered**: inline in the consume loop when `buffer_size >= batch_size`.
- **Time-triggered**: a background daemon thread flushes every `flush_interval_seconds`.

## Snowflake DDL

Apply once to your Snowflake account:

```sql
CREATE SCHEMA IF NOT EXISTS SILVER_EVENTS;

CREATE TABLE IF NOT EXISTS SILVER_EVENTS.CURVE_EVENTS (
    event_id           VARCHAR(36)    NOT NULL,
    source_event_id    VARCHAR(36),
    curve_name         VARCHAR(100),
    instrument         VARCHAR(50),
    tenor              VARCHAR(20),
    delivery_period    VARCHAR(20),
    price              NUMBER(20, 6),
    currency           VARCHAR(10),
    unit               VARCHAR(20),
    provider           VARCHAR(50),
    version            INTEGER,
    event_timestamp    TIMESTAMP_TZ,
    ingestion_timestamp TIMESTAMP_TZ,
    quality_score      NUMBER(5, 4),
    is_replay          BOOLEAN DEFAULT FALSE,
    replay_source      VARCHAR(50),
    trace_id           VARCHAR(36),
    loaded_at          TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

CREATE STAGE IF NOT EXISTS SILVER_EVENTS.MDRP_STAGE
    FILE_FORMAT = (TYPE = 'JSON');
```

## Configuration

All settings are read from environment variables (or a `.env` file):

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka / Redpanda bootstrap address |
| `KAFKA_CONSUMER_GROUP` | `silver-loader` | Consumer group ID |
| `METRICS_PORT` | `8008` | Prometheus metrics HTTP port |
| `BATCH_SIZE` | `1000` | Events to buffer before a size-triggered flush |
| `FLUSH_INTERVAL_SECONDS` | `60` | Maximum seconds between flushes |
| `SNOWFLAKE_ACCOUNT` | *(unset)* | Snowflake account identifier |
| `SNOWFLAKE_USER` | *(unset)* | Snowflake username |
| `SNOWFLAKE_PASSWORD` | *(unset)* | Snowflake password |
| `SNOWFLAKE_DATABASE` | `MARKET_DATA_PLATFORM` | Snowflake database |
| `SNOWFLAKE_SCHEMA_SILVER` | `SILVER_EVENTS` | Schema containing CURVE_EVENTS |
| `SNOWFLAKE_WAREHOUSE` | `INGESTION_WH` | Compute warehouse |
| `SNOWFLAKE_STAGE_NAME` | `MDRP_STAGE` | Internal stage for COPY INTO |
| `SNOWFLAKE_LOAD_RETRIES` | `3` | Reconnect attempts before giving up |

**Snowflake is optional.** If `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and
`SNOWFLAKE_PASSWORD` are not set, the service runs normally but skips all
Snowflake writes (logged at WARN level).

## Running locally

```bash
# Without Snowflake (events buffered and silently dropped)
docker compose up silver-loader

# With Snowflake
SNOWFLAKE_ACCOUNT=myorg-myaccount \
SNOWFLAKE_USER=loader_svc \
SNOWFLAKE_PASSWORD=s3cr3t \
docker compose up silver-loader
```

## Metrics

Prometheus metrics exposed on `:8008/metrics`:

| Metric | Labels | Description |
|---|---|---|
| `mdrp_snowflake_loads_total` | `layer=silver, outcome` | COPY INTO operations |
| `mdrp_snowflake_load_duration_seconds` | `layer=silver` | Load latency histogram |
| `mdrp_snowflake_rows_loaded_total` | `layer=silver` | Cumulative rows loaded |
| `mdrp_consumer_lag_messages` | `topic, partition, consumer_group` | Kafka consumer lag |
