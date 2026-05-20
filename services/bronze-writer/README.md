# bronze-writer

Consumes raw market events from `market.events.raw` and writes them to the
Bronze layer on MinIO/S3 as Snappy-compressed Parquet files.

This service runs in parallel with `validation-service` — both subscribe to the
same topic with **different consumer groups**, so neither affects the other's
read position.

## Behaviour

1. Each consumed event is added to an in-memory `EventBuffer`.
2. The buffer is flushed when either:
   - `BATCH_SIZE` events have accumulated (default: 500), **or**
   - `FLUSH_INTERVAL_SECONDS` have elapsed since the last flush (default: 30s).
3. On flush, events are grouped by provider and one Parquet file is written per provider.
4. Offsets are committed **only after a successful S3 write**.
5. On S3 write failure: the error is logged, the metric is incremented, and the
   offset is **not committed** — the messages are re-read on restart.

## S3 partition scheme

```
s3://{bucket}/bronze/{provider}/{YYYY-MM-DD}/{HH}/events_{batch_id}.parquet
```

Example:
```
s3://mdrp-bronze/bronze/bloomberg/2026-05-20/14/events_3f7a1c2d-....parquet
```

## Architecture

```
market.events.raw
       │
       ▼
 MdrpConsumer (group: bronze-writer)
       │
       ▼
  BronzeWriter.process(event_dict)
       │
   EventBuffer
  (thread-safe)
       │  size >= batch_size
       │  OR age >= flush_interval
       ▼
  flush() — group by provider
       │
       ▼
 BronzeStorageClient.write_parquet_batch()
       │
    S3/MinIO
```

A background daemon thread fires ``flush()`` every ``flush_interval_seconds``
so quiet periods still produce timely files.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka/Redpanda bootstrap |
| `KAFKA_CONSUMER_GROUP` | `bronze-writer` | Consumer group ID |
| `S3_ENDPOINT_URL` | _(unset)_ | MinIO endpoint (omit for real AWS S3) |
| `S3_BUCKET_BRONZE` | `mdrp-bronze` | Target S3 bucket |
| `AWS_ACCESS_KEY_ID` | _(unset)_ | AWS / MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | _(unset)_ | AWS / MinIO secret key |
| `AWS_REGION` | `eu-west-1` | AWS region |
| `BATCH_SIZE` | `500` | Events per Parquet file (max) |
| `FLUSH_INTERVAL_SECONDS` | `30` | Max seconds between flushes |
| `METRICS_PORT` | `8003` | Prometheus metrics port |
| `LOG_LEVEL` | `INFO` | Log level (DEBUG/INFO/WARNING/ERROR) |

## Metrics exposed

| Metric | Labels | Description |
|--------|--------|-------------|
| `mdrp_bronze_writes_total` | provider, outcome | Parquet files written (success/failed) |
| `mdrp_bronze_write_duration_seconds` | — | Write wall-clock time histogram |
| `mdrp_bronze_bytes_written_total` | provider | Approximate bytes written |
| `mdrp_consumer_lag_messages` | topic, partition, consumer_group | Consumer lag |

## Running locally

```bash
# With Docker Compose (recommended — requires MinIO)
docker compose up bronze-writer

# Standalone (requires Kafka and MinIO running)
pip install -e ".[dev]"
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
S3_ENDPOINT_URL=http://localhost:9000 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
bronze-writer
```

## Running tests

```bash
pytest tests/ -v --cov=bronze_writer
```

The test suite uses `moto[s3]` to mock the S3 API so no real AWS credentials
are required.
