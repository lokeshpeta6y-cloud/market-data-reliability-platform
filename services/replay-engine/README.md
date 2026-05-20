# Replay Engine

The replay-engine service executes replay jobs submitted through the ops-api.
It polls Redis for pending jobs and dispatches to one of three replayer modes.

## Replay Modes

| Source | Description | Output Topic |
|--------|-------------|--------------|
| `bronze_s3` | Read Parquet files from S3/MinIO Bronze layer | `market.events.replay` |
| `databento_historical` | Pull from Databento REST API | `market.events.replay` |
| `dlq` | Re-consume from Dead-Letter Queue | `market.events.raw` |

## Job State Machine

```
pending → running → completed
                 → failed
```

Jobs are stored in Redis hashes at `replay:job:{job_id}`.
The pending queue is a Redis sorted set at `replay:jobs:pending` (score = submission epoch).
Atomic claim via `ZPOPMIN` prevents double-execution across multiple engine instances.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `S3_ENDPOINT_URL` | *(unset)* | MinIO endpoint (leave unset for real AWS S3) |
| `S3_BUCKET_BRONZE` | `mdrp-bronze` | Bronze S3 bucket name |
| `METRICS_PORT` | `8006` | Prometheus metrics HTTP port |
| `REPLAY_RATE_LIMIT_PER_SECOND` | `1000` | Max events/sec published to Kafka |
| `JOB_POLL_INTERVAL_SECONDS` | `5` | How often to check Redis for new jobs |
| `DATABENTO_API_KEY` | *(unset)* | Enables Databento historical replay |
| `LOG_LEVEL` | `INFO` | Logging level |

## Running Locally

```bash
# With Docker Compose (recommended)
docker compose up replay-engine

# Directly
pip install -e services/replay-engine
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
REDIS_URL=redis://localhost:6379/0 \
replay-engine
```

## Metrics

Prometheus metrics are exposed on port `8006` at `/metrics`.

| Metric | Labels | Description |
|--------|--------|-------------|
| `mdrp_replay_jobs_total` | `source`, `outcome` | Jobs completed/failed |
| `mdrp_replay_events_total` | `source` | Events published during replay |
| `mdrp_replay_duration_seconds` | `source` | Histogram of job duration |

## DLQ Replay

DLQ replay seeks Kafka partitions to the requested start timestamp using
`offsets_for_times`, then consumes until the end timestamp.  Events are
re-published to `market.events.raw` so they traverse the full
validation → normalisation → storage pipeline.

Use DLQ replay after deploying a fix to recover events that previously failed.
