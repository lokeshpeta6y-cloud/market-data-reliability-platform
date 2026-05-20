# redis-writer

Consumes `CurveEvent` records from `market.events.normalized` and maintains
several Redis data structures used by the ops API, alerting, and downstream
consumers.

## Redis key schema

| Key pattern | Type | Contents |
|-------------|------|----------|
| `curve:latest:{instrument}` | Hash | Field = tenor, value = `TenorPrice` JSON |
| `curve:history:{instrument}` | Sorted Set | Member = `CurveEvent` JSON, score = `event_timestamp` Unix-ms. Capped at `curve_history_max_entries` |
| `curve:snapshot:{instrument}` | String | `ForwardCurveSnapshot` JSON (only written when completeness ≥ threshold) |
| `provider:health:{provider}` | Hash | `last_event_at` (ISO-8601), `events_per_minute` |
| `provider:health:{provider}:minute:{epoch_minute}` | String | Per-minute event counter (TTL 120 s) |

## Forward curve snapshot assembly

After each tenor write, the service checks whether the number of stored tenors
for that instrument is ≥ `snapshot_completeness_threshold` × `expected_tenors`.
When the threshold is met, a `ForwardCurveSnapshot` is serialised and written to
`curve:snapshot:{instrument}`.

## Staleness detection

Every `STALENESS_CHECK_INTERVAL` (100) events, the service scans all
`curve:latest:*` keys and checks the most-recent event timestamp in the
corresponding history sorted set.  If the gap exceeds
`staleness_threshold_seconds` (default 600 s / 10 min), a structured
`WARNING` log record is emitted:

```json
{
  "event": "instrument_data_stale",
  "instrument": "TTF",
  "last_event_at": "2026-05-20T09:45:00+00:00",
  "staleness_seconds": 720.3
}
```

The ops API tails these records to drive alerting.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka/Redpanda broker list |
| `KAFKA_CONSUMER_GROUP` | `redis-writer` | Consumer group ID |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `REDIS_CONNECT_TIMEOUT` | `10` | Seconds to wait for Redis on startup |
| `METRICS_PORT` | `8005` | Prometheus metrics HTTP port |
| `CURVE_HISTORY_MAX_ENTRIES` | `1000` | Max entries in history sorted set per instrument |
| `STALENESS_THRESHOLD_SECONDS` | `600` | Seconds before an instrument is flagged stale |
| `SNAPSHOT_COMPLETENESS_THRESHOLD` | `0.80` | Minimum fraction of expected tenors to assemble a snapshot |
| `EXPECTED_TENORS_PER_INSTRUMENT` | See settings.py | JSON object mapping instrument → expected tenor count |
| `LOG_LEVEL` | `INFO` | Logging level |
| `SERVICE_VERSION` | `0.1.0` | Reported in service-info metric |

## Running locally

```bash
docker compose up -d redpanda redis

pip install -e libs/common -e services/redis-writer

KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
REDIS_URL=redis://localhost:6379/0 \
redis-writer
```

## Docker

```bash
# From the repo root
docker build \
  -f services/redis-writer/Dockerfile \
  -t mdrp/redis-writer:latest \
  .
```

## Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `mdrp_events_normalized_total` | Counter | `provider`, `instrument` |
| `mdrp_event_quality_score` | Histogram | `provider` |
| `mdrp_event_processing_latency_seconds` | Histogram | `service`, `provider` |

## Architecture

```
market.events.normalized
        │
        ▼
  MdrpConsumer
        │
        ▼
  RedisWriter
  └── CurveStore
      ├── HSET  curve:latest:{instrument}
      ├── ZADD  curve:history:{instrument}  (+ trim)
      ├── HSET  provider:health:{provider}
      └── SET   curve:snapshot:{instrument}  (conditional)
```
