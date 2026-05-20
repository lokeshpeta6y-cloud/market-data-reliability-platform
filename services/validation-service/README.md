# validation-service

Consumes raw market events from `market.events.raw`, runs a multi-rule validation
pipeline, and routes each event to either `market.events.validated` or
`market.events.dlq`.

## Responsibilities

| Rule | Description | DLQ Category |
|------|-------------|--------------|
| Schema validation | Required fields: event_id, provider, instrument, event_timestamp, payload | `missing_required_field` |
| Type validation | payload must be dict; price must be numeric if present | `schema_violation` |
| Duplicate detection | Redis SETNX keyed on event_id with configurable TTL | silent discard |
| Timestamp bounds | event_timestamp within [-24h, +5min] of now | `stale` / `out_of_order` |
| Price sanity | 0 < price < 1,000,000 (if price field present) | `malformed` |
| Quality scoring | Computes 0.0–1.0 score from injected_faults; stores rolling average per provider in Redis | — |

## Architecture

```
market.events.raw
       │
       ▼
 MdrpConsumer (group: validation-service)
       │
       ▼
 ValidationService.validate()
  ├── Deduplicator (Redis SETNX)
  ├── QualityScorer (Redis hash rolling avg)
  └── Rules 1–6
       │
   ┌───┴───┐
   ▼       ▼
validated  DLQ
 events   events
```

## Configuration

All settings are loaded from environment variables (`.env` file also supported):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka/Redpanda bootstrap |
| `KAFKA_CONSUMER_GROUP` | `validation-service` | Consumer group ID |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `DEDUP_TTL_SECONDS` | `3600` | How long to remember event IDs |
| `MAX_EVENT_AGE_HOURS` | `24` | Max age before STALE |
| `MAX_FUTURE_MINUTES` | `5` | Max future drift before OUT_OF_ORDER |
| `MIN_PRICE` | `0.0` | Minimum valid price (exclusive) |
| `MAX_PRICE` | `1000000.0` | Maximum valid price (exclusive) |
| `QUALITY_ROLLING_WINDOW` | `100` | Rolling window for quality averages |
| `METRICS_PORT` | `8002` | Prometheus metrics port |
| `LOG_LEVEL` | `INFO` | Log level (DEBUG/INFO/WARNING/ERROR) |

## Metrics exposed

| Metric | Labels | Description |
|--------|--------|-------------|
| `mdrp_events_validated_total` | provider, outcome | Events processed (passed/failed/deduplicated) |
| `mdrp_events_deduplicated_total` | provider | Duplicate events discarded |
| `mdrp_dlq_events_total` | provider, failure_category | Events routed to DLQ |
| `mdrp_validation_errors_total` | provider, error_type | Validation rule violations |
| `mdrp_event_quality_score` | provider | Histogram of quality scores |
| `mdrp_event_processing_latency_seconds` | service, provider | End-to-end latency |
| `mdrp_consumer_lag_messages` | topic, partition, consumer_group | Consumer lag |

## Running locally

```bash
# With Docker Compose (recommended)
docker compose up validation-service

# Standalone (requires Kafka and Redis running)
pip install -e ".[dev]"
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 REDIS_URL=redis://localhost:6379/0 validation-service
```

## Running tests

```bash
pytest tests/ -v --cov=validation_service
```

## Quality scoring details

Each fault type in `injected_faults` contributes a penalty to the base score of 1.0:

| Fault | Penalty |
|-------|---------|
| `malformed` | 0.50 |
| `missing_field` | 0.40 |
| `duplicate` | 0.30 |
| `stale` | 0.25 |
| `out_of_order` | 0.25 |
| `schema_drift` | 0.20 |
| `delayed` | 0.10 |
| `partial_curve` | 0.15 |

Penalties are additive; the final score is clamped to `[0.0, 1.0]`. A rolling
average per provider is maintained in Redis with proportional decay once the
window is exceeded.
