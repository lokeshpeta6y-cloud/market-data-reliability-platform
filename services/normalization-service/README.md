# normalization-service

Consumes `ValidatedMarketEvent` records from `market.events.validated`, applies
canonical normalisation rules, and publishes `CurveEvent` records to
`market.events.normalized`.

## Responsibilities

| Step | Detail |
|------|--------|
| **Instrument mapping** | Maps provider symbols (`TTF_GAS`, `BRN`, `EUA`, …) to canonical names (`TTF`, `BRENT`, `EU_ETS`, …) and assigns default currency/unit |
| **Tenor standardisation** | Parses free-form tenor strings (`MAR24`, `Q1-2024`, `Summer 2024`, `CAL24`, …) into ISO-style canonical tenors (`2024-03`, `2024-Q1`, `2024-SUM`, `2024-CAL`) |
| **Delivery period detection** | Infers `DeliveryPeriod` enum value from the canonical tenor (`MONTHLY`, `QUARTERLY`, `SEASONAL`, `ANNUAL`, `SPOT`) |
| **Curve name construction** | Builds `{INSTRUMENT}_{DELIVERY_PERIOD}_FWD` e.g. `TTF_MONTHLY_FWD` |
| **Quality scoring** | Starts at 1.0 and deducts per injected fault type: DELAYED −0.05, SCHEMA_DRIFT −0.20, STALE −0.30, PARTIAL_CURVE −0.25, OUT_OF_ORDER −0.10. Minimum 0.0. |
| **Version counter** | Atomically increments a Redis `INCR` counter per curve name to produce a monotonically increasing version number |

Events that cannot be normalised (unknown instrument, missing price, unrecognised
tenor) are logged at `WARNING` level and **silently dropped** — they already
cleared validation, so DLQ routing is not appropriate.

## Configuration

All settings are read from environment variables (or a `.env` file):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka/Redpanda broker list |
| `KAFKA_CONSUMER_GROUP` | `normalization-service` | Consumer group ID |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `REDIS_VERSION_COUNTER_TTL` | `0` | TTL for version counters (0 = no expiry) |
| `REDIS_CONNECT_TIMEOUT` | `10` | Seconds to wait for Redis on startup |
| `METRICS_PORT` | `8004` | Prometheus metrics HTTP port |
| `LOG_LEVEL` | `INFO` | Logging level |
| `SERVICE_VERSION` | `0.1.0` | Reported in service-info metric |

## Running locally

```bash
# From the repo root
docker compose up -d redpanda redis

pip install -e libs/common -e services/normalization-service

KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
REDIS_URL=redis://localhost:6379/0 \
normalization-service
```

## Docker

```bash
# From the repo root
docker build \
  -f services/normalization-service/Dockerfile \
  -t mdrp/normalization-service:latest \
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
market.events.validated
        │
        ▼
  MdrpConsumer
        │
        ▼
  Normalizer
  ├── InstrumentMapper   (symbol → canonical + currency/unit)
  ├── TenorMapper        (raw string → canonical tenor + DeliveryPeriod)
  ├── Quality scoring    (fault penalties)
  └── Redis INCR         (version counter per curve_name)
        │
        ▼
  MdrpProducer
        │
        ▼
market.events.normalized
```
