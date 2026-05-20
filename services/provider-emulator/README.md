# provider-emulator

Synthetic and real-data market event producer for the Market Data Reliability Platform.

The provider emulator is the upstream edge of the MDRP pipeline.  It generates
realistic EEX/CME-style energy forward curve data, injects configurable faults,
and publishes raw events to the `market.events.raw` Redpanda topic.

## What it does

1. **Generates synthetic forward curves** for TTF, NBP, TTF_POWER, BRENT, WTI, and EU_ETS
   using geometric Brownian motion with mean reversion.  Prices are anchored near
   real-world levels (TTF ~30 EUR/MWh, Brent ~80 USD/bbl, etc.) and evolve
   plausibly across publish cycles.

2. **Optionally ingests real Databento data** when `DATABENTO_API_KEY` is set.
   OHLCV-1d bars are pulled for CME-mapped instruments; unmapped instruments
   continue to use synthetic data.

3. **Injects realistic faults** before publishing:
   - **DUPLICATE** — same event published twice
   - **MALFORMED** — required payload field nulled or corrupted
   - **DELAYED** — event held for 2–30 s before release
   - **OUT_OF_ORDER** — event held briefly and released out of sequence
   - **SCHEMA_DRIFT** — payload field renamed (e.g. `price` → `px`)
   - **STALE** — `event_timestamp` backdated 2–24 hours
   - **PARTIAL_CURVE** — some tenors dropped from a curve batch

4. **Publishes** `RawMarketEvent` JSON to `market.events.raw` with `instrument`
   as the Kafka key (preserves per-instrument ordering).

5. **Exposes Prometheus metrics** on port 8001.

## Configuration

All settings are read from environment variables (or a `.env` file in the working directory).

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Redpanda / Kafka bootstrap address |
| `PUBLISH_INTERVAL_SECONDS` | `5` | Seconds between full forward-curve publish cycles |
| `INSTRUMENTS` | `TTF,NBP,TTF_POWER,BRENT,WTI,EU_ETS` | Comma-separated list of instruments to simulate |
| `PROVIDER_NAME` | `provider-emulator` | `provider` label on every event |
| `METRICS_PORT` | `8001` | Prometheus exposition port |
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `DATABENTO_API_KEY` | _(unset)_ | Enables real Databento data when set |
| `DATABENTO_DATASET` | `GLBX.MDP3` | Databento dataset to pull from |
| `DATABENTO_LOOKBACK_DAYS` | `5` | Days of history pulled from Databento per refresh |
| `FAULT_RATE_DUPLICATE` | `0.02` | Probability of duplicating an event |
| `FAULT_RATE_MALFORMED` | `0.01` | Probability of corrupting an event's payload |
| `FAULT_RATE_DELAYED` | `0.05` | Probability of holding an event for a random delay |
| `FAULT_RATE_OUT_OF_ORDER` | `0.03` | Probability of releasing an event out of sequence |
| `FAULT_RATE_SCHEMA_DRIFT` | `0.005` | Probability of renaming a payload field |
| `FAULT_RATE_STALE` | `0.01` | Probability of backdating an event's timestamp |
| `FAULT_RATE_PARTIAL_CURVE` | `0.02` | Probability of dropping tenors from a curve batch |
| `DELAY_MIN_SECONDS` | `2.0` | Minimum hold time for DELAYED events |
| `DELAY_MAX_SECONDS` | `30.0` | Maximum hold time for DELAYED events |
| `DELAY_QUEUE_MAX_SIZE` | `500` | Maximum events in the delay / OOO hold queues |

## Running standalone

```bash
# Install (from monorepo root)
pip install -e libs/common
pip install -e services/provider-emulator

# Run with defaults (synthetic data only, Kafka at localhost:9092)
provider-emulator

# With real Databento data
DATABENTO_API_KEY=db-xxxx provider-emulator

# All faults disabled (clean data only)
FAULT_RATE_DUPLICATE=0 FAULT_RATE_MALFORMED=0 FAULT_RATE_DELAYED=0 \
FAULT_RATE_OUT_OF_ORDER=0 FAULT_RATE_SCHEMA_DRIFT=0 FAULT_RATE_STALE=0 \
FAULT_RATE_PARTIAL_CURVE=0 provider-emulator
```

## Running with Docker

```bash
# Build (from monorepo root — Docker context must be the root)
docker build \
  -f services/provider-emulator/Dockerfile \
  -t mdrp/provider-emulator:latest \
  .

# Run against a local Redpanda instance
docker run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
  -e PUBLISH_INTERVAL_SECONDS=5 \
  -p 8001:8001 \
  mdrp/provider-emulator:latest

# With Databento and the optional SDK installed
docker build \
  --build-arg INSTALL_DATABENTO=true \
  -f services/provider-emulator/Dockerfile \
  -t mdrp/provider-emulator:databento \
  .

docker run --rm \
  -e DATABENTO_API_KEY=db-xxxx \
  -e KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
  -p 8001:8001 \
  mdrp/provider-emulator:databento
```

## Metrics

The emulator exposes the following Prometheus metrics at `http://localhost:8001/metrics`:

| Metric | Type | Description |
|---|---|---|
| `mdrp_events_ingested_total` | Counter | Events generated (pre-fault), by provider + instrument |
| `mdrp_events_published_total` | Counter | Events published to Kafka, by topic + provider |
| `mdrp_faults_injected_total` | Counter | Faults applied, by fault_type |
| `mdrp_emulator_delay_queue_depth` | Gauge | Events currently held in the delay queue |
| `mdrp_emulator_ooo_queue_depth` | Gauge | Events currently held in the OOO queue |
| `mdrp_emulator_publish_cycle_duration_seconds` | Gauge | Wall time of the last publish cycle |
| `mdrp_provider_last_event_timestamp_seconds` | Gauge | Unix timestamp of the last published event |
| `mdrp_service_info` | Info | Static service metadata (name, version) |
