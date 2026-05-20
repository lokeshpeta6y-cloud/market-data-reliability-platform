# Architecture Decision Record: Data Flow & Technology Choices

**Status:** Accepted  
**Date:** 2026-05-20  
**Authors:** Platform Engineering  

---

## Context

The Market Data Reliability Platform ingests real-time forward curve data from commodity market data providers (initially Databento / ICE Endex), validates and normalises it, stores it in a three-tier data lake (Bronze / Silver / Gold), and makes authoritative forward curve snapshots available to downstream risk and analytics systems via Snowflake.

This document records the key technology decisions made during the platform design phase and the reasoning behind each one.

---

## Decision 1: Redpanda over Apache Kafka

### Decision

Use [Redpanda](https://redpanda.com/) as the streaming backbone rather than Apache Kafka.

### Context

The platform requires a durable, ordered, replayable message bus between nine microservices. Apache Kafka is the industry standard for this pattern. Redpanda is a Kafka-API-compatible alternative written in C++.

### Reasoning

| Dimension | Apache Kafka | Redpanda |
|---|---|---|
| API compatibility | Reference implementation | Fully Kafka-compatible (same client libs, same topic/consumer APIs) |
| Operational dependencies | Requires separate ZooKeeper or KRaft controller quorum | Single binary, no ZooKeeper, built-in Raft consensus |
| Latency | JVM warm-up; typical p99 ~5 ms | C++ binary; typical p99 ~1 ms |
| Memory footprint | ~1 GB JVM heap minimum | ~256 MB per broker |
| Local development | Requires kafka + zookeeper containers | Single `redpanda` container |

Redpanda's identical Kafka API means all services use the `confluent-kafka-python` client without modification. Migrating to MSK (Apache Kafka on AWS) in production is a one-line configuration change (`KAFKA_BOOTSTRAP_SERVERS`), making Redpanda a pure operational simplification with no lock-in.

### Consequences

- All message schemas, consumer group semantics, topic configurations, and client code are identical to what would be written for Kafka.
- Local developer environments require only a single `redpanda` Docker container instead of two (`kafka` + `zookeeper`).
- Redpanda's built-in Schema Registry is available if we later adopt Avro or Protobuf serialisation.

---

## Decision 2: MinIO for Local S3 Object Storage

### Decision

Use [MinIO](https://min.io/) as the local Bronze tier object store, exposing an identical S3 API.

### Context

The Bronze tier writes validated raw market events as Parquet files partitioned by `provider/instrument/year/month/day/hour`. In production this will be AWS S3. During local development and CI, a real S3 bucket is impractical (cost, credential management, network latency).

### Reasoning

MinIO implements the S3 API completely, including all operations used by the platform:

- `PutObject` (bronze-writer)
- `GetObject` / `ListObjectsV2` (replay-engine, silver-loader)
- Pre-signed URLs (ops-api)

The `boto3` client code is identical for both MinIO and real S3. The only difference is the `endpoint_url` parameter, which is controlled by the `S3_ENDPOINT_URL` environment variable (`None` in production → uses AWS S3; set to `http://minio:9000` locally → uses MinIO).

The `BaseServiceSettings.s3_is_minio` property captures this distinction for services that need to adjust behaviour (e.g. disabling SSL verification for local MinIO).

### Consequences

- Zero cloud cost for local development and CI.
- Bronze Parquet files can be inspected locally via the MinIO Console at `http://localhost:9001`.
- Integration tests run against the same S3 client code that will hit real AWS S3 in production — no mocking layer introduces hidden divergence.
- AWS credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) are set to the MinIO admin credentials in `.env.example` and overridden with real IAM credentials in production.

---

## Decision 3: Parquet for the Bronze Tier

### Decision

Store Bronze tier data as Apache Parquet files, partitioned by `provider/instrument/year/month/day/hour`.

### Context

The Bronze tier is the immutable raw store — every event that passes schema validation is written here before any transformation. It serves two purposes: forensic inspection and replay source.

### Reasoning

**Columnar format for analytics.** Parquet is columnar rather than row-oriented. Queries that read only a subset of fields (e.g. `price`, `event_timestamp`) skip unread columns entirely. Athena (S3 Select) query costs are billed per byte scanned — columnar storage reduces cost by 5-10× compared to JSON or CSV for typical analytical queries.

**Compression.** Parquet with Snappy compression reduces storage size by ~60-80% compared to raw JSON NDJSON files. Forward curve data contains many repeated string values (`provider`, `instrument`, `currency`, `unit`) that compress extremely well with dictionary encoding.

**Schema enforcement.** Parquet embeds the schema in the file footer. The bronze-writer uses PyArrow to validate the schema at write time, catching type mismatches before they silently corrupt the Bronze layer.

**Replay performance.** The replay-engine reads Bronze files sequentially and re-emits them to Kafka. Parquet's row-group structure (default 128 MB) allows the engine to stream row groups without loading the entire file into memory.

**Partition pruning.** The `provider/instrument/year/month/day/hour` partition layout allows Athena and the replay-engine to skip entire partition directories when the query or replay window is bounded by provider, instrument, or time range.

### Consequences

- Bronze files cannot be inspected with a simple text editor. Use `pyarrow` or `pandas` locally, or query via Athena.
- Each bronze-writer instance must buffer events for up to `BRONZE_FLUSH_INTERVAL_SECONDS` (default: 60) before writing a row group, introducing a small write latency.
- The schema is fixed per Parquet file; schema evolution requires the bronze-writer to write new files under a new partition path rather than appending to existing files.

---

## Decision 4: Redis Bloom Filter for Deduplication

### Decision

Use a Redis-backed `SET NX EX` (set-if-not-exists with TTL) pattern in the `Deduplicator` class to detect and suppress duplicate events.

### Context

Providers may emit the same event multiple times (FaultType.DUPLICATE). The validation-service must detect and silently discard duplicates before they propagate to the validated topic. The deduplication check must be atomic and safe for multiple concurrent validation-service replicas.

### Reasoning

**O(1) lookups.** Redis `SET NX EX` is a single atomic command. The time complexity of checking and setting a key is O(1) regardless of how many keys are stored.

**Configurable false-positive rate.** The TTL (`DEDUP_TTL_SECONDS`, default 3600) controls the deduplication window. Events arriving more than one hour apart with the same `event_id` are treated as genuinely new (correct behaviour for provider replay after a long outage).

**Atomicity across replicas.** Multiple validation-service instances share the same Redis instance. The `SET NX` operation is atomic at the Redis level — there is no TOCTOU race between "check if key exists" and "insert key". Both replicas cannot simultaneously accept the same `event_id`.

**Low memory footprint.** Each dedup key stores a Unix timestamp string (~20 bytes) plus the key name (~40 bytes). At 10,000 events/second with a 1-hour window, the dedup keyspace requires approximately 2.2 GB of Redis memory — well within a single Redis instance's capacity.

**Note on bloom filters.** A true bloom filter would offer lower memory usage at the cost of false positives (some non-duplicate events incorrectly discarded). The current `SET NX` approach has zero false positives. If memory becomes a constraint at higher throughput, the `Deduplicator` can be migrated to a Redis Bloom Filter module (`BF.ADD` / `BF.EXISTS`) without changing the calling code.

### Consequences

- Redis is a required infrastructure dependency for the validation-service.
- The deduplication window is bounded by `DEDUP_TTL_SECONDS`. Events older than the TTL with the same `event_id` are not deduplicated — this is intentional and correct for replay scenarios.
- Redis persistence (`appendonly yes`) is recommended in production to avoid losing the dedup window on Redis restart.

---

## Decision 5: COPY INTO over Snowpipe for Snowflake Loading

### Decision

Load Silver and Gold data into Snowflake using explicit `COPY INTO` statements (triggered by the silver-loader and gold-loader services) rather than Snowpipe (event-driven continuous loading).

### Context

Snowflake offers two approaches for loading data from S3:

1. **Snowpipe** — event-driven, triggered by S3 event notifications, runs automatically as files arrive.
2. **COPY INTO** — SQL statement executed on demand, idempotent, transactional.

### Reasoning

**Replay idempotency.** During a Bronze replay, the silver-loader re-processes Parquet files that were already loaded. `COPY INTO` tracks which files have been loaded in the Snowflake load history and skips already-loaded files by default (`FORCE=FALSE`). Snowpipe does not provide this guarantee — re-triggering Snowpipe for an already-loaded file will load it again.

**Explicit control.** `COPY INTO` is executed as part of the silver-loader's processing loop, which means:
- Load timing is deterministic and observable via the replay job API.
- Errors are surfaced synchronously and can be included in the job status.
- Load batches can be sized to match Snowflake's optimal micro-partition size (100-500 MB compressed).

**Transactional semantics.** `COPY INTO` runs within a Snowflake transaction. If the load fails partway through, the transaction rolls back and no partial data is committed. Snowpipe loads are non-transactional — a failure mid-load may leave partial data in the target table.

**Cost predictability.** Snowpipe charges per credit consumed based on file size and frequency. With `COPY INTO`, the silver-loader controls exactly when Snowflake compute runs, making warehouse utilisation predictable and schedulable.

### Consequences

- Silver and Gold data is available in Snowflake with a latency equal to the silver-loader's flush interval (default: 5 minutes). This is acceptable for the platform's use case (risk analytics, not sub-second trading).
- The silver-loader and gold-loader must be monitored for consumer lag — if they fall behind, the `COPY INTO` cadence slows and Snowflake data freshness degrades.
- Snowflake load history must be monitored via `INFORMATION_SCHEMA.LOAD_HISTORY` to confirm successful loads.

---

## Decision 6: Provider Emulator as a Separate Service

### Decision

The `provider-emulator` is a standalone microservice, separate from the validation and normalisation services, even though it only exists for development and testing.

### Context

In production, market data arrives from external providers (Databento) via an ingestor service. In development and CI, a provider-emulator generates synthetic market events with configurable fault injection. The question was whether to embed the emulator logic in the validation-service or keep it separate.

### Reasoning

**Separation of concerns.** The emulator's job is to produce `RawMarketEvent` messages. The validation-service's job is to consume and validate them. Keeping them separate means each can be developed, tested, deployed, and scaled independently.

**Testability.** Because the emulator is a separate service with its own fault injection configuration (`FaultInjector`), integration and chaos tests can control the emulator via the ops-api (`POST /api/v1/test/set-fault-rates`) without touching the validation-service. This enables realistic fault scenarios that would be impossible if the fault injection were embedded in the consumer.

**Identical topology to production.** In production, the ingestor service (which talks to Databento) produces to `market.events.raw`. Keeping the emulator as a separate producer means the validation-service code is identical in development and production — there is no conditional "if dev use emulator, else use Databento" logic inside the validator.

**Emulator can be stopped independently.** Chaos tests need to simulate a provider outage by stopping the emulator. If the emulator were embedded in the validation-service, stopping it would also stop validation — a very different failure mode.

**Easy substitution.** When the real Databento adapter is ready, it replaces the emulator service without any changes to the validation-service, normalisation-service, or any downstream component. The `provider-emulator` and `databento-ingestor` services are interchangeable from the rest of the platform's perspective.

### Consequences

- One additional Docker container in the local development stack.
- The emulator's `FaultInjector` is a test-only component; it is not deployed in production. The ops-api test endpoints (`/api/v1/test/*`) are only enabled when `ENVIRONMENT=development`.
- Chaos and integration tests depend on the emulator being healthy. The test suite checks `OPS_API_URL/api/v1/status` before running chaos scenarios to ensure the emulator is up.
