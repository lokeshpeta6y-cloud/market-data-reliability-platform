"""
Prometheus metrics registry for the Market Data Reliability Platform.

Each service imports the metric objects it needs from here. Metrics are registered
once at import time — calling register_metrics() in a service sets the service
label on all metrics and starts the HTTP exposition server.

Dashboards and alert rules reference these exact metric names.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server

# ---------------------------------------------------------------------------
# Ingestion metrics
# ---------------------------------------------------------------------------

EVENTS_INGESTED_TOTAL = Counter(
    "mdrp_events_ingested_total",
    "Total number of raw events received from upstream providers",
    ["provider", "instrument"],
)

EVENTS_PUBLISHED_TOTAL = Counter(
    "mdrp_events_published_total",
    "Total number of events published to a Kafka topic",
    ["topic", "provider"],
)

# ---------------------------------------------------------------------------
# Validation metrics
# ---------------------------------------------------------------------------

EVENTS_VALIDATED_TOTAL = Counter(
    "mdrp_events_validated_total",
    "Total events processed by the validation service",
    ["provider", "outcome"],  # outcome: passed | failed
)

EVENTS_DEDUPLICATED_TOTAL = Counter(
    "mdrp_events_deduplicated_total",
    "Total duplicate events discarded",
    ["provider"],
)

DLQ_EVENTS_TOTAL = Counter(
    "mdrp_dlq_events_total",
    "Total events routed to the dead-letter queue",
    ["provider", "failure_category"],
)

DLQ_QUEUE_DEPTH = Gauge(
    "mdrp_dlq_queue_depth",
    "Estimated number of unprocessed events in the DLQ",
)

VALIDATION_ERRORS_TOTAL = Counter(
    "mdrp_validation_errors_total",
    "Validation rule violations by type",
    ["provider", "error_type"],
)

# ---------------------------------------------------------------------------
# Fault injection metrics (emulator)
# ---------------------------------------------------------------------------

FAULTS_INJECTED_TOTAL = Counter(
    "mdrp_faults_injected_total",
    "Total faults injected by the provider emulator",
    ["fault_type"],
)

# ---------------------------------------------------------------------------
# Bronze storage metrics
# ---------------------------------------------------------------------------

BRONZE_WRITES_TOTAL = Counter(
    "mdrp_bronze_writes_total",
    "Total Parquet files written to the Bronze layer",
    ["provider", "outcome"],  # outcome: success | failed
)

BRONZE_WRITE_DURATION_SECONDS = Histogram(
    "mdrp_bronze_write_duration_seconds",
    "Time taken to write a Parquet batch to S3/MinIO",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

BRONZE_BYTES_WRITTEN_TOTAL = Counter(
    "mdrp_bronze_bytes_written_total",
    "Total bytes written to the Bronze layer",
    ["provider"],
)

# ---------------------------------------------------------------------------
# Normalisation metrics
# ---------------------------------------------------------------------------

EVENTS_NORMALIZED_TOTAL = Counter(
    "mdrp_events_normalized_total",
    "Total events successfully normalised to canonical schema",
    ["provider", "instrument"],
)

QUALITY_SCORE = Histogram(
    "mdrp_event_quality_score",
    "Distribution of provider event quality scores",
    ["provider"],
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)

# ---------------------------------------------------------------------------
# Snowflake loader metrics
# ---------------------------------------------------------------------------

SNOWFLAKE_LOADS_TOTAL = Counter(
    "mdrp_snowflake_loads_total",
    "Total Snowflake COPY INTO operations",
    ["layer", "outcome"],  # layer: silver | gold
)

SNOWFLAKE_LOAD_DURATION_SECONDS = Histogram(
    "mdrp_snowflake_load_duration_seconds",
    "Time taken for a Snowflake COPY INTO operation",
    ["layer"],
    buckets=[1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)

SNOWFLAKE_ROWS_LOADED_TOTAL = Counter(
    "mdrp_snowflake_rows_loaded_total",
    "Total rows loaded into Snowflake",
    ["layer"],
)

# ---------------------------------------------------------------------------
# Replay metrics
# ---------------------------------------------------------------------------

REPLAY_JOBS_TOTAL = Counter(
    "mdrp_replay_jobs_total",
    "Total replay jobs executed",
    ["source", "outcome"],
)

REPLAY_EVENTS_TOTAL = Counter(
    "mdrp_replay_events_total",
    "Total events replayed",
    ["source"],
)

REPLAY_DURATION_SECONDS = Histogram(
    "mdrp_replay_duration_seconds",
    "Total duration of a replay job",
    ["source"],
    buckets=[1.0, 5.0, 30.0, 60.0, 300.0, 600.0, 1800.0],
)

# ---------------------------------------------------------------------------
# Consumer lag metrics
# ---------------------------------------------------------------------------

CONSUMER_LAG = Gauge(
    "mdrp_consumer_lag_messages",
    "Number of messages behind the head of partition",
    ["topic", "partition", "consumer_group"],
)

# ---------------------------------------------------------------------------
# Provider health metrics
# ---------------------------------------------------------------------------

PROVIDER_LAST_EVENT_TIMESTAMP = Gauge(
    "mdrp_provider_last_event_timestamp_seconds",
    "Unix timestamp of the last event received from a provider",
    ["provider"],
)

PROVIDER_QUALITY_SCORE_GAUGE = Gauge(
    "mdrp_provider_quality_score",
    "Rolling average quality score per provider",
    ["provider"],
)

# ---------------------------------------------------------------------------
# Processing latency
# ---------------------------------------------------------------------------

EVENT_PROCESSING_LATENCY_SECONDS = Histogram(
    "mdrp_event_processing_latency_seconds",
    "End-to-end latency from event_timestamp to ingestion_timestamp",
    ["service", "provider"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0],
)

# ---------------------------------------------------------------------------
# Service info
# ---------------------------------------------------------------------------

SERVICE_INFO = Info(
    "mdrp_service",
    "Static metadata about the running service instance",
)


def register_metrics(service_name: str, port: int, version: str = "0.1.0") -> None:
    """Start the Prometheus HTTP metrics server and record service metadata."""
    SERVICE_INFO.info({"service": service_name, "version": version})
    start_http_server(port)
