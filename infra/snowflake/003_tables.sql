-- =============================================================================
-- 003_tables.sql
-- Market Data Reliability Platform — Full table DDL
-- Run as SYSADMIN.
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE MARKET_DATA;
USE WAREHOUSE INGESTION_WH;

-- =============================================================================
-- SILVER_EVENTS.CURVE_EVENTS
-- One row per validated market event received from a data provider.
-- Populated by silver-loader via COPY INTO from the Bronze S3 bucket.
-- =============================================================================

USE SCHEMA SILVER_EVENTS;

CREATE TABLE IF NOT EXISTS CURVE_EVENTS (
    -- Surrogate key
    event_id            VARCHAR(36)     NOT NULL    COMMENT 'UUID assigned by validation-service',

    -- Source identification
    provider            VARCHAR(64)     NOT NULL    COMMENT 'Data provider name (e.g. databento, bloomberg)',
    instrument          VARCHAR(128)    NOT NULL    COMMENT 'Instrument identifier (e.g. TTF_2025_JAN)',
    instrument_type     VARCHAR(32)                 COMMENT 'Asset class / instrument type (e.g. FUTURES, SWAP)',
    venue               VARCHAR(32)                 COMMENT 'Execution venue or exchange',

    -- Curve data
    curve_date          DATE            NOT NULL    COMMENT 'As-of date for the forward curve',
    tenor               VARCHAR(32)                 COMMENT 'Tenor label (e.g. 1M, 3M, 1Y)',
    tenor_months        INTEGER                     COMMENT 'Numeric tenor expressed in months',
    price               NUMBER(20, 8)               COMMENT 'Mid-market price',
    bid                 NUMBER(20, 8)               COMMENT 'Bid price',
    ask                 NUMBER(20, 8)               COMMENT 'Ask price',
    settlement_price    NUMBER(20, 8)               COMMENT 'Official settlement price if available',
    volume              NUMBER(20, 4)               COMMENT 'Traded volume',
    open_interest       NUMBER(20, 4)               COMMENT 'Open interest',
    currency            VARCHAR(8)                  COMMENT 'Pricing currency (ISO 4217)',
    unit                VARCHAR(32)                 COMMENT 'Unit of measure (e.g. MWh, MMBtu)',

    -- Validation metadata
    validation_status   VARCHAR(16)     NOT NULL    COMMENT 'PASSED | FAILED | WARNING',
    validation_version  VARCHAR(16)                 COMMENT 'Version of the validation ruleset applied',
    quality_score       NUMBER(5, 4)                COMMENT 'Computed quality score 0.0–1.0',

    -- Timestamps
    event_timestamp     TIMESTAMP_TZ    NOT NULL    COMMENT 'Timestamp of the original market event (UTC)',
    received_at         TIMESTAMP_TZ    NOT NULL    COMMENT 'Timestamp when the event was received by the platform',
    loaded_at           TIMESTAMP_TZ    NOT NULL
                            DEFAULT CURRENT_TIMESTAMP()
                                                    COMMENT 'Timestamp when the row was loaded into Snowflake',

    -- Lineage
    bronze_s3_key       VARCHAR(512)                COMMENT 'S3 object key of the source Bronze file',
    batch_id            VARCHAR(36)                 COMMENT 'Ingestion batch identifier',
    trace_id            VARCHAR(36)                 COMMENT 'Distributed trace ID for cross-service correlation',
    source_schema_ver   VARCHAR(16)                 COMMENT 'Schema version of the source Bronze payload',

    -- Raw payload preserved for auditability
    raw_payload         VARIANT                     COMMENT 'Original JSON payload before normalisation',

    CONSTRAINT pk_curve_events PRIMARY KEY (event_id)
)
CLUSTER BY (curve_date, provider, instrument)
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Validated and normalised market curve events (Silver layer)';

-- =============================================================================
-- SILVER_EVENTS.DLQ_EVENTS
-- Events that failed validation and were sent to the dead-letter queue.
-- =============================================================================

CREATE TABLE IF NOT EXISTS DLQ_EVENTS (
    dlq_event_id        VARCHAR(36)     NOT NULL    COMMENT 'UUID assigned to the DLQ entry',
    original_event_id   VARCHAR(36)                 COMMENT 'Original event UUID if available',
    provider            VARCHAR(64)     NOT NULL    COMMENT 'Data provider that sent the event',
    instrument          VARCHAR(128)                COMMENT 'Instrument identifier if parseable',
    failure_reason      VARCHAR(1024)   NOT NULL    COMMENT 'Human-readable description of the failure',
    failure_category    VARCHAR(64)     NOT NULL    COMMENT 'Structured failure category (e.g. SCHEMA_MISMATCH, STALE_DATA, PRICE_SPIKE)',
    raw_payload         VARIANT                     COMMENT 'Original raw JSON payload',
    dlq_timestamp       TIMESTAMP_TZ    NOT NULL    COMMENT 'Timestamp when the event entered the DLQ',
    retry_count         INTEGER         NOT NULL
                            DEFAULT 0               COMMENT 'Number of replay attempts',
    last_retry_at       TIMESTAMP_TZ                COMMENT 'Timestamp of the most recent replay attempt',
    retry_status        VARCHAR(16)                 COMMENT 'PENDING | RETRYING | RESOLVED | ABANDONED',
    trace_id            VARCHAR(36)                 COMMENT 'Distributed trace ID',
    loaded_at           TIMESTAMP_TZ    NOT NULL
                            DEFAULT CURRENT_TIMESTAMP()
                                                    COMMENT 'Timestamp when the row was loaded into Snowflake',

    CONSTRAINT pk_dlq_events PRIMARY KEY (dlq_event_id)
)
CLUSTER BY (dlq_timestamp, provider, failure_category)
DATA_RETENTION_TIME_IN_DAYS = 30
COMMENT = 'Dead-letter queue events — validation failures requiring investigation or replay';

-- =============================================================================
-- GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS
-- Aggregated forward curve snapshots produced by gold-loader.
-- One row per (provider, instrument, curve_date, snapshot_time).
-- =============================================================================

USE SCHEMA GOLD_CURVES;

CREATE TABLE IF NOT EXISTS FORWARD_CURVE_SNAPSHOTS (
    snapshot_id         VARCHAR(36)     NOT NULL    COMMENT 'UUID for the snapshot row',
    provider            VARCHAR(64)     NOT NULL    COMMENT 'Data provider',
    instrument          VARCHAR(128)    NOT NULL    COMMENT 'Instrument identifier',
    instrument_type     VARCHAR(32)                 COMMENT 'Asset class / instrument type',
    currency            VARCHAR(8)                  COMMENT 'Pricing currency (ISO 4217)',
    unit                VARCHAR(32)                 COMMENT 'Unit of measure',

    -- Curve shape
    curve_date          DATE            NOT NULL    COMMENT 'As-of date of the forward curve',
    tenors              VARIANT                     COMMENT 'JSON array of {tenor, months, price, bid, ask} objects',
    front_price         NUMBER(20, 8)               COMMENT 'Front-month price (convenience column)',
    curve_slope         NUMBER(20, 8)               COMMENT 'Simple first-to-last-tenor price slope',
    num_tenors          INTEGER                     COMMENT 'Number of valid tenors in the snapshot',

    -- Quality indicators
    quality_score       NUMBER(5, 4)                COMMENT 'Aggregate quality score for this snapshot 0.0–1.0',
    completeness_pct    NUMBER(5, 2)                COMMENT 'Percentage of expected tenors present',
    stale_tenors        INTEGER         DEFAULT 0   COMMENT 'Count of tenors with stale prices',
    outlier_tenors      INTEGER         DEFAULT 0   COMMENT 'Count of tenors flagged as price outliers',

    -- Timestamps
    snapshot_time       TIMESTAMP_TZ    NOT NULL    COMMENT 'Timestamp at which the snapshot was taken (UTC)',
    loaded_at           TIMESTAMP_TZ    NOT NULL
                            DEFAULT CURRENT_TIMESTAMP()
                                                    COMMENT 'Timestamp when the row was loaded into Snowflake',
    batch_id            VARCHAR(36)                 COMMENT 'Gold-loader batch identifier',
    trace_id            VARCHAR(36)                 COMMENT 'Distributed trace ID',

    CONSTRAINT pk_forward_curve_snapshots PRIMARY KEY (snapshot_id)
)
CLUSTER BY (curve_date, provider, instrument)
DATA_RETENTION_TIME_IN_DAYS = 14
COMMENT = 'Aggregated forward curve snapshots — one row per provider/instrument/curve_date/snapshot (Gold layer)';

-- =============================================================================
-- GOLD_CURVES.PROVIDER_QUALITY_HISTORY
-- Rolling quality metrics per provider, computed by gold-loader.
-- =============================================================================

CREATE TABLE IF NOT EXISTS PROVIDER_QUALITY_HISTORY (
    quality_history_id  VARCHAR(36)     NOT NULL    COMMENT 'UUID for this quality record',
    provider            VARCHAR(64)     NOT NULL    COMMENT 'Data provider',
    quality_score       NUMBER(5, 4)    NOT NULL    COMMENT 'Weighted quality score 0.0–1.0',
    events_count        INTEGER         NOT NULL    COMMENT 'Total events processed in the measurement window',
    dlq_count           INTEGER         NOT NULL    DEFAULT 0
                                                    COMMENT 'Events that entered the DLQ in the measurement window',
    warning_count       INTEGER         NOT NULL    DEFAULT 0
                                                    COMMENT 'Events that passed with warnings',
    completeness_pct    NUMBER(5, 2)                COMMENT 'Average completeness % across all instruments for this provider',
    avg_latency_ms      NUMBER(10, 2)               COMMENT 'Average event-to-load latency in milliseconds',
    p99_latency_ms      NUMBER(10, 2)               COMMENT 'P99 event-to-load latency in milliseconds',
    measurement_window  VARCHAR(16)     NOT NULL    COMMENT 'Granularity label: HOURLY | DAILY | WEEKLY',
    window_start        TIMESTAMP_TZ    NOT NULL    COMMENT 'Start of the measurement window (inclusive)',
    window_end          TIMESTAMP_TZ    NOT NULL    COMMENT 'End of the measurement window (exclusive)',
    snapshot_time       TIMESTAMP_TZ    NOT NULL    COMMENT 'Timestamp when this quality record was computed',
    loaded_at           TIMESTAMP_TZ    NOT NULL
                            DEFAULT CURRENT_TIMESTAMP()
                                                    COMMENT 'Timestamp when the row was loaded into Snowflake',

    CONSTRAINT pk_provider_quality_history PRIMARY KEY (quality_history_id)
)
CLUSTER BY (snapshot_time, provider)
DATA_RETENTION_TIME_IN_DAYS = 90
COMMENT = 'Historical quality metrics per provider — used for SLA tracking and trend analysis';
