-- =============================================================================
-- 005_stages_and_pipes.sql
-- Market Data Reliability Platform — Internal stages and Snowpipes
-- Run as SYSADMIN.
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE MDRP;
USE WAREHOUSE INGESTION_WH;

-- =============================================================================
-- Internal Stages
-- Silver-loader and gold-loader PUT files here then run COPY INTO.
-- =============================================================================

USE SCHEMA SILVER_EVENTS;

CREATE STAGE IF NOT EXISTS MDRP_SILVER_STAGE
  FILE_FORMAT = (
    TYPE             = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    NULL_IF           = ('NULL', 'null', '')
    COMPRESSION       = 'AUTO'
  )
  COMMENT = 'Internal stage for silver-loader COPY INTO operations';

USE SCHEMA GOLD_CURVES;

CREATE STAGE IF NOT EXISTS MDRP_GOLD_STAGE
  FILE_FORMAT = (
    TYPE              = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    NULL_IF            = ('NULL', 'null', '')
    COMPRESSION        = 'AUTO'
  )
  COMMENT = 'Internal stage for gold-loader COPY INTO operations';

-- =============================================================================
-- Shared file format — JSON newline-delimited (used by COPY INTO calls)
-- =============================================================================

USE SCHEMA SILVER_EVENTS;

CREATE FILE FORMAT IF NOT EXISTS JSON_STRIP_ARRAY
  TYPE              = JSON
  STRIP_OUTER_ARRAY = TRUE
  NULL_IF            = ('NULL', 'null', '')
  COMPRESSION        = AUTO
  COMMENT            = 'JSON file format with outer-array stripping for bulk loads';

-- =============================================================================
-- Snowpipes
-- Snowpipe enables near-real-time continuous loading when files are staged.
-- In production these are triggered by SQS notifications from the S3 bronze
-- bucket (or by the ECS loaders calling insertFiles via the REST API).
-- Uncomment and configure the S3 external stage ARN when enabling Snowpipe.
-- =============================================================================

-- ── CURVE_EVENTS pipe ────────────────────────────────────────────────────────

CREATE PIPE IF NOT EXISTS MDRP.SILVER_EVENTS.PIPE_CURVE_EVENTS
  AUTO_INGEST = FALSE     -- ECS services call insertFiles explicitly
  COMMENT     = 'Snowpipe for SILVER_EVENTS.CURVE_EVENTS — triggered by silver-loader'
AS
COPY INTO MDRP.SILVER_EVENTS.CURVE_EVENTS (
    event_id,
    provider,
    instrument,
    instrument_type,
    venue,
    curve_date,
    tenor,
    tenor_months,
    price,
    bid,
    ask,
    settlement_price,
    volume,
    open_interest,
    currency,
    unit,
    validation_status,
    validation_version,
    quality_score,
    event_timestamp,
    received_at,
    bronze_s3_key,
    batch_id,
    trace_id,
    source_schema_ver,
    raw_payload
)
FROM (
    SELECT
        $1:event_id::VARCHAR(36),
        $1:provider::VARCHAR(64),
        $1:instrument::VARCHAR(128),
        $1:instrument_type::VARCHAR(32),
        $1:venue::VARCHAR(32),
        $1:curve_date::DATE,
        $1:tenor::VARCHAR(32),
        $1:tenor_months::INTEGER,
        $1:price::NUMBER(20,8),
        $1:bid::NUMBER(20,8),
        $1:ask::NUMBER(20,8),
        $1:settlement_price::NUMBER(20,8),
        $1:volume::NUMBER(20,4),
        $1:open_interest::NUMBER(20,4),
        $1:currency::VARCHAR(8),
        $1:unit::VARCHAR(32),
        $1:validation_status::VARCHAR(16),
        $1:validation_version::VARCHAR(16),
        $1:quality_score::NUMBER(5,4),
        $1:event_timestamp::TIMESTAMP_TZ,
        $1:received_at::TIMESTAMP_TZ,
        $1:bronze_s3_key::VARCHAR(512),
        $1:batch_id::VARCHAR(36),
        $1:trace_id::VARCHAR(36),
        $1:source_schema_ver::VARCHAR(16),
        $1
    FROM @MDRP.SILVER_EVENTS.MDRP_SILVER_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'MDRP.SILVER_EVENTS.JSON_STRIP_ARRAY');

-- ── DLQ_EVENTS pipe ──────────────────────────────────────────────────────────

CREATE PIPE IF NOT EXISTS MDRP.SILVER_EVENTS.PIPE_DLQ_EVENTS
  AUTO_INGEST = FALSE
  COMMENT     = 'Snowpipe for SILVER_EVENTS.DLQ_EVENTS — triggered by silver-loader'
AS
COPY INTO MDRP.SILVER_EVENTS.DLQ_EVENTS (
    dlq_event_id,
    original_event_id,
    provider,
    instrument,
    failure_reason,
    failure_category,
    raw_payload,
    dlq_timestamp,
    retry_count,
    retry_status,
    trace_id
)
FROM (
    SELECT
        $1:dlq_event_id::VARCHAR(36),
        $1:original_event_id::VARCHAR(36),
        $1:provider::VARCHAR(64),
        $1:instrument::VARCHAR(128),
        $1:failure_reason::VARCHAR(1024),
        $1:failure_category::VARCHAR(64),
        $1,
        $1:dlq_timestamp::TIMESTAMP_TZ,
        COALESCE($1:retry_count::INTEGER, 0),
        $1:retry_status::VARCHAR(16),
        $1:trace_id::VARCHAR(36)
    FROM @MDRP.SILVER_EVENTS.MDRP_SILVER_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'MDRP.SILVER_EVENTS.JSON_STRIP_ARRAY');

-- ── FORWARD_CURVE_SNAPSHOTS pipe ─────────────────────────────────────────────

USE SCHEMA GOLD_CURVES;

CREATE FILE FORMAT IF NOT EXISTS JSON_STRIP_ARRAY
  TYPE              = JSON
  STRIP_OUTER_ARRAY = TRUE
  NULL_IF            = ('NULL', 'null', '')
  COMPRESSION        = AUTO
  COMMENT            = 'JSON file format for gold-layer loads';

CREATE PIPE IF NOT EXISTS MDRP.GOLD_CURVES.PIPE_FORWARD_CURVE_SNAPSHOTS
  AUTO_INGEST = FALSE
  COMMENT     = 'Snowpipe for GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS — triggered by gold-loader'
AS
COPY INTO MDRP.GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS (
    snapshot_id,
    provider,
    instrument,
    instrument_type,
    currency,
    unit,
    curve_date,
    tenors,
    front_price,
    curve_slope,
    num_tenors,
    quality_score,
    completeness_pct,
    stale_tenors,
    outlier_tenors,
    snapshot_time,
    batch_id,
    trace_id
)
FROM (
    SELECT
        $1:snapshot_id::VARCHAR(36),
        $1:provider::VARCHAR(64),
        $1:instrument::VARCHAR(128),
        $1:instrument_type::VARCHAR(32),
        $1:currency::VARCHAR(8),
        $1:unit::VARCHAR(32),
        $1:curve_date::DATE,
        $1:tenors::VARIANT,
        $1:front_price::NUMBER(20,8),
        $1:curve_slope::NUMBER(20,8),
        $1:num_tenors::INTEGER,
        $1:quality_score::NUMBER(5,4),
        $1:completeness_pct::NUMBER(5,2),
        COALESCE($1:stale_tenors::INTEGER, 0),
        COALESCE($1:outlier_tenors::INTEGER, 0),
        $1:snapshot_time::TIMESTAMP_TZ,
        $1:batch_id::VARCHAR(36),
        $1:trace_id::VARCHAR(36)
    FROM @MDRP.GOLD_CURVES.MDRP_GOLD_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'MDRP.GOLD_CURVES.JSON_STRIP_ARRAY');

-- ── PROVIDER_QUALITY_HISTORY pipe ────────────────────────────────────────────

CREATE PIPE IF NOT EXISTS MDRP.GOLD_CURVES.PIPE_PROVIDER_QUALITY_HISTORY
  AUTO_INGEST = FALSE
  COMMENT     = 'Snowpipe for GOLD_CURVES.PROVIDER_QUALITY_HISTORY — triggered by gold-loader'
AS
COPY INTO MDRP.GOLD_CURVES.PROVIDER_QUALITY_HISTORY (
    quality_history_id,
    provider,
    quality_score,
    events_count,
    dlq_count,
    warning_count,
    completeness_pct,
    avg_latency_ms,
    p99_latency_ms,
    measurement_window,
    window_start,
    window_end,
    snapshot_time
)
FROM (
    SELECT
        $1:quality_history_id::VARCHAR(36),
        $1:provider::VARCHAR(64),
        $1:quality_score::NUMBER(5,4),
        $1:events_count::INTEGER,
        COALESCE($1:dlq_count::INTEGER, 0),
        COALESCE($1:warning_count::INTEGER, 0),
        $1:completeness_pct::NUMBER(5,2),
        $1:avg_latency_ms::NUMBER(10,2),
        $1:p99_latency_ms::NUMBER(10,2),
        $1:measurement_window::VARCHAR(16),
        $1:window_start::TIMESTAMP_TZ,
        $1:window_end::TIMESTAMP_TZ,
        $1:snapshot_time::TIMESTAMP_TZ
    FROM @MDRP.GOLD_CURVES.MDRP_GOLD_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'MDRP.GOLD_CURVES.JSON_STRIP_ARRAY');
