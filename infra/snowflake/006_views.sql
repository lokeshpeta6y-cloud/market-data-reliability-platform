-- =============================================================================
-- 006_views.sql
-- Market Data Reliability Platform — Operational views
-- Run as SYSADMIN.
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE MARKET_DATA;
USE WAREHOUSE QUERY_WH;

-- =============================================================================
-- V_LATEST_CURVES
-- Latest forward-curve snapshot per (provider, instrument, curve_date).
-- Uses ROW_NUMBER to pick the most recent snapshot_time within each partition.
-- =============================================================================

USE SCHEMA GOLD_CURVES;

CREATE OR REPLACE VIEW V_LATEST_CURVES
  COMMENT = 'Latest forward-curve snapshot per provider/instrument/curve_date'
AS
WITH ranked AS (
    SELECT
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
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY provider, instrument, curve_date
            ORDER BY snapshot_time DESC
        ) AS rn
    FROM MARKET_DATA.GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS
)
SELECT
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
    loaded_at
FROM ranked
WHERE rn = 1;

-- =============================================================================
-- V_PROVIDER_QUALITY_SUMMARY
-- Rolling quality statistics per provider over the last 7 days (daily windows).
-- Shows trend columns to surface improving or degrading providers quickly.
-- =============================================================================

CREATE OR REPLACE VIEW V_PROVIDER_QUALITY_SUMMARY
  COMMENT = 'Rolling 7-day quality stats per provider with trend indicators'
AS
WITH daily AS (
    SELECT
        provider,
        DATE_TRUNC('DAY', window_start)         AS quality_date,
        AVG(quality_score)                       AS avg_quality_score,
        SUM(events_count)                        AS total_events,
        SUM(dlq_count)                           AS total_dlq,
        SUM(warning_count)                       AS total_warnings,
        AVG(completeness_pct)                    AS avg_completeness_pct,
        AVG(avg_latency_ms)                      AS avg_latency_ms,
        MAX(p99_latency_ms)                      AS max_p99_latency_ms,
        CASE
            WHEN SUM(events_count) > 0
            THEN ROUND(SUM(dlq_count) * 100.0 / NULLIF(SUM(events_count), 0), 4)
            ELSE NULL
        END                                      AS dlq_rate_pct
    FROM MARKET_DATA.GOLD_CURVES.PROVIDER_QUALITY_HISTORY
    WHERE measurement_window = 'DAILY'
      AND window_start >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
),
with_trend AS (
    SELECT
        provider,
        quality_date,
        avg_quality_score,
        total_events,
        total_dlq,
        total_warnings,
        avg_completeness_pct,
        avg_latency_ms,
        max_p99_latency_ms,
        dlq_rate_pct,
        LAG(avg_quality_score) OVER (
            PARTITION BY provider ORDER BY quality_date
        )                                        AS prev_day_quality_score,
        avg_quality_score - LAG(avg_quality_score) OVER (
            PARTITION BY provider ORDER BY quality_date
        )                                        AS quality_score_delta
    FROM daily
)
SELECT
    provider,
    quality_date,
    ROUND(avg_quality_score, 4)          AS avg_quality_score,
    total_events,
    total_dlq,
    total_warnings,
    ROUND(avg_completeness_pct, 2)       AS avg_completeness_pct,
    ROUND(avg_latency_ms, 2)             AS avg_latency_ms,
    ROUND(max_p99_latency_ms, 2)         AS max_p99_latency_ms,
    ROUND(dlq_rate_pct, 4)               AS dlq_rate_pct,
    ROUND(quality_score_delta, 4)        AS quality_score_delta,
    CASE
        WHEN quality_score_delta > 0.01  THEN 'IMPROVING'
        WHEN quality_score_delta < -0.01 THEN 'DEGRADING'
        ELSE                                  'STABLE'
    END                                  AS trend
FROM with_trend
ORDER BY provider, quality_date DESC;

-- =============================================================================
-- V_DLQ_SUMMARY
-- DLQ event counts grouped by failure_category and provider.
-- Covers the last 24 hours and last 7 days for side-by-side comparison.
-- =============================================================================

USE SCHEMA SILVER_EVENTS;

CREATE OR REPLACE VIEW V_DLQ_SUMMARY
  COMMENT = 'DLQ event counts by failure_category and provider — last 24h and 7d'
AS
SELECT
    provider,
    failure_category,
    COUNT_IF(dlq_timestamp >= DATEADD('HOUR', -24, CURRENT_TIMESTAMP()))  AS events_last_24h,
    COUNT_IF(dlq_timestamp >= DATEADD('DAY',  -7,  CURRENT_TIMESTAMP()))  AS events_last_7d,
    COUNT(*)                                                               AS events_total,
    MIN(dlq_timestamp)                                                     AS first_seen,
    MAX(dlq_timestamp)                                                     AS last_seen,
    COUNT_IF(retry_status = 'RESOLVED')                                   AS resolved_count,
    COUNT_IF(retry_status = 'PENDING')                                    AS pending_count,
    COUNT_IF(retry_status = 'ABANDONED')                                  AS abandoned_count,
    ROUND(
        COUNT_IF(retry_status = 'RESOLVED') * 100.0 / NULLIF(COUNT(*), 0), 2
    )                                                                      AS resolution_rate_pct
FROM MARKET_DATA.SILVER_EVENTS.DLQ_EVENTS
GROUP BY provider, failure_category
ORDER BY events_last_24h DESC, events_last_7d DESC;

-- =============================================================================
-- V_CURVE_COMPLETENESS
-- Completeness percentage per instrument over the last 24 hours.
-- Compares actual snapshot count to the expected snapshot count (per provider
-- SLA: 24 snapshots per instrument per day = one per hour).
-- =============================================================================

USE SCHEMA GOLD_CURVES;

CREATE OR REPLACE VIEW V_CURVE_COMPLETENESS
  COMMENT = 'Completeness % per provider/instrument over the last 24 hours'
AS
WITH hourly_slots AS (
    -- Generate one row per expected hour in the last 24h window
    SELECT
        DATEADD('HOUR', -seq4(), CURRENT_TIMESTAMP())  AS expected_hour
    FROM TABLE(GENERATOR(ROWCOUNT => 24))
),
instruments AS (
    SELECT DISTINCT provider, instrument
    FROM MARKET_DATA.GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS
    WHERE snapshot_time >= DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
),
expected AS (
    SELECT
        i.provider,
        i.instrument,
        DATE_TRUNC('HOUR', h.expected_hour)            AS expected_slot
    FROM instruments i
    CROSS JOIN hourly_slots h
),
actuals AS (
    SELECT
        provider,
        instrument,
        DATE_TRUNC('HOUR', snapshot_time)              AS actual_slot,
        COUNT(*)                                        AS snapshots_in_slot,
        MAX(quality_score)                              AS best_quality_score,
        MAX(completeness_pct)                           AS best_completeness_pct
    FROM MARKET_DATA.GOLD_CURVES.FORWARD_CURVE_SNAPSHOTS
    WHERE snapshot_time >= DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
    GROUP BY 1, 2, 3
),
joined AS (
    SELECT
        e.provider,
        e.instrument,
        e.expected_slot,
        CASE WHEN a.actual_slot IS NOT NULL THEN 1 ELSE 0 END  AS slot_filled,
        a.snapshots_in_slot,
        a.best_quality_score,
        a.best_completeness_pct
    FROM expected e
    LEFT JOIN actuals a
        ON  a.provider   = e.provider
        AND a.instrument = e.instrument
        AND a.actual_slot = e.expected_slot
)
SELECT
    provider,
    instrument,
    COUNT(*)                                                        AS expected_slots,
    SUM(slot_filled)                                                AS filled_slots,
    ROUND(SUM(slot_filled) * 100.0 / NULLIF(COUNT(*), 0), 2)       AS completeness_pct,
    ROUND(AVG(CASE WHEN slot_filled = 1 THEN best_quality_score END), 4)
                                                                    AS avg_quality_score,
    ROUND(AVG(CASE WHEN slot_filled = 1 THEN best_completeness_pct END), 2)
                                                                    AS avg_curve_completeness_pct,
    CASE
        WHEN SUM(slot_filled) * 100.0 / NULLIF(COUNT(*), 0) >= 95  THEN 'GREEN'
        WHEN SUM(slot_filled) * 100.0 / NULLIF(COUNT(*), 0) >= 80  THEN 'AMBER'
        ELSE                                                              'RED'
    END                                                             AS rag_status,
    MIN(CASE WHEN slot_filled = 0 THEN expected_slot END)           AS first_gap,
    MAX(CASE WHEN slot_filled = 0 THEN expected_slot END)           AS last_gap
FROM joined
GROUP BY provider, instrument
ORDER BY completeness_pct ASC, provider, instrument;

-- =============================================================================
-- Grant views to MDRP_READER
-- =============================================================================

USE ROLE SECURITYADMIN;

GRANT SELECT ON VIEW MARKET_DATA.GOLD_CURVES.V_LATEST_CURVES          TO ROLE MDRP_READER;
GRANT SELECT ON VIEW MARKET_DATA.GOLD_CURVES.V_PROVIDER_QUALITY_SUMMARY TO ROLE MDRP_READER;
GRANT SELECT ON VIEW MARKET_DATA.SILVER_EVENTS.V_DLQ_SUMMARY          TO ROLE MDRP_READER;
GRANT SELECT ON VIEW MARKET_DATA.GOLD_CURVES.V_CURVE_COMPLETENESS     TO ROLE MDRP_READER;

GRANT SELECT ON VIEW MARKET_DATA.GOLD_CURVES.V_LATEST_CURVES          TO ROLE MDRP_LOADER;
GRANT SELECT ON VIEW MARKET_DATA.GOLD_CURVES.V_PROVIDER_QUALITY_SUMMARY TO ROLE MDRP_LOADER;
GRANT SELECT ON VIEW MARKET_DATA.SILVER_EVENTS.V_DLQ_SUMMARY          TO ROLE MDRP_LOADER;
GRANT SELECT ON VIEW MARKET_DATA.GOLD_CURVES.V_CURVE_COMPLETENESS     TO ROLE MDRP_LOADER;
