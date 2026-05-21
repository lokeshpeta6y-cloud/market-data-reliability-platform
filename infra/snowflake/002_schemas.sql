-- =============================================================================
-- 002_schemas.sql
-- Market Data Reliability Platform — Schema setup
-- Run as SYSADMIN.
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE MARKET_DATA;

-- =============================================================================
-- BRONZE_EVENTS
-- Reference schema only. Raw event data lives in S3 (Bronze bucket).
-- Snowflake external tables or Athena can be used for ad-hoc Bronze queries.
-- No persistent tables are created here by the application.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS BRONZE_EVENTS
  DATA_RETENTION_TIME_IN_DAYS = 1
  COMMENT = 'Reference schema for Bronze layer. Actual data resides in S3.';

-- =============================================================================
-- SILVER_EVENTS
-- Validated, normalised events loaded by silver-loader.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS SILVER_EVENTS
  DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'Validated and normalised market events (Silver layer)';

-- =============================================================================
-- GOLD_CURVES
-- Aggregated forward-curve snapshots and quality metrics loaded by gold-loader.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS GOLD_CURVES
  DATA_RETENTION_TIME_IN_DAYS = 14
  COMMENT = 'Aggregated forward curves and quality history (Gold layer)';
