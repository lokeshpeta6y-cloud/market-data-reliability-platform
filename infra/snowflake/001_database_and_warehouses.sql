-- =============================================================================
-- 001_database_and_warehouses.sql
-- Market Data Reliability Platform — Snowflake bootstrap
-- Run as SYSADMIN (for objects) and ACCOUNTADMIN (for grants).
-- =============================================================================

USE ROLE SYSADMIN;

-- =============================================================================
-- Database
-- =============================================================================

CREATE DATABASE IF NOT EXISTS MDRP
  DATA_RETENTION_TIME_IN_DAYS = 7
  COMMENT = 'Market Data Reliability Platform — all layers';

-- =============================================================================
-- Compute Warehouses
-- =============================================================================

-- INGESTION_WH: used by silver-loader and gold-loader ECS services.
-- X-Small is sufficient for row-by-row micro-batch COPY INTO loads.
-- Auto-suspend after 60 s idle; auto-resume on first query.
CREATE WAREHOUSE IF NOT EXISTS INGESTION_WH
  WAREHOUSE_SIZE          = 'X-SMALL'
  AUTO_SUSPEND            = 60
  AUTO_RESUME             = TRUE
  INITIALLY_SUSPENDED     = TRUE
  MAX_CLUSTER_COUNT       = 1
  MIN_CLUSTER_COUNT       = 1
  SCALING_POLICY          = 'ECONOMY'
  COMMENT                 = 'Loader warehouse — silver and gold ECS services';

-- QUERY_WH: used by analysts and the ops-api for ad-hoc queries and views.
-- X-Small default; analysts can resize temporarily through the UI.
CREATE WAREHOUSE IF NOT EXISTS QUERY_WH
  WAREHOUSE_SIZE          = 'X-SMALL'
  AUTO_SUSPEND            = 60
  AUTO_RESUME             = TRUE
  INITIALLY_SUSPENDED     = TRUE
  MAX_CLUSTER_COUNT       = 1
  MIN_CLUSTER_COUNT       = 1
  SCALING_POLICY          = 'ECONOMY'
  COMMENT                 = 'Ad-hoc query warehouse — analysts and ops-api';
