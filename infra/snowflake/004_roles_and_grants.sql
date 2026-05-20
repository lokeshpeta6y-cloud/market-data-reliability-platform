-- =============================================================================
-- 004_roles_and_grants.sql
-- Market Data Reliability Platform — Roles, grants, service user
-- Run as SECURITYADMIN (role/grant management) and SYSADMIN (warehouse grants).
-- =============================================================================

-- =============================================================================
-- Create roles
-- =============================================================================

USE ROLE SECURITYADMIN;

-- MDRP_LOADER: granted to silver-loader and gold-loader ECS services.
-- Write access to Silver and Gold schemas; read on Bronze reference schema.
CREATE ROLE IF NOT EXISTS MDRP_LOADER
  COMMENT = 'Service role for MDRP ECS loader services — INSERT/COPY INTO privileges';

-- MDRP_READER: granted to analysts, dashboards, and ops-api read paths.
-- SELECT-only across all MDRP schemas and views.
CREATE ROLE IF NOT EXISTS MDRP_READER
  COMMENT = 'Read-only role for analysts and internal tooling';

-- =============================================================================
-- Database-level grants
-- =============================================================================

-- LOADER: full usage on the database
GRANT USAGE ON DATABASE MDRP TO ROLE MDRP_LOADER;
GRANT USAGE ON DATABASE MDRP TO ROLE MDRP_READER;

-- =============================================================================
-- Schema-level grants — SILVER_EVENTS
-- =============================================================================

GRANT USAGE ON SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_LOADER;
GRANT USAGE ON SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_READER;

-- Loader: insert and copy
GRANT INSERT, UPDATE, TRUNCATE, DELETE ON ALL TABLES IN SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_LOADER;
GRANT SELECT ON ALL TABLES IN SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_LOADER;
-- Ensure future tables are also covered
GRANT INSERT, UPDATE, TRUNCATE, DELETE ON FUTURE TABLES IN SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_LOADER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_LOADER;

-- Reader: select only
GRANT SELECT ON ALL TABLES IN SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_READER;
GRANT SELECT ON ALL VIEWS  IN SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_READER;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_READER;

-- =============================================================================
-- Schema-level grants — GOLD_CURVES
-- =============================================================================

GRANT USAGE ON SCHEMA MDRP.GOLD_CURVES TO ROLE MDRP_LOADER;
GRANT USAGE ON SCHEMA MDRP.GOLD_CURVES TO ROLE MDRP_READER;

GRANT INSERT, UPDATE, TRUNCATE, DELETE ON ALL TABLES IN SCHEMA MDRP.GOLD_CURVES TO ROLE MDRP_LOADER;
GRANT SELECT ON ALL TABLES IN SCHEMA MDRP.GOLD_CURVES TO ROLE MDRP_LOADER;
GRANT INSERT, UPDATE, TRUNCATE, DELETE ON FUTURE TABLES IN SCHEMA MDRP.GOLD_CURVES TO ROLE MDRP_LOADER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MDRP.GOLD_CURVES TO ROLE MDRP_LOADER;

GRANT SELECT ON ALL TABLES IN SCHEMA MDRP.GOLD_CURVES TO ROLE MDRP_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MDRP.GOLD_CURVES TO ROLE MDRP_READER;
GRANT SELECT ON ALL VIEWS  IN SCHEMA MDRP.GOLD_CURVES TO ROLE MDRP_READER;
GRANT SELECT ON FUTURE VIEWS  IN SCHEMA MDRP.GOLD_CURVES TO ROLE MDRP_READER;

-- =============================================================================
-- Schema-level grants — BRONZE_EVENTS (reference only)
-- =============================================================================

GRANT USAGE ON SCHEMA MDRP.BRONZE_EVENTS TO ROLE MDRP_LOADER;
GRANT USAGE ON SCHEMA MDRP.BRONZE_EVENTS TO ROLE MDRP_READER;

GRANT SELECT ON ALL TABLES IN SCHEMA MDRP.BRONZE_EVENTS TO ROLE MDRP_LOADER;
GRANT SELECT ON ALL TABLES IN SCHEMA MDRP.BRONZE_EVENTS TO ROLE MDRP_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MDRP.BRONZE_EVENTS TO ROLE MDRP_LOADER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MDRP.BRONZE_EVENTS TO ROLE MDRP_READER;

-- =============================================================================
-- Stage grants (required for COPY INTO)
-- =============================================================================

GRANT READ, WRITE ON ALL STAGES IN SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_LOADER;
GRANT READ, WRITE ON ALL STAGES IN SCHEMA MDRP.GOLD_CURVES   TO ROLE MDRP_LOADER;
GRANT READ, WRITE ON FUTURE STAGES IN SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_LOADER;
GRANT READ, WRITE ON FUTURE STAGES IN SCHEMA MDRP.GOLD_CURVES   TO ROLE MDRP_LOADER;

GRANT READ ON ALL STAGES IN SCHEMA MDRP.SILVER_EVENTS TO ROLE MDRP_READER;
GRANT READ ON ALL STAGES IN SCHEMA MDRP.GOLD_CURVES   TO ROLE MDRP_READER;

-- =============================================================================
-- Warehouse grants
-- =============================================================================

USE ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE INGESTION_WH TO ROLE MDRP_LOADER;
GRANT USAGE ON WAREHOUSE QUERY_WH     TO ROLE MDRP_READER;
-- Loader also needs QUERY_WH for validation queries
GRANT USAGE ON WAREHOUSE QUERY_WH     TO ROLE MDRP_LOADER;

-- =============================================================================
-- Role hierarchy — grant both roles to SYSADMIN for manageability
-- =============================================================================

USE ROLE SECURITYADMIN;

GRANT ROLE MDRP_LOADER TO ROLE SYSADMIN;
GRANT ROLE MDRP_READER TO ROLE SYSADMIN;

-- =============================================================================
-- Service user — MDRP_SVC_USER
-- Used by ECS services (silver-loader, gold-loader) via Secrets Manager creds.
-- =============================================================================

-- NOTE: Replace <STRONG_PASSWORD> with the value stored in Secrets Manager
-- at mdrp/prod/snowflake-password before running this script.
CREATE USER IF NOT EXISTS MDRP_SVC_USER
  PASSWORD            = '<STRONG_PASSWORD>'
  LOGIN_NAME          = 'MDRP_SVC_USER'
  DISPLAY_NAME        = 'MDRP Service User'
  DEFAULT_WAREHOUSE   = 'INGESTION_WH'
  DEFAULT_NAMESPACE   = 'MDRP.SILVER_EVENTS'
  DEFAULT_ROLE        = 'MDRP_LOADER'
  MUST_CHANGE_PASSWORD = FALSE
  COMMENT             = 'Non-interactive service account for MDRP ECS loader services';

-- Grant the loader role to the service user
GRANT ROLE MDRP_LOADER TO USER MDRP_SVC_USER;

-- Also grant reader so the service user can query views for validation checks
GRANT ROLE MDRP_READER TO USER MDRP_SVC_USER;
