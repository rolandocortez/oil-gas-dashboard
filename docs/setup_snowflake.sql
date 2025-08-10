-- Snowflake setup for "oil-gas-dashboard"
-- This script assumes you already created:
--   - Warehouse: COMPUTE_WH
--   - Database:  OILGAS_DB
--   - Schema:    OILGAS_DB.PUBLIC (created by you)
-- It will:
--   - Create a service role (optional)
--   - Create analytics schemas (RAW, STAGING, MARTS)
--   - Grant minimal privileges to the role/user
-- Run with a sufficiently privileged role (e.g., SECURITYADMIN + SYSADMIN)
-- Adjust <YOUR_USERNAME> if you want to grant directly to a specific user.

------------------------------------------------------------
-- 0) Safety: choose a privileged role for the session
------------------------------------------------------------
-- USE ROLE SECURITYADMIN;

------------------------------------------------------------
-- 1) Warehouse (already created by you)
------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Warehouse for ETL and project queries';

------------------------------------------------------------
-- 2) Database (already created by you)
------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS OILGAS_DB
  COMMENT = 'Oil & Gas production data';

------------------------------------------------------------
-- 3) Schemas
------------------------------------------------------------
-- Keep PUBLIC as-is and add dedicated schemas for the pipeline:
CREATE SCHEMA IF NOT EXISTS OILGAS_DB.RAW     COMMENT = 'Raw ingested CSVs';
CREATE SCHEMA IF NOT EXISTS OILGAS_DB.STAGING COMMENT = 'dbt staging models';
CREATE SCHEMA IF NOT EXISTS OILGAS_DB.MARTS   COMMENT = 'dbt marts / final models';

------------------------------------------------------------
-- 4) Service role (optional but recommended)
------------------------------------------------------------
CREATE ROLE IF NOT EXISTS ROLE_OILGAS_SVC;

-- If you have a dedicated service user, grant the role to it:
-- GRANT ROLE ROLE_OILGAS_SVC TO USER OILGAS_SVC;

------------------------------------------------------------
-- 5) Grants (minimal set)
------------------------------------------------------------
-- Warehouse usage
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ROLE_OILGAS_SVC;

-- Database usage
GRANT USAGE ON DATABASE OILGAS_DB TO ROLE ROLE_OILGAS_SVC;

-- RAW: ETL needs to create/replace tables here
GRANT USAGE, CREATE TABLE, CREATE STAGE
  ON SCHEMA OILGAS_DB.RAW
  TO ROLE ROLE_OILGAS_SVC;

-- STAGING / MARTS: dbt will manage objects later
GRANT USAGE ON SCHEMA OILGAS_DB.STAGING TO ROLE ROLE_OILGAS_SVC;
GRANT USAGE ON SCHEMA OILGAS_DB.MARTS   TO ROLE ROLE_OILGAS_SVC;

-- (Optional) If you don't plan to use ROLE_OILGAS_SVC, grant to your current user/role instead:
-- GRANT USAGE ON WAREHOUSE COMPUTE_WH TO USER <YOUR_USERNAME>;
-- GRANT USAGE ON DATABASE OILGAS_DB   TO USER <YOUR_USERNAME>;
-- GRANT USAGE, CREATE TABLE, CREATE STAGE ON SCHEMA OILGAS_DB.RAW TO USER <YOUR_USERNAME>;
-- GRANT USAGE ON SCHEMA OILGAS_DB.STAGING TO USER <YOUR_USERNAME>;
-- GRANT USAGE ON SCHEMA OILGAS_DB.MARTS   TO USER <YOUR_USERNAME>;

------------------------------------------------------------
-- 6) (Optional) Set session defaults for convenience
------------------------------------------------------------
-- ALTER USER <YOUR_USERNAME>
--   SET DEFAULT_ROLE = ROLE_OILGAS_SVC,
--       DEFAULT_WAREHOUSE = COMPUTE_WH,
--       DEFAULT_NAMESPACE = OILGAS_DB.PUBLIC;  -- or RAW/STAGING as you prefer
