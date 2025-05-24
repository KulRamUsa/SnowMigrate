-- 02_create_warehouse.sql
-- This script creates virtual warehouses for the Snowflake-Native Migration Calculator.

-- Replace warehouse names and configurations as per your requirements.
-- Ensure the role executing this script has CREATE WAREHOUSE privileges.

USE ROLE SYSADMIN; -- Or any role with necessary permissions

USE DATABASE MIGRATION_CALCULATOR_DB; -- Ensure we are in the correct database context (optional, but good practice)

-- Warehouse for the Streamlit Application
-- Streamlit apps generally do not require large warehouses unless they are doing heavy computation themselves.
-- Start with a small size and monitor usage.
CREATE WAREHOUSE IF NOT EXISTS MIGRATION_CALC_APP_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60 -- Suspend after 1 minute of inactivity
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for running the Migration Calculator Streamlit application';

-- Warehouse for Snowpark Jobs (Stored Procedures, UDFs performing analysis)
-- The size of this warehouse might depend on the complexity of external DB queries and Python processing.
-- Start with a small size and adjust based on performance and workload.
CREATE WAREHOUSE IF NOT EXISTS MIGRATION_CALC_JOB_WH
    WAREHOUSE_SIZE = 'SMALL' -- Consider starting smaller if external calls are the main bottleneck
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Snowpark jobs (SPs, UDFs) of the Migration Calculator';

-- Grant usage of these warehouses to the application role
-- (replace APP_MIGRATION_CALCULATOR_ROLE with your role)
-- This role will be used by the Streamlit app and for deploying objects.
-- Ensure this role is created separately (e.g., in 03_create_roles_permissions.sql)
-- GRANT USAGE ON WAREHOUSE MIGRATION_CALC_APP_WH TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT USAGE ON WAREHOUSE MIGRATION_CALC_JOB_WH TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT OPERATE ON WAREHOUSE MIGRATION_CALC_JOB_WH TO ROLE APP_MIGRATION_CALCULATOR_ROLE; -- If SPs need to resume/suspend

SELECT 'Warehouses MIGRATION_CALC_APP_WH and MIGRATION_CALC_JOB_WH created/verified successfully.' AS status;
