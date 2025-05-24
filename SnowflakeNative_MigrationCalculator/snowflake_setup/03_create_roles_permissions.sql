-- 03_create_roles_permissions.sql
-- This script creates a dedicated role for the Migration Calculator application and grants initial permissions.

-- Replace 'APP_MIGRATION_CALCULATOR_ROLE' with your desired role name.
-- It is recommended to grant this role to users or service accounts that will manage/deploy/run the application.

USE ROLE SECURITYADMIN; -- Or ACCOUNTADMIN if SECURITYADMIN cannot create roles or grant to SYSADMIN for ownership transfer

-- Create the application role
CREATE ROLE IF NOT EXISTS APP_MIGRATION_CALCULATOR_ROLE
    COMMENT = 'Role for managing and running the Snowflake-Native Migration Calculator application';

-- Grant the role to SYSADMIN to allow SYSADMIN to manage it or assign users
-- Alternatively, grant to specific user roles that will manage the app
GRANT ROLE APP_MIGRATION_CALCULATOR_ROLE TO ROLE SYSADMIN;


-- Switch to SYSADMIN (or a role that owns the database/schema/warehouses) to grant privileges to the new role
USE ROLE SYSADMIN;

-- Grant privileges on the database and schemas (created in 01_create_database_schema.sql)
GRANT USAGE ON DATABASE MIGRATION_CALCULATOR_DB TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
GRANT USAGE ON SCHEMA MIGRATION_CALCULATOR_DB.APP_SCHEMA TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
GRANT USAGE ON SCHEMA MIGRATION_CALCULATOR_DB.STAGES TO ROLE APP_MIGRATION_CALCULATOR_ROLE;

-- Grant privileges to create application objects in the APP_SCHEMA
GRANT CREATE FUNCTION ON SCHEMA MIGRATION_CALCULATOR_DB.APP_SCHEMA TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
GRANT CREATE PROCEDURE ON SCHEMA MIGRATION_CALCULATOR_DB.APP_SCHEMA TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
GRANT CREATE STREAMLIT ON SCHEMA MIGRATION_CALCULATOR_DB.APP_SCHEMA TO ROLE APP_MIGRATION_CALCULATOR_ROLE;

-- Grant privileges to create stages (if needed for uploading packages)
GRANT CREATE STAGE ON SCHEMA MIGRATION_CALCULATOR_DB.STAGES TO ROLE APP_MIGRATION_CALCULATOR_ROLE;

-- Grant privileges on the warehouses (created in 02_create_warehouse.sql)
GRANT USAGE ON WAREHOUSE MIGRATION_CALC_APP_WH TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
GRANT USAGE ON WAREHOUSE MIGRATION_CALC_JOB_WH TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
GRANT OPERATE ON WAREHOUSE MIGRATION_CALC_JOB_WH TO ROLE APP_MIGRATION_CALCULATOR_ROLE; -- If SPs need to control the warehouse state
GRANT OPERATE ON WAREHOUSE MIGRATION_CALC_APP_WH TO ROLE APP_MIGRATION_CALCULATOR_ROLE; -- If Streamlit app needs to control its WH state

-- Grant privileges for External Network Access (Network Rules and Secrets will be handled in separate scripts)
-- The role will need USAGE on any specific Network Rules and Secrets used by the application.
-- Example (assuming Network Rule and Secret are created by SYSADMIN or similar): 
-- GRANT USAGE ON INTEGRATION MY_EXTERNAL_ACCESS_INTEGRATION TO ROLE APP_MIGRATION_CALCULATOR_ROLE; (if using External Access Integration directly)
-- GRANT USAGE ON NETWORK RULE MY_ALLOW_LIST_NETWORK_RULE TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT READ ON SECRET MY_EXTERNAL_DB_CREDENTIAL_SECRET TO ROLE APP_MIGRATION_CALCULATOR_ROLE;


-- Grant privilege to create secrets IF this role is also responsible for managing them
-- This might be too broad; typically secrets are managed by a more privileged role.
-- Consider if a separate, more privileged role should own secrets.
-- GRANT CREATE SECRET ON SCHEMA MIGRATION_CALCULATOR_DB.APP_SCHEMA TO ROLE APP_MIGRATION_CALCULATOR_ROLE; 

SELECT 'Role APP_MIGRATION_CALCULATOR_ROLE created/verified and initial grants applied.' AS status;
