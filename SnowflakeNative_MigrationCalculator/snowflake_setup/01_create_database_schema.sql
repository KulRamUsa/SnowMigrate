-- 01_create_database_schema.sql
-- This script creates the database and schema for the Snowflake-Native Migration Calculator.

-- Replace 'MIGRATION_CALCULATOR_DB' and 'APP_SCHEMA' with your desired names if needed.
-- Ensure the role executing this script has CREATE DATABASE and CREATE SCHEMA privileges.

USE ROLE SYSADMIN; -- Or any role with necessary permissions

-- Create the database
CREATE DATABASE IF NOT EXISTS MIGRATION_CALCULATOR_DB
    COMMENT = 'Database for the Snowflake-Native Migration Effort Calculator application';

-- Use the new database
USE DATABASE MIGRATION_CALCULATOR_DB;

-- Create the schema for application objects (Streamlit app, SPs, UDFs)
CREATE SCHEMA IF NOT EXISTS APP_SCHEMA
    COMMENT = 'Schema for Streamlit app, Stored Procedures, and UDFs of the Migration Calculator';

-- Create a separate schema for stages if you plan to upload packages/drivers manually
CREATE SCHEMA IF NOT EXISTS STAGES
    COMMENT = 'Schema for internal and external stages used by the Migration Calculator';

-- Grant usage to a primary application role (replace APP_MIGRATION_CALCULATOR_ROLE with your role)
-- This role will be used by the Streamlit app and for deploying objects.
-- Ensure this role is created separately (e.g., in 03_create_roles_permissions.sql)
-- GRANT USAGE ON DATABASE MIGRATION_CALCULATOR_DB TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT USAGE ON SCHEMA MIGRATION_CALCULATOR_DB.APP_SCHEMA TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT USAGE ON SCHEMA MIGRATION_CALCULATOR_DB.STAGES TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT CREATE STAGE ON SCHEMA MIGRATION_CALCULATOR_DB.STAGES TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT CREATE FUNCTION ON SCHEMA MIGRATION_CALCULATOR_DB.APP_SCHEMA TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT CREATE PROCEDURE ON SCHEMA MIGRATION_CALCULATOR_DB.APP_SCHEMA TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT CREATE STREAMLIT ON SCHEMA MIGRATION_CALCULATOR_DB.APP_SCHEMA TO ROLE APP_MIGRATION_CALCULATOR_ROLE;


-- Display success message (optional, for interactive execution)
SELECT 'Database MIGRATION_CALCULATOR_DB and schemas APP_SCHEMA, STAGES created/verified successfully.' AS status;
