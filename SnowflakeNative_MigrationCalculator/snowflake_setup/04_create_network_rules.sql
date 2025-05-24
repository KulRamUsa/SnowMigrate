-- 04_create_network_rules.sql
-- This script creates network rules to allow outbound traffic from Snowpark Stored Procedures/UDFs
-- to external database hosts and ports.

-- IMPORTANT: Replace placeholder values for hosts with actual IPs or domain names of your source databases.
-- Be as specific as possible to enhance security.
-- Ensure the role executing this has CREATE NETWORK RULE privileges (typically ACCOUNTADMIN or a custom role with this grant).

USE ROLE ACCOUNTADMIN; -- Or a role with CREATE NETWORK RULE privilege

USE DATABASE MIGRATION_CALCULATOR_DB; -- Optional: ensure context
USE SCHEMA APP_SCHEMA; -- Optional: ensure context, though network rules are account-level objects

-- Example Network Rule for Oracle (port 1521)
-- You might create one rule per database type or a more general one if hosts are known.
CREATE NETWORK RULE IF NOT EXISTS ALLOW_ORACLE_ACCESS
    TYPE = HOST_PORT
    MODE = EGRESS -- Outbound traffic
    VALUE_LIST = ('your_oracle_db_host1.example.com:1521', 'your_oracle_db_host2_ip:1521') -- REPLACE THESE
    COMMENT = 'Allow outbound access to Oracle database hosts on port 1521';

-- Example Network Rule for SQL Server (port 1433)
CREATE NETWORK RULE IF NOT EXISTS ALLOW_SQLSERVER_ACCESS
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = ('your_sqlserver_host.example.com:1433') -- REPLACE THESE
    COMMENT = 'Allow outbound access to SQL Server hosts on port 1433';

-- Example Network Rule for PostgreSQL (port 5432)
CREATE NETWORK RULE IF NOT EXISTS ALLOW_POSTGRESQL_ACCESS
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = ('your_postgres_host.example.com:5432') -- REPLACE THESE
    COMMENT = 'Allow outbound access to PostgreSQL hosts on port 5432';

-- Example Network Rule for Teradata (port 1025)
CREATE NETWORK RULE IF NOT EXISTS ALLOW_TERADATA_ACCESS
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = ('your_teradata_host.example.com:1025') -- REPLACE THESE
    COMMENT = 'Allow outbound access to Teradata hosts on port 1025';

-- Example Network Rule for Databricks SQL Endpoints (port 443)
-- Databricks workspace URLs are usually FQDNs.
CREATE NETWORK RULE IF NOT EXISTS ALLOW_DATABRICKS_ACCESS
    TYPE = HOST_PORT
    MODE = EGRESS
    VALUE_LIST = ('your_databricks_workspace.cloud.databricks.com:443') -- REPLACE THESE
    COMMENT = 'Allow outbound access to Databricks SQL endpoints on port 443';

-- It's often better to create an External Access Integration that uses these network rules.
-- CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS MIGRATION_CALC_EXT_ACCESS_INT
--     ALLOWED_NETWORK_RULES = (ALLOW_ORACLE_ACCESS, ALLOW_SQLSERVER_ACCESS, ALLOW_POSTGRESQL_ACCESS, ALLOW_TERADATA_ACCESS, ALLOW_DATABRICKS_ACCESS) -- Add all relevant rules
--     ENABLED = TRUE
--     COMMENT = 'External Access Integration for the Migration Calculator to connect to source databases';

-- Grant USAGE on these network rules (and the integration if created) to your application role.
-- This is critical for Snowpark procedures/UDFs to use them.
-- Ensure the role APP_MIGRATION_CALCULATOR_ROLE is created first.
-- USE ROLE SYSADMIN; -- Or the owner of the APP_MIGRATION_CALCULATOR_ROLE
-- GRANT USAGE ON NETWORK RULE ALLOW_ORACLE_ACCESS TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT USAGE ON NETWORK RULE ALLOW_SQLSERVER_ACCESS TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT USAGE ON NETWORK RULE ALLOW_POSTGRESQL_ACCESS TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT USAGE ON NETWORK RULE ALLOW_TERADATA_ACCESS TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT USAGE ON NETWORK RULE ALLOW_DATABRICKS_ACCESS TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT USAGE ON INTEGRATION MIGRATION_CALC_EXT_ACCESS_INT TO ROLE APP_MIGRATION_CALCULATOR_ROLE; -- If integration is used

SELECT 'Network rules created/verified. Remember to replace placeholder host values and grant USAGE to the app role.' AS status;
