-- 05_create_secrets.sql
-- This script creates secrets in Snowflake to securely store credentials for external source databases.

-- IMPORTANT: Replace placeholder values for usernames and passwords with actual credentials during execution OR use a secure method to inject them.
-- DO NOT COMMIT ACTUAL PASSWORDS TO VERSION CONTROL if you modify this script directly with them.
-- Ensure the role executing this has CREATE SECRET privileges on the schema (e.g., APP_SCHEMA or a dedicated secrets schema).

USE ROLE SYSADMIN; -- Or a role with CREATE SECRET privilege on the target schema

USE DATABASE MIGRATION_CALCULATOR_DB;
USE SCHEMA APP_SCHEMA; -- Or a more restricted schema dedicated to secrets if preferred

-- Example: Secret for an Oracle database connection
-- The stored procedure will reference this secret by name (e.g., 'ORACLE_DB_CRED_SECRET')
CREATE SECRET IF NOT EXISTS ORACLE_DB_CRED_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = '{"username":"YOUR_ORACLE_USERNAME", "password":"YOUR_ORACLE_PASSWORD"}' -- REPLACE THESE VALUES
    COMMENT = 'Credentials for connecting to a specific Oracle source database.';

-- Example: Secret for an SQL Server database connection
CREATE SECRET IF NOT EXISTS SQLSERVER_DB_CRED_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = '{"username":"YOUR_SQLSERVER_USERNAME", "password":"YOUR_SQLSERVER_PASSWORD"}' -- REPLACE THESE VALUES
    COMMENT = 'Credentials for connecting to a specific SQL Server source database.';

-- Example: Secret for a PostgreSQL database connection
CREATE SECRET IF NOT EXISTS POSTGRESQL_DB_CRED_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = '{"username":"YOUR_POSTGRES_USERNAME", "password":"YOUR_POSTGRES_PASSWORD"}' -- REPLACE THESE VALUES
    COMMENT = 'Credentials for connecting to a specific PostgreSQL source database.';

-- Example: Secret for a Teradata database connection
CREATE SECRET IF NOT EXISTS TERADATA_DB_CRED_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = '{"username":"YOUR_TERADATA_USERNAME", "password":"YOUR_TERADATA_PASSWORD"}' -- REPLACE THESE VALUES
    COMMENT = 'Credentials for connecting to a specific Teradata source database.';

-- Example: Secret for Databricks (e.g., Personal Access Token for SQL Endpoint)
CREATE SECRET IF NOT EXISTS DATABRICKS_PAT_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = '{"token":"YOUR_DATABRICKS_PERSONAL_ACCESS_TOKEN"}' -- REPLACE THIS VALUE
    COMMENT = 'Personal Access Token for connecting to Databricks SQL Endpoint.';

-- You might need multiple secrets if you connect to many different source instances.
-- The naming convention for secrets should be clear for the Snowpark code to reference them.

-- Grant READ access on these secrets to your application role.
-- This is essential for the Snowpark procedures/UDFs to retrieve the credentials.
-- Ensure the role APP_MIGRATION_CALCULATOR_ROLE is created first.
-- GRANT READ ON SECRET ORACLE_DB_CRED_SECRET TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT READ ON SECRET SQLSERVER_DB_CRED_SECRET TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT READ ON SECRET POSTGRESQL_DB_CRED_SECRET TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT READ ON SECRET TERADATA_DB_CRED_SECRET TO ROLE APP_MIGRATION_CALCULATOR_ROLE;
-- GRANT READ ON SECRET DATABRICKS_PAT_SECRET TO ROLE APP_MIGRATION_CALCULATOR_ROLE;

SELECT 'Secret objects created/verified. REMEMBER TO REPLACE PLACEHOLDER CREDENTIALS SECURELY and grant READ to the app role.' AS status;
