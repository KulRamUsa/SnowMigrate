# snowpark_logic/common/db_connector_utils.py
# Utilities for connecting to external databases from Snowpark Stored Procedures/UDFs
# using External Network Access and Snowflake Secrets.

import json
# Required for type hinting, will be available in Snowpark environment
# from snowflake.snowpark import Session # type: ignore
# from snowflake.snowpark.exceptions import SnowparkSQLException # type: ignore

# Database driver imports
import psycopg2 # For PostgreSQL
import oracledb # For Oracle
# import pyodbc # For SQL Server
import teradatasql # For Teradata
from databricks import sql as databricks_sql # For Databricks
import snowflake.connector # For Snowflake as a source
# from databricks import sql as databricks_sql


def _get_secret_credentials(session, secret_name: str) -> dict:
    """Helper function to retrieve and parse credentials from a Snowflake Secret."""
    # In a real Snowpark Stored Procedure, you would call SYSTEM$GET_SECRET.
    # The SP's owner or the role executing the SP needs USAGE on the secret.
    # The secret should store a JSON string like: {"username": "user", "password": "pass"}
    
    # --- SIMULATION FOR LOCAL TESTING / UI FLOW --- 
    if not hasattr(session, 'sql'): # If it's a mock session (typical for Streamlit direct call simulation)
        print(f"[DB_UTILS] LOCAL TEST: Received secret trigger: {secret_name}")
        if secret_name == "_LOCAL_POSTGRESQL_":
             print(f"[DB_UTILS] LOCAL TEST: Using HARDCODED credentials for PostgreSQL.")
             return {"username": "postgres", "password": "g8rYxznV_5ErXvD"} # From previous user input
        elif secret_name == "_LOCAL_ORACLE_":
             print(f"[DB_UTILS] LOCAL TEST: Placeholder for Oracle credentials.")
             # TODO: Fill with actual Oracle test credentials when configuring Oracle
             return {"username": "oracle_user", "password": "oracle_password_placeholder"}
        elif secret_name == "_LOCAL_SQLSERVER_":
             print(f"[DB_UTILS] LOCAL TEST: Placeholder for SQL Server credentials.")
             # TODO: Fill with actual SQL Server test credentials when configuring SQL Server
             return {"username": "sqlserver_user", "password": "sqlserver_password_placeholder"}
        elif secret_name == "_LOCAL_TERADATA_":
             print(f"[DB_UTILS] LOCAL TEST: Placeholder for Teradata credentials.")
             # TODO: Fill with actual Teradata test credentials when configuring Teradata
             return {"username": "teradata_user_placeholder", "password": "teradata_password_placeholder"}
        elif secret_name == "_LOCAL_DATABRICKS_":
             print(f"[DB_UTILS] LOCAL TEST: Placeholder for Databricks credentials.")
             # For Databricks, the secret should ideally contain the Personal Access Token (PAT)
             # The http_path will be taken from connection_params (new UI field)
             # TODO: Fill with actual Databricks PAT when provided by user
             return {"token": "databricks_token_placeholder"}
        elif secret_name == "_LOCAL_SNOWFLAKE_SOURCE_":
            print(f"[DB_UTILS] LOCAL TEST: Placeholder for Snowflake (source) credentials.")
            # TODO: Fill with actual Snowflake source test credentials when configuring
            return {"username": "snowflake_source_user", "password": "snowflake_source_password_placeholder", "account": "your_sf_source_account"}
        elif not secret_name:
            print(f"[DB_UTILS] LOCAL TEST: No secret name provided in simulation mode. Cannot provide mock credentials.")
            raise ValueError("Secret name (local trigger) cannot be empty for credential retrieval in simulation.")
        else:
            print(f"[DB_UTILS] LOCAL TEST: Unknown local secret trigger: '{secret_name}'. No credentials returned.")
            # Or raise an error:
            raise ValueError(f"Unknown local secret trigger: '{secret_name}'. Supported triggers: _LOCAL_POSTGRESQL_, _LOCAL_ORACLE_, etc.")

    # --- END SIMULATION ---

    # This part below would be for a real Snowpark session that has .sql()
    st.info(f"[DB_UTILS] Attempting to call SYSTEM$GET_SECRET('{secret_name}') via session.sql")
    try:
        # Actual Snowpark code:
        fully_qualified_secret_name = f"'{secret_name}'" # Ensure proper quoting if not already done
        # It is critical that secret_name itself doesn't contain quotes that break the SQL string.
        # A safer way for fully_qualified_secret_name if secret_name can be complex:
        # fq_secret_name_for_sql = secret_name.replace("'", "''") # Escape single quotes for SQL
        # query = f"SELECT SYSTEM$GET_SECRET('{fq_secret_name_for_sql}') AS secret_val;"
        # For simplicity, assuming secret_name is a simple identifier like MY_SCHEMA.MY_SECRET

        query = f"SELECT SYSTEM$GET_SECRET('{secret_name}') AS secret_val;" # Simplistic, assumes valid simple secret name
        
        # Prevent actual call if session is a very basic mock without .sql() (already handled above but as safeguard)
        if not hasattr(session, 'sql') or not callable(session.sql):
            print("[DB_UTILS] Mock session does not support .sql(). This indicates an issue in simulation setup.")
            raise NotImplementedError("session.sql is not implemented in this mock session context for _get_secret_credentials")

        secret_val_row = session.sql(query).collect()
        if not secret_val_row or secret_val_row[0]["SECRET_VAL"] is None:
            print(f"[DB_UTILS] Secret '{secret_name}' is empty or not found via SYSTEM$GET_SECRET.")
            raise ValueError(f"Secret '{secret_name}' is empty or not found.")
        
        secret_json_str = secret_val_row[0]["SECRET_VAL"]
        print(f"[DB_UTILS] Successfully retrieved secret '{secret_name}'. Length: {len(secret_json_str)}")
        return json.loads(secret_json_str)
        
    except Exception as e:
        print(f"[DB_UTILS] Error retrieving or parsing secret '{secret_name}' via SYSTEM$GET_SECRET: {str(e)}")
        # Consider specific exception types e.g. SnowparkSQLException
        raise ValueError(f"Error retrieving/parsing secret '{secret_name}' via SYSTEM$GET_SECRET: {str(e)}")


def get_source_db_connection(session, db_type: str, connection_params: dict):
    """
    Establishes a connection to the source database using appropriate drivers and credentials.
    This function will utilize Snowpark's External Network Access.
    Args:
        session: The Snowpark session object (implicitly available in SPs).
        db_type: The type of the source database (e.g., 'oracle', 'sqlserver', 'postgresql').
        connection_params: Dictionary containing host, port, database, secret_name, etc.
    Returns:
        A database connection object or None if connection fails.
    """
    print(f"[DB_UTILS_GET_CONN] Received db_type: '{db_type}'") # DEBUG
    credentials = {}
    try:
        credentials = _get_secret_credentials(session, connection_params['secret_name'])
    except ValueError as e:
        print(f"[DB_UTILS] Failed to get credentials: {e}")
        return None # Could raise or return None depending on desired error handling in SP

    user = credentials.get('username')
    password = credentials.get('password')
    host = connection_params.get('host')
    port = connection_params.get('port')
    database_name = connection_params.get('database')

    if not all([user, password, host, port, database_name]):
        print("[DB_UTILS] Missing one or more connection parameters (user, host, port, database_name) after getting secrets.")
        return None

    conn = None
    # IMPORTANT: The actual connection establishment needs to happen within 
    # a Snowpark Python Stored Procedure or UDF that has an External Access Integration specified.

    if db_type == "postgresql":
        try:
            print(f"[DB_UTILS] Attempting to connect to PostgreSQL at {host}:{port}, DB: {database_name} with user {user}")
            # In a real SP, this call requires External Network Access to be set up for the SP.
            conn = psycopg2.connect(
                host=host, 
                port=int(port), 
                dbname=database_name, 
                user=user, 
                password=password,
                connect_timeout=10 # Example timeout
            )
            print("[DB_UTILS] Successfully connected to PostgreSQL (conceptual).")
        except psycopg2.Error as e:
            print(f"[DB_UTILS] psycopg2 connection error: {e}")
            # Consider how to surface this error to the Streamlit app (e.g., via SP return value)
            return None
        except Exception as e:
            print(f"[DB_UTILS] Generic error during PostgreSQL connection: {e}")
            return None
    elif db_type == "oracle":
        try:
            ora_host = host
            ora_port = str(port) if port else "1521" # Default Oracle port
            # 'database' from UI maps to Oracle's Service Name or SID
            ora_service_name = database_name 
            ora_user = user
            ora_password = password

            if not ora_service_name:
                print("[DB_UTILS] Oracle Service Name/SID not provided.")
                return None

            # Construct DSN for oracledb. Thin mode should be default if no Oracle Client is found.
            # dsn = f"{ora_host}:{ora_port}/{ora_service_name}"
            # More robust DSN creation using oracledb.makedsn()
            dsn = oracledb.makedsn(ora_host, int(ora_port), service_name=ora_service_name)
            # For SID, use: dsn = oracledb.makedsn(ora_host, int(ora_port), sid=ora_service_name)
            # We are assuming service_name for now based on typical usage.

            print(f"[DB_UTILS] Attempting to connect to Oracle at {dsn} with user {ora_user}")
            
            # Attempt to initialize in thin mode if client not available/configured
            # This is generally handled by the driver itself if no client is found.
            # oracledb.init_oracle_client() # Only if thick mode is explicitly needed and client path is set

            conn = oracledb.connect(user=ora_user, password=ora_password, dsn=dsn)
            print("[DB_UTILS] Successfully connected to Oracle (conceptual).")
        except oracledb.Error as e:
            print(f"[DB_UTILS] oracledb connection error: {e}")
            return None
        except Exception as e:
            print(f"[DB_UTILS] Generic error during Oracle connection: {e}")
            return None
    elif db_type == "teradata":
        try:
            # Teradata port is often 1025 and specified as dbs_port
            # The 'database' parameter in UI maps to teradatasql's 'database' param.
            # If 'database' is not provided, it might connect to the default user database.
            td_host = host
            td_user = user
            td_password = password
            td_port = str(port) if port else "1025" # Default Teradata port
            td_database = connection_params.get('database') # Optional for Teradata, uses default if None

            print(f"[DB_UTILS] Attempting to connect to Teradata at {td_host}:{td_port}, DB: {td_database or 'default user DB'} with user {td_user}")
            
            conn_params_td = {
                'host': td_host,
                'user': td_user,
                'password': td_password,
                'dbs_port': td_port,
                'logmech': 'TD2' # Common authentication mechanism, can be others like LDAP, JWT
            }
            if td_database:
                conn_params_td['database'] = td_database

            conn = teradatasql.connect(**conn_params_td)
            print("[DB_UTILS] Successfully connected to Teradata (conceptual).")
        except teradatasql.Error as e:
            print(f"[DB_UTILS] teradatasql connection error: {e}")
            return None
        except Exception as e:
            print(f"[DB_UTILS] Generic error during Teradata connection: {e}")
            return None
    elif db_type == "databricks":
        try:
            # Databricks connection: server_hostname, http_path, access_token
            dbx_host = host # UI "Host" maps to server_hostname
            dbx_http_path = connection_params.get('http_path') # From new UI field
            
            if not dbx_http_path:
                print("[DB_UTILS] Databricks HTTP Path not provided in connection_params.")
                return None

            # Token is expected from the secret
            dbx_token = credentials.get('token')
            if not dbx_token:
                print("[DB_UTILS] Databricks access token not found in credentials.")
                return None

            print(f"[DB_UTILS] Attempting to connect to Databricks at {dbx_host} with HTTP Path {dbx_http_path}")
            
            conn = databricks_sql.connect(
                server_hostname=dbx_host,
                http_path=dbx_http_path,
                access_token=dbx_token,
                # _user_agent_entry="SnowflakeNativeMigrationCalculator" # Optional: for identifying the client
            )
            print("[DB_UTILS] Successfully connected to Databricks (conceptual).")
        except databricks_sql.exc.DatabricksSqlException as e:
            print(f"[DB_UTILS] Databricks SQL connection error: {e}")
            return None
        except Exception as e:
            print(f"[DB_UTILS] Generic error during Databricks connection: {e}")
            return None
    elif db_type == "snowflake": # Connecting to another Snowflake instance as a source
        try:
            sf_user = user
            sf_password = password
            sf_account = connection_params.get('account') # From UI specific field, passed in connection_params
            sf_database = database_name # UI "Database Name"
            sf_schema = connection_params.get('schema') # UI "Schema Name (Optional)"
            # Optional: Add warehouse and role to connection_params if needed from UI or config
            sf_warehouse = connection_params.get('warehouse') 
            sf_role = connection_params.get('role')

            if not sf_account:
                print("[DB_UTILS] Snowflake account identifier not provided.")
                return None
            
            print(f"[DB_UTILS] Attempting to connect to Snowflake source: account={sf_account}, user={sf_user}, db={sf_database}, schema={sf_schema}")
            conn = snowflake.connector.connect(
                user=sf_user,
                password=sf_password,
                account=sf_account,
                database=sf_database, # Optional at connection time, can be set later
                schema=sf_schema,     # Optional at connection time
                warehouse=sf_warehouse, # Optional
                role=sf_role          # Optional
            )
            print("[DB_UTILS] Successfully connected to Snowflake source (conceptual).")
        except snowflake.connector.errors.DatabaseError as e:
            print(f"[DB_UTILS] Snowflake connector error: {e}")
            return None
        except Exception as e:
            print(f"[DB_UTILS] Generic error during Snowflake source connection: {e}")
            return None
    else:
        print(f"[DB_UTILS] Unsupported database type for connection: {db_type}")
        raise ValueError(f"Unsupported database type: {db_type}")

    return conn

def fetch_object_counts(connection, db_type: str, db_name: str, schema_name: str = None) -> dict:
    """
    Fetches object counts (tables, views, procedures, functions) from the source database.
    Args:
        connection: Active database connection object from get_source_db_connection.
        db_type: The type of the source database.
        db_name: The name of the database to analyze (often implicit in connection for PG).
        schema_name: Optional specific schema to analyze. If None, analyze all accessible schemas in search_path or current_user.
    Returns:
        A dictionary containing object counts like: 
        {"total_objects": {"tables": N, "views": M, ...}, "schemas_summary": [{name: S, tables: T...}]}
    """
    print(f"[DB_UTILS_FETCH_COUNTS] Received db_type: '{db_type}'") # DEBUG
    if connection is None:
        print("[DB_UTILS] Mocking object counts as connection object is None.")
        # Return structure consistent with successful fetch
        return {"total_objects": {"tables": 10, "views": 5, "procedures": 2, "functions": 3}, "schemas_summary": []} 

    counts = {"tables": 0, "views": 0, "procedures": 0, "functions": 0}
    schemas_summary_list = [] # List of dicts for each schema summary
    cursor = None

    try:
        cursor = connection.cursor()
        target_database_teradata = schema_name if schema_name else db_name
        target_schema_databricks = schema_name if schema_name else db_name
        
        # For SQL Server, db_name is the database. schema_name is the schema within that database.
        # If schema_name is not provided, we might want to query all schemas in the db_name or default to 'dbo'.
        # For simplicity, if schema_name is None, we will query for objects in all schemas within the connected database.
        # If schema_name is provided, we will filter by it.
        target_schema_sqlserver = schema_name # Can be None

        # For Oracle, schema_name is the Oracle schema (often the username if not specified differently).
        # db_name from UI is the Service Name/SID, not directly used for object filtering here unless it IS the schema name.
        # We will prioritize schema_name from UI if provided. If not, some queries might need a default (e.g., current user) or fail if ambiguous.
        # The USERNAME for connection is often the default schema.
        target_schema_oracle = schema_name.upper() if schema_name else None # Oracle schema names are typically uppercase

        # For Snowflake source, db_name is the database (catalog), schema_name is the schema.
        target_catalog_snowflake = db_name # From UI "Database Name"
        target_schema_snowflake = schema_name.upper() if schema_name else None # Schemas often UC, but IS views are case-insensitive for identifiers

        print(f"[DB_UTILS] Fetching object counts for {db_type} - Target DB/Catalog: {target_catalog_snowflake if db_type == 'snowflake' else (target_database_teradata if db_type == 'teradata' else target_schema_databricks)}, Target Schema: {target_schema_snowflake if db_type == 'snowflake' else (target_schema_oracle or 'CURRENT_USER (or unspecified)' if db_type == 'oracle' else (target_schema_sqlserver or 'all' if db_type == 'sqlserver' else None))}")

        if db_type == "postgresql":
            # Query to get all accessible schemas if no specific schema_name is provided, or focus on one
            # This is a simplified approach. A real-world scenario might want to list schemas and then iterate.
            target_schemas_query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg_%' AND schema_name != 'information_schema';"
            if schema_name:
                # Use a parameterized query to be safe, though schema_name here is from app logic
                target_schemas_query = f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name.lower()}';"
            
            cursor.execute(target_schemas_query)
            schemas_to_query = [row[0] for row in cursor.fetchall()]

            if not schemas_to_query and schema_name:
                 print(f"[DB_UTILS] Specified schema '{schema_name}' not found or not accessible.")
                 # Fallback to empty results for this schema to avoid errors down the line
                 return {"total_objects": counts, "schemas_summary": schemas_summary_list}
            elif not schemas_to_query:
                 print(f"[DB_UTILS] No user schemas found to analyze.")
                 return {"total_objects": counts, "schemas_summary": schemas_summary_list}

            print(f"[DB_UTILS] Will query schemas: {schemas_to_query}")

            for current_schema_name in schemas_to_query:
                schema_counts = {"tables": 0, "views": 0, "procedures": 0, "functions": 0}

                # Tables
                cursor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{current_schema_name}' AND table_type = 'BASE TABLE';")
                schema_counts["tables"] = cursor.fetchone()[0]
                
                # Views
                cursor.execute(f"SELECT COUNT(*) FROM information_schema.views WHERE table_schema = '{current_schema_name}';")
                schema_counts["views"] = cursor.fetchone()[0]
                
                # Functions (includes procedures in PostgreSQL >= 11)
                # For PG, routines include both functions and procedures.
                # We can try to distinguish if needed, but for a general count, this is often sufficient.
                # `pg_proc` is more detailed but `information_schema.routines` is standard.
                cursor.execute(f"SELECT COUNT(*) FROM information_schema.routines WHERE specific_schema = '{current_schema_name}' AND routine_type = 'FUNCTION';")
                schema_counts["functions"] = cursor.fetchone()[0]
                
                cursor.execute(f"SELECT COUNT(*) FROM information_schema.routines WHERE specific_schema = '{current_schema_name}' AND routine_type = 'PROCEDURE';")
                schema_counts["procedures"] = cursor.fetchone()[0]

                schemas_summary_list.append({
                    "name": current_schema_name,
                    **schema_counts
                })
                # Aggregate totals if we are processing multiple schemas because schema_name was not specified
                if not schema_name or len(schemas_to_query) > 1:
                    counts["tables"] += schema_counts["tables"]
                    counts["views"] += schema_counts["views"]
                    counts["procedures"] += schema_counts["procedures"]
                    counts["functions"] += schema_counts["functions"]
                else: # if specific schema_name was given and found, counts are just that schema's counts
                    counts = schema_counts

        elif db_type == "teradata":
            if not target_database_teradata:
                print("[DB_UTILS] Teradata: Database/Schema name not provided. Cannot fetch counts.")
                return {"total_objects": counts, "schemas_summary": schemas_summary_list, "error": "Database/Schema name is required for Teradata."}

            # For Teradata, queries are against DBC views.
            # TableKind: T=Table, V=View, M=Macro (often used like SPs), P=Stored Procedure, F=Function (UDFs), R=Table Function
            
            # Tables
            query_tables = f"SELECT COUNT(*) FROM DBC.TablesV WHERE DatabaseName = '{target_database_teradata}' AND TableKind = 'T';"
            cursor.execute(query_tables)
            counts["tables"] = cursor.fetchone()[0]
            
            # Views
            query_views = f"SELECT COUNT(*) FROM DBC.TablesV WHERE DatabaseName = '{target_database_teradata}' AND TableKind = 'V';"
            cursor.execute(query_views)
            counts["views"] = cursor.fetchone()[0]
            
            # Procedures (Stored Procedures 'P' and Macros 'M' which are often used like procedures)
            # Note: DBC.RoutinesV or DBC.SPObjectCodeT might be alternatives for newer TD versions / more detail
            query_procedures = f"SELECT COUNT(*) FROM DBC.TablesV WHERE DatabaseName = '{target_database_teradata}' AND TableKind IN ('P', 'M');"
            cursor.execute(query_procedures)
            counts["procedures"] = cursor.fetchone()[0]
            
            # Functions (User Defined Functions 'F', 'A', 'B', 'E', 'G', 'N', 'R', 'S', 'U', '1')
            # TableKind for UDFs can be varied: F (Scalar UDF), A (Aggregate UDF), R (Table Function / PTF), etc.
            # We'll do a broader catch here. More specific types exist (DBC.FunctionsV, DBC.UDTInfoV).
            # For simplicity, we'll query TableKind for common function-like types.
            # Common UDFs often are 'F' (Scalar UDF) or 'A' (Aggregate UDF). 'R' is Table Function.
            # This might need refinement based on the types of "functions" to be migrated.
            query_functions = f"SELECT COUNT(*) FROM DBC.TablesV WHERE DatabaseName = '{target_database_teradata}' AND TableKind IN ('F', 'A', 'R', 'S', 'U', 'E', 'G', 'N', '1');"
            cursor.execute(query_functions)
            counts["functions"] = cursor.fetchone()[0]

            # Teradata doesn't have a separate "schema" concept within a "database" like PostgreSQL.
            # A "Database" in Teradata is akin to a schema. So, schemas_summary will reflect the single queried database.
            schemas_summary_list.append({
                "name": target_database_teradata,
                **counts # The counts for this single "database/schema"
            })
            # Total objects are just the counts from this single database.

        elif db_type == "databricks":
            if not target_schema_databricks:
                print("[DB_UTILS] Databricks: Schema (Database) name not provided. Cannot fetch counts.")
                return {"total_objects": counts, "schemas_summary": schemas_summary_list, "error": "Schema (Database) name is required for Databricks."}

            # Databricks uses information_schema.
            # Catalog context: USE CATALOG <catalog_name>; then USE SCHEMA <schema_name>;
            # We assume the connection is to the correct catalog, or UI needs a catalog field.
            # For simplicity, db_name from UI is the schema.
            
            # Tables
            # For Databricks, tables are in information_schema.tables
            # table_type can be 'BASE TABLE' or 'TABLE' for managed/external, 'VIEW' for views
            query_tables = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{target_schema_databricks}' AND table_type = 'TABLE';" # Or 'BASE TABLE'
            cursor.execute(query_tables)
            counts["tables"] = cursor.fetchone()[0]
            
            # Views
            query_views = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{target_schema_databricks}' AND table_type = 'VIEW';"
            cursor.execute(query_views)
            counts["views"] = cursor.fetchone()[0]
            
            # Functions (User-Defined Functions)
            # information_schema.routines includes functions and procedures.
            # routine_type can be 'FUNCTION' or 'PROCEDURE'.
            query_functions = f"SELECT COUNT(*) FROM information_schema.routines WHERE specific_schema = '{target_schema_databricks}' AND routine_type = 'FUNCTION';"
            cursor.execute(query_functions)
            counts["functions"] = cursor.fetchone()[0]
            
            # Procedures
            query_procedures = f"SELECT COUNT(*) FROM information_schema.routines WHERE specific_schema = '{target_schema_databricks}' AND routine_type = 'PROCEDURE';"
            cursor.execute(query_procedures)
            counts["procedures"] = cursor.fetchone()[0]

            schemas_summary_list.append({
                "name": target_schema_databricks,
                **counts
            })
            # Total objects are just the counts from this single schema.

        elif db_type == "oracle":
            # target_schema_oracle is already initialized from schema_name.upper() or None
            # If schema_name was not provided via UI (so target_schema_oracle is None), try to derive it.
            if not target_schema_oracle:
                 # If no schema provided, try to get current schema from connection
                try:
                    cursor.execute("SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM DUAL")
                    current_user_schema = cursor.fetchone()[0]
                    if current_user_schema:
                        target_schema_oracle = current_user_schema.upper() # Update and ensure uppercase
                        print(f"[DB_UTILS] Oracle: No schema provided by UI, using current user schema: {target_schema_oracle}")
                    else:
                        print("[DB_UTILS] Oracle: Schema not specified by UI and could not determine current user schema. Queries might be broad or fail.")
                except oracledb.Error as e:
                    print(f"[DB_UTILS] Oracle: Error getting current schema: {e}. Queries might be broad or fail.")
            
            if not target_schema_oracle:
                print("[DB_UTILS] Oracle: Schema name is required (and could not be derived/provided). Skipping Oracle counts.")
                return {"total_objects": counts, "schemas_summary": schemas_summary_list, "error": "Schema name required for Oracle object counting and could not be derived."}

            # Queries against ALL_ views for broader visibility, filtered by OWNER.
            # Tables
            query_tables = f"SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = '{target_schema_oracle}'"
            cursor.execute(query_tables)
            counts["tables"] = cursor.fetchone()[0]
            
            # Views
            query_views = f"SELECT COUNT(*) FROM ALL_VIEWS WHERE OWNER = '{target_schema_oracle}'"
            cursor.execute(query_views)
            counts["views"] = cursor.fetchone()[0]
            
            # Procedures (Stored Procedures)
            # ALL_PROCEDURES view includes procedures, functions, and package components.
            # We filter by OBJECT_TYPE = 'PROCEDURE'
            query_procedures = f"SELECT COUNT(*) FROM ALL_PROCEDURES WHERE OWNER = '{target_schema_oracle}' AND OBJECT_TYPE = 'PROCEDURE'"
            cursor.execute(query_procedures)
            counts["procedures"] = cursor.fetchone()[0]
            
            # Functions
            query_functions = f"SELECT COUNT(*) FROM ALL_PROCEDURES WHERE OWNER = '{target_schema_oracle}' AND OBJECT_TYPE = 'FUNCTION'"
            cursor.execute(query_functions)
            counts["functions"] = cursor.fetchone()[0]
            
            # Note: Oracle also has Packages, Types, Sequences, etc. which are not counted here for simplicity.
            # Could also use ALL_OBJECTS with object_type filtering for a more comprehensive list if needed.
            # e.g., COUNT(*) FROM ALL_OBJECTS WHERE OWNER = 'SCHEMA_NAME' AND OBJECT_TYPE = 'PACKAGE'

            schemas_summary_list.append({
                "name": target_schema_oracle,
                **counts
            })

        elif db_type == "snowflake":
            if not target_catalog_snowflake:
                print("[DB_UTILS] Snowflake: Database (Catalog) name not provided. Cannot fetch counts.")
                return {"total_objects": counts, "schemas_summary": schemas_summary_list, "error": "Database (Catalog) name required for Snowflake."}
            
            schema_filter = f"AND TABLE_SCHEMA = '{target_schema_snowflake}'" if target_schema_snowflake else ""
            # If no schema, count across all schemas in the database. INFORMATION_SCHEMA views are per-DB.
            
            # Tables
            query_tables = f"SELECT COUNT(*) FROM {target_catalog_snowflake}.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' {schema_filter};"
            cursor.execute(query_tables)
            counts["tables"] = cursor.fetchone()[0]
            
            # Views
            query_views = f"SELECT COUNT(*) FROM {target_catalog_snowflake}.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'VIEW' {schema_filter};"
            cursor.execute(query_views)
            counts["views"] = cursor.fetchone()[0]
            
            # Procedures
            # Snowflake INFORMATION_SCHEMA.PROCEDURES uses PROCEDURE_SCHEMA
            proc_schema_filter = f"AND PROCEDURE_SCHEMA = '{target_schema_snowflake}'" if target_schema_snowflake else ""
            query_procedures = f"SELECT COUNT(*) FROM {target_catalog_snowflake}.INFORMATION_SCHEMA.PROCEDURES WHERE 1=1 {proc_schema_filter};" # 1=1 to allow easy AND append
            cursor.execute(query_procedures)
            counts["procedures"] = cursor.fetchone()[0]
            
            # Functions (User-Defined Functions)
            # Snowflake INFORMATION_SCHEMA.FUNCTIONS uses FUNCTION_SCHEMA
            func_schema_filter = f"AND FUNCTION_SCHEMA = '{target_schema_snowflake}'" if target_schema_snowflake else ""
            query_functions = f"SELECT COUNT(*) FROM {target_catalog_snowflake}.INFORMATION_SCHEMA.FUNCTIONS WHERE 1=1 {func_schema_filter};" # Includes UDTFs
            cursor.execute(query_functions)
            counts["functions"] = cursor.fetchone()[0]

            # For Snowflake, summary is for the specified database and schema (or all schemas in DB)
            summary_name = f"{target_catalog_snowflake}{('.' + target_schema_snowflake) if target_schema_snowflake else ' (all schemas)'}"
            schemas_summary_list.append({
                "name": summary_name,
                **counts
            })

        else:
            print(f"[DB_UTILS] Unsupported database type for object counting: {db_type}")
            return {"total_objects": counts, "schemas_summary": schemas_summary_list, "error": f"Object counting not implemented for {db_type}."}

        print(f"[DB_UTILS] Fetched counts: {counts}, Schemas Summary: {schemas_summary_list}")
        return {"total_objects": counts, "schemas_summary": schemas_summary_list}

    except psycopg2.Error as e:
        print(f"[DB_UTILS] psycopg2 error during count: {e}")
        # Return last known counts or empty structure to prevent total failure
        return {"total_objects": counts, "schemas_summary": schemas_summary_list, "error": str(e)}
    except Exception as e:
        print(f"[DB_UTILS] Generic error during object count: {e}")
        return {"total_objects": counts, "schemas_summary": schemas_summary_list, "error": str(e)}
    finally:
        if cursor:
            cursor.close()

# Note: Closing the main connection (conn) should be handled by the calling Stored Procedure
# in analyze_database_sp.py after all database operations are complete.

# Placeholder for streamlit if used directly for info messages, not for Snowpark SPs
class StInfo:
    def info(self, msg):
        print(msg)
st = StInfo() # Simple mock for st.info if not in Streamlit context
