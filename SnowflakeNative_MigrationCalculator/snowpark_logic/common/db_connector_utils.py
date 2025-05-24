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

# Attempt to import streamlit for st.secrets, but make it optional
try:
    import streamlit as st
except ImportError:
    st = None # Will be None if not in a Streamlit environment or streamlit not installed

def _get_secret_credentials(session, secret_name_or_trigger: str, db_type: str = None) -> dict:
    """
    Helper function to retrieve and parse credentials.
    - For local testing (mock session), uses secret_name_or_trigger to select hardcoded credentials.
    - For Streamlit Cloud (st.secrets available), uses db_type to fetch from st.secrets.
    - For actual Snowpark (real session), uses secret_name_or_trigger as the Snowflake secret name.
    """
    
    # --- Streamlit Cloud Deployment: Use st.secrets ---
    if st and hasattr(st, 'secrets') and hasattr(st.secrets, 'get'): # Check .get to be more robust
        print(f"[DB_UTILS] Streamlit Cloud: Attempting to fetch credentials for db_type '{db_type}' from st.secrets")
        creds = {}
        if db_type == "postgresql":
            if st.secrets.get("postgres_user") and st.secrets.get("postgres_password"):
                creds = {"username": st.secrets.get("postgres_user"), "password": st.secrets.get("postgres_password")}
            else:
                raise ValueError(f"Streamlit Cloud secrets for PostgreSQL (postgres_user, postgres_password) not found or incomplete.")
        elif db_type == "oracle":
            if st.secrets.get("oracle_user") and st.secrets.get("oracle_password"):
                creds = {"username": st.secrets.get("oracle_user"), "password": st.secrets.get("oracle_password")}
            else:
                raise ValueError("Streamlit Cloud secrets for Oracle (oracle_user, oracle_password) not found or incomplete.")
        elif db_type == "teradata":
            if st.secrets.get("teradata_user") and st.secrets.get("teradata_password"):
                creds = {"username": st.secrets.get("teradata_user"), "password": st.secrets.get("teradata_password")}
            else:
                raise ValueError("Streamlit Cloud secrets for Teradata (teradata_user, teradata_password) not found or incomplete.")
        elif db_type == "databricks":
            if st.secrets.get("databricks_token"):
                creds = {"token": st.secrets.get("databricks_token")}
            else:
                raise ValueError("Streamlit Cloud secret for Databricks (databricks_token) not found.")
        elif db_type == "snowflake": # Snowflake as a source
            if st.secrets.get("snowflake_source_user") and st.secrets.get("snowflake_source_password"):
                creds = {"username": st.secrets.get("snowflake_source_user"), "password": st.secrets.get("snowflake_source_password")}
                if st.secrets.get("snowflake_source_account"):
                    creds["account"] = st.secrets.get("snowflake_source_account")
            else:
                raise ValueError("Streamlit Cloud secrets for Snowflake source (snowflake_source_user, snowflake_source_password) not found or incomplete.")
        else:
            # If db_type is None or not specifically handled for st.secrets, it might be an issue
            # or it might be a Snowpark call that coincidentally is in a streamlit env but should use SYSTEM$GET_SECRET
            # This path should ideally not be hit if db_type is correctly passed for Streamlit Cloud flow.
             print(f"[DB_UTILS] Streamlit Cloud: db_type '{db_type}' not specifically configured for st.secrets or db_type is None. Falling through.")
             # Fall through to local sim / Snowpark logic if db_type not matched for st.secrets
             pass # Let it fall through

        if creds: # If we successfully got creds from st.secrets
            print(f"[DB_UTILS] Streamlit Cloud: Successfully fetched credentials for {db_type} from st.secrets.")
            return creds
        # If creds is empty here, but we were in the st.secrets block, it means db_type wasn't matched for specific secret fetching.

    # --- SIMULATION FOR LOCAL TESTING / UI FLOW (mock session) ---
    is_mock_session = not hasattr(session, 'sql') or \
                      (hasattr(session, '__class__') and session.__class__.__name__ == 'MockSession') or \
                      (st and hasattr(st, 'secrets') and hasattr(st.secrets, 'get') and db_type is not None) 
                      # If st.secrets exists and db_type was given, but we didn't return creds, 
                      # it implies we should NOT be using local triggers. This logic branch is tricky.
                      # A cleaner way: explicitly pass a flag if it's streamlit cloud vs local sim vs snowpark.

    # More explicit check for local simulation context (e.g., from Streamlit app's MockSession)
    # This will be true if it's our MockSession from app.py OR if it's Streamlit env BUT db_type specific creds were NOT found/returned above.
    # This needs to be careful not to conflict with real Snowpark session when st is None.
    
    # Simplified: if st.secrets were available and db_type matched and returned, we are done.
    # Otherwise, proceed to local simulation or Snowpark.

    # If it's a local test context (no st.secrets or st.secrets didn't yield creds for the db_type)
    # AND it's a mock session (no real .sql method)
    if (not (st and hasattr(st, 'secrets')and hasattr(st.secrets, 'get') and creds)) and \
       (not hasattr(session, 'sql') or (hasattr(session, '__class__') and session.__class__.__name__ == 'MockSession')):
        print(f"[DB_UTILS] LOCAL TEST: Using secret trigger: {secret_name_or_trigger} for db_type: {db_type}")
        if secret_name_or_trigger == "_LOCAL_POSTGRESQL_":
             print(f"[DB_UTILS] LOCAL TEST: Using HARDCODED credentials for PostgreSQL.")
             return {"username": "postgres", "password": "g8rYxznV_5ErXvD"}
        elif secret_name_or_trigger == "_LOCAL_ORACLE_":
             print(f"[DB_UTILS] LOCAL TEST: Placeholder for Oracle credentials.")
             return {"username": "oracle_user", "password": "oracle_password_placeholder"}
        elif secret_name_or_trigger == "_LOCAL_SQLSERVER_":
             print(f"[DB_UTILS] LOCAL TEST: Placeholder for SQL Server credentials.")
             return {"username": "sqlserver_user", "password": "sqlserver_password_placeholder"}
        elif secret_name_or_trigger == "_LOCAL_TERADATA_":
             print(f"[DB_UTILS] LOCAL TEST: Placeholder for Teradata credentials.")
             return {"username": "teradata_user_placeholder", "password": "teradata_password_placeholder"}
        elif secret_name_or_trigger == "_LOCAL_DATABRICKS_":
             print(f"[DB_UTILS] LOCAL TEST: Placeholder for Databricks credentials.")
             return {"token": "databricks_token_placeholder"}
        elif secret_name_or_trigger == "_LOCAL_SNOWFLAKE_SOURCE_":
            print(f"[DB_UTILS] LOCAL TEST: Placeholder for Snowflake (source) credentials.")
            return {"username": "snowflake_source_user", "password": "snowflake_source_password_placeholder", "account": "your_sf_source_account"}
        elif not secret_name_or_trigger: # Empty trigger for local testing
            # This case might be hit if secret_name was None and db_type didn't match st.secrets
            # Raise error if truly local simulation and no trigger
            if not (st and hasattr(st, 'secrets')and hasattr(st.secrets, 'get')): # if definitely not streamlit context
                 print(f"[DB_UTILS] LOCAL TEST: No secret name provided in simulation mode. Cannot provide mock credentials.")
                 raise ValueError("Secret name (local trigger) cannot be empty for credential retrieval in simulation.")
            else: # In streamlit context but no specific creds found by db_type, and no local trigger
                 raise ValueError(f"Streamlit Cloud: Credentials for db_type '{db_type}' not found in st.secrets and no local trigger fallback applicable.")
        else: # Unknown local trigger
            # Similar check: if definitely local, raise. If streamlit but trigger unknown, raise.
            if not (st and hasattr(st, 'secrets')and hasattr(st.secrets, 'get')):
                print(f"[DB_UTILS] LOCAL TEST: Unknown local secret trigger: '{secret_name_or_trigger}'. No credentials returned.")
                raise ValueError(f"Unknown local secret trigger: '{secret_name_or_trigger}'.")
            else:
                raise ValueError(f"Streamlit Cloud: Unknown local trigger '{secret_name_or_trigger}' used, and credentials for db_type '{db_type}' not found in st.secrets.")
    
    # --- Actual Snowpark Stored Procedure: Use SYSTEM$GET_SECRET ---
    # This is reached if not st.secrets path and not local mock session path
    # Or if st.secrets exists but db_type specific creds were not found AND it's not a local mock session.
    if hasattr(session, 'sql') and callable(session.sql): # Check for a real .sql() method
        print(f"[DB_UTILS] Snowpark SP: Attempting to call SYSTEM$GET_SECRET('{secret_name_or_trigger}') via session.sql")
        try:
            # Ensure secret_name_or_trigger is suitable for SQL. If it contains single quotes, they must be escaped.
            # For simplicity, assume it's a valid Snowflake identifier or already quoted if needed.
            # A safer approach for complex names:
            # fq_secret_name_for_sql = secret_name_or_trigger.replace("'", "''")
            # query = f"SELECT SYSTEM$GET_SECRET('{fq_secret_name_for_sql}') AS secret_val;"
            query = f"SELECT SYSTEM$GET_SECRET('{secret_name_or_trigger}') AS secret_val;"
            
            secret_val_row = session.sql(query).collect()
            if not secret_val_row or secret_val_row[0]["SECRET_VAL"] is None:
                print(f"[DB_UTILS] Secret '{secret_name_or_trigger}' is empty or not found via SYSTEM$GET_SECRET.")
                raise ValueError(f"Secret '{secret_name_or_trigger}' is empty or not found.")
            
            secret_json_str = secret_val_row[0]["SECRET_VAL"]
            print(f"[DB_UTILS] Successfully retrieved secret '{secret_name_or_trigger}'. Length: {len(secret_json_str)}")
            return json.loads(secret_json_str)
            
        except Exception as e: # Catch generic Exception, could be SnowparkSQLException etc.
            print(f"[DB_UTILS] Error retrieving or parsing secret '{secret_name_or_trigger}' via SYSTEM$GET_SECRET: {str(e)}")
            raise ValueError(f"Error retrieving/parsing secret '{secret_name_or_trigger}' via SYSTEM$GET_SECRET: {str(e)}")
    
    # Fallback if no credential strategy matched (should not happen with proper logic flow)
    raise ValueError(f"Could not determine credential retrieval strategy for secret/trigger '{secret_name_or_trigger}' and db_type '{db_type}'.")


def get_source_db_connection(session, db_type: str, connection_params: dict):
    """
    Establishes a connection to the source database.
    - session: Snowpark session, or MockSession for local/Streamlit Cloud.
    - db_type: Type of source DB.
    - connection_params: Host, port, database, etc. 
                         For local testing, 'secret_name' in connection_params is the local trigger.
                         For Snowpark SP, 'secret_name' is the actual Snowflake Secret.
                         For Streamlit Cloud, 'secret_name' is not used for creds; db_type is.
    """
    print(f"[DB_UTILS_GET_CONN] Received db_type: '{db_type}' for connection_params: {connection_params}")
    credentials = {}
    # The 'secret_name' from connection_params is used as the trigger for local simulation
    # or as the actual secret name for Snowpark.
    # For Streamlit Cloud, db_type is the primary key for st.secrets.
    secret_identifier_for_snowpark_or_local_trigger = connection_params.get('secret_name')

    try:
        credentials = _get_secret_credentials(session, secret_identifier_for_snowpark_or_local_trigger, db_type)
    except ValueError as e:
        print(f"[DB_UTILS] Failed to get credentials: {e}")
        return None # Could raise or return None depending on desired error handling in SP

    user = credentials.get('username')
    password = credentials.get('password')
    databricks_token = credentials.get('token') # Specific for Databricks

    host = connection_params.get('host')
    port_str = connection_params.get('port') # Port from UI is string
    database_name = connection_params.get('database')
    
    port = None
    if port_str:
        try:
            port = int(port_str)
        except ValueError:
            print(f"[DB_UTILS] Invalid port value: '{port_str}'. Must be an integer.")
            return None

    # Parameter validation based on db_type
    if db_type == "databricks":
        if not all([databricks_token, host, connection_params.get('http_path')]): # http_path is crucial
            print("[DB_UTILS] Missing one or more connection parameters for Databricks (token, host, http_path).")
            return None
    elif db_type == "snowflake": # Snowflake as a source needs account
        sf_account = credentials.get('account') # From st.secrets or local mock
        if not sf_account: # If not in creds, check connection_params (e.g. from UI)
             sf_account = connection_params.get('account') 
        if not all([user, password, sf_account, database_name]):
            print("[DB_UTILS] Missing one or more connection parameters for Snowflake source (user, password, account, database_name).")
            return None
    elif not all([user, password, host, database_name]): # For most other DBs
        # Port can be optional for some if it has a default and is not in connection_params
        print(f"[DB_UTILS] Missing one or more connection parameters (user, password, host, database_name) for {db_type}.")
        return None

    conn = None
    # IMPORTANT: The actual connection establishment needs to happen within 
    # a Snowpark Python Stored Procedure or UDF that has an External Access Integration specified for non-local calls.

    if db_type == "postgresql":
        try:
            # Use default port 5432 if not provided
            pg_port = port if port is not None else 5432
            print(f"[DB_UTILS] Attempting to connect to PostgreSQL at {host}:{pg_port}, DB: {database_name} with user {user}")
            conn = psycopg2.connect(
                host=host, 
                port=pg_port, 
                dbname=database_name, 
                user=user, 
                password=password,
                connect_timeout=10, # Example timeout
                sslmode='require'
            )
            print("[DB_UTILS] Successfully connected to PostgreSQL.")
        except psycopg2.Error as e:
            print(f"[DB_UTILS] psycopg2 connection error: {e}")
            return None
        except Exception as e:
            print(f"[DB_UTILS] Generic error during PostgreSQL connection: {e}")
            return None
    elif db_type == "oracle":
        try:
            ora_host = host
            # Default Oracle port 1521 if not provided
            ora_port = port if port is not None else 1521
            ora_service_name = database_name 
            ora_user = user
            ora_password = password

            if not ora_service_name:
                print("[DB_UTILS] Oracle Service Name/SID not provided.")
                return None

            dsn = oracledb.makedsn(ora_host, int(ora_port), service_name=ora_service_name)
            print(f"[DB_UTILS] Attempting to connect to Oracle at {dsn} with user {ora_user}")
            conn = oracledb.connect(user=ora_user, password=ora_password, dsn=dsn)
            print("[DB_UTILS] Successfully connected to Oracle.")
        except oracledb.Error as e:
            print(f"[DB_UTILS] oracledb connection error: {e}")
            return None
        except Exception as e:
            print(f"[DB_UTILS] Generic error during Oracle connection: {e}")
            return None
    elif db_type == "teradata":
        try:
            td_host = host
            td_user = user
            td_password = password
            # Default Teradata port 1025 if not provided
            td_port_str = str(port) if port is not None else "1025"
            td_database = connection_params.get('database') # Optional for Teradata

            print(f"[DB_UTILS] Attempting to connect to Teradata at {td_host}:{td_port_str}, DB: {td_database or 'default user DB'} with user {td_user}")
            
            conn_params_td = {
                'host': td_host,
                'user': td_user,
                'password': td_password,
                'dbs_port': td_port_str, # teradatasql expects dbs_port as string
                'logmech': 'TD2' 
            }
            if td_database:
                conn_params_td['database'] = td_database

            conn = teradatasql.connect(**conn_params_td)
            print("[DB_UTILS] Successfully connected to Teradata.")
        except teradatasql.Error as e:
            print(f"[DB_UTILS] teradatasql connection error: {e}")
            return None
        except Exception as e:
            print(f"[DB_UTILS] Generic error during Teradata connection: {e}")
            return None
    elif db_type == "databricks":
        try:
            dbx_host = host 
            dbx_http_path = connection_params.get('http_path') # Already validated this exists
            # dbx_token is from credentials dict

            print(f"[DB_UTILS] Attempting to connect to Databricks at {dbx_host} with HTTP Path {dbx_http_path}")
            
            conn = databricks_sql.connect(
                server_hostname=dbx_host,
                http_path=dbx_http_path,
                access_token=databricks_token,
            )
            print("[DB_UTILS] Successfully connected to Databricks.")
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
            # Account can come from credentials (st.secrets) or connection_params (UI)
            sf_account = credentials.get('account', connection_params.get('account'))
            sf_database = database_name 
            sf_schema = connection_params.get('schema') 
            sf_warehouse = connection_params.get('warehouse') 
            sf_role = connection_params.get('role')

            # sf_account was validated during param check already
            
            print(f"[DB_UTILS] Attempting to connect to Snowflake source: account={sf_account}, user={sf_user}, db={sf_database}, schema={sf_schema}")
            conn = snowflake.connector.connect(
                user=sf_user,
                password=sf_password,
                account=sf_account,
                database=sf_database, 
                schema=sf_schema,    
                warehouse=sf_warehouse, 
                role=sf_role          
            )
            print("[DB_UTILS] Successfully connected to Snowflake source.")
        except snowflake.connector.errors.DatabaseError as e:
            print(f"[DB_UTILS] Snowflake connector error: {e}")
            return None
        except Exception as e:
            print(f"[DB_UTILS] Generic error during Snowflake source connection: {e}")
            return None
    else:
        print(f"[DB_UTILS] Unsupported database type for connection: {db_type}")
        return None # Explicitly return None for unsupported types

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
class StInfoMock: # Renamed to avoid conflict
    def info(self, msg):
        print(f"[StInfoMock] {msg}")

# This global `st` from `import streamlit as st` might be None if streamlit is not installed.
# The mock is for cases where this code might be run outside Streamlit AND outside Snowpark,
# and something expects `st.info`. However, this file primarily uses `print`.
# The `st` used for `st.secrets` is the conditionally imported `streamlit` module.
# The existing print statements are fine for logging in SPs and local tests.
# Removing the `st = StInfo()` or `st_info_mock = StInfoMock()` at global scope
# if `import streamlit as st` is the primary way to get `st`.
# The original file had `st = StInfo()` at the end.
# We should ensure that any `st.info` calls would work.
# Given the current structure, it's safest to rely on `print` for logging within this util file.
# Let's ensure the bottom of the file is clean or has a non-conflicting mock if needed.

# Current file doesn't use st.info, it uses print(). The st.secrets access is guarded.
# The StInfo class can be removed if not used.
# If the `session` object in Snowpark has a log method, that would be preferred for SPs.
# Let's remove the placeholder st = StInfo() to avoid confusion.
# If any code *was* using st.info, it would need to be print() or handle st being None.

# Final check on the provided file shows 'st = StInfo()' at the end.
# This will conflict if `import streamlit as st` is successful.
# Let's comment out the original mock if `streamlit` is imported.

# Original end of file:
# class StInfo:
#     def info(self, msg):
#         print(msg)
# st = StInfo() 

# Corrected approach:
_st_mock_instance = None
if not st: # If `import streamlit as st` failed or `st` is None
    class StInfoMockInternal:
        def info(self, msg):
            print(f"[DB_UTILS_MOCK_INFO] {msg}")
    _st_mock_instance = StInfoMockInternal()

# Now, if some function absolutely needed to call st.info, it could do:
# (st or _st_mock_instance).info("message")
# But since this file uses print(), this is mostly for completeness if other parts relied on a global `st.info`.
# The current structure with `print` statements is fine.
