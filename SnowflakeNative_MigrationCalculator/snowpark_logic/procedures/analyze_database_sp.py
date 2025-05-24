# snowpark_logic/procedures/analyze_database_sp.py
# Snowpark Stored Procedure for Database Analysis

import sys
import json
from typing import Dict, Any

# --- Dynamically add project root to sys.path for local simulation ---
# This allows direct execution/testing of this SP file locally if needed,
# and helps Streamlit app's direct imports during its simulation mode.
import pathlib
PROCEDURE_FILE_PATH = pathlib.Path(__file__).resolve()
SNOWPARK_LOGIC_DIR = PROCEDURE_FILE_PATH.parent.parent
NATIVE_CALCULATOR_DIR = SNOWPARK_LOGIC_DIR.parent
PROJECT_ROOT_FOR_SP = NATIVE_CALCULATOR_DIR.parent

if str(PROJECT_ROOT_FOR_SP) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_FOR_SP))
# --- End dynamic path addition ---

# --- TEMPORARILY COMMENT OUT DB_CONNECTOR_UTILS IMPORT ---
from SnowflakeNative_MigrationCalculator.snowpark_logic.common import db_connector_utils

# Placeholder for Snowpark session type hint if not fully available in local dev
# In Snowflake, the session is passed by the runtime.
class Session:
    def sql(self, query: str):
        # Mock implementation for local testing if any direct SQL was needed here (not currently)
        print(f"[MockSession-SP] SQL: {query}")
        return self
    def collect(self):
        return []

def analyze_database_sp(session: Session, # Snowpark session object, automatically passed
                        source_db_type: str, 
                        connection_details_json: str, # JSON string for connection_params
                        target_db_type: str = "snowflake" # Default target is Snowflake
                        ) -> str:
    """
    Snowpark Stored Procedure to connect to a source database, analyze its objects,
    and return a summary.
    Args:
        session: The Snowpark session.
        source_db_type: Type of the source database (e.g., 'oracle', 'sqlserver', 'postgresql').
        connection_details_json: JSON string containing connection parameters for the source DB.
                                  Expected keys: host, port, database, secret_name, [schema], etc.
        target_db_type: Type of the target database (currently always 'snowflake').
    Returns:
        JSON string containing the analysis results (object counts, schema summary)
        or an error message.
    """
    print(f"[ANALYZE_DB_SP] Received source_db_type: '{source_db_type}'") # DEBUG
    print(f"[ANALYZE_DB_SP] Received connection_details_json: {connection_details_json}") # DEBUG
    
    # --- TEMPORARY MODIFICATION: Return dummy success immediately ---
    # print("[ANALYZE_DB_SP] TEMPORARY DEBUG: Returning dummy success before db_connector_utils involvement.")
    # dummy_success_result = {
    # "total_objects": {"tables": 0, "views": 0, "procedures": 0, "functions": 0},
    # "schemas_summary": [],
    # "debug_message": "Returned by temporary modification in analyze_database_sp.py"
    # }
    # return json.dumps(dummy_success_result)
    # --- END TEMPORARY MODIFICATION ---

    conn = None 
    try:
        connection_params = json.loads(connection_details_json)
        db_name = connection_params.get('database')
        schema_name = connection_params.get('schema')
        print(f"[SP_ANALYZE] Attempting connection for {source_db_type} with params: {connection_params}")
        
        # --- Original call that uses db_connector_utils ---
        conn = db_connector_utils.get_source_db_connection(session, source_db_type, connection_params)
        analysis_results = {}
        if conn:
           print(f"[SP_ANALYZE] Connection successful for {source_db_type}. Fetching object counts.")
           try:
               analysis_results = db_connector_utils.fetch_object_counts(
                   conn, source_db_type, db_name, schema_name
               )
               if isinstance(analysis_results, dict) and 'error' in analysis_results:
                   return json.dumps({"error": f"Error fetching object counts: {analysis_results['error']}"})
               print(f"[SP_ANALYZE] Object counts fetched: {analysis_results}")
           except Exception as e:
               return json.dumps({"error": f"Error fetching object counts: {str(e)}"})
        else:
           return json.dumps({"error": f"Failed to connect to source database: {source_db_type}."})
        return json.dumps(analysis_results)
        # pass # Placeholder for original logic

    except json.JSONDecodeError as e:
        return json.dumps({"error": f"Invalid JSON: {str(e)}"})
    except ValueError as e: 
        return json.dumps({"error": str(e)})
    except Exception as e:
        return json.dumps({"error": f"Unexpected error in SP: {str(e)}"}) 
    finally:
        if conn:
            try:
                conn.close()
            except Exception as e:
                print(f"[SP_ANALYZE] Error closing connection: {str(e)}")

# Example of how this might be registered (conceptual, actual registration is via SQL):
# if __name__ == "__main__":
#     # This block is for local testing/understanding and won't run in Snowflake directly.
#     # Create a mock session object for local testing
#     mock_session = Session() 

#     print("--- Test Case 1: Successful PostgreSQL Analysis (Simulated) ---")
#     mock_pg_connection_details = {
#         "host": "localhost", 
#         "port": 5432, 
#         "database": "dvdrental", # A common sample PostgreSQL database
#         "secret_name": "MOCK_DB_SECRET", # This will use the explicitly mocked credentials in db_connector_utils
#         "schema": "public" # Optional, test with and without
#     }
#     result_pg = analyze_database_sp(mock_session, "postgresql", json.dumps(mock_pg_connection_details))
#     print(f"SP Result (PostgreSQL):\n{json.dumps(json.loads(result_pg), indent=4)}\n")

#     print("--- Test Case 2: Connection Failure (e.g., bad host in real scenario, simulated by no host) ---")
#     mock_fail_connection_details = {
#         "port": 5432, 
#         "database": "dvdrental",
#         "secret_name": "MOCK_DB_SECRET_BAD" # Will try actual secret logic (placeholder) or fail in _get_secret_credentials
#     }
#     # To truly test connection failure simulation, you might need to modify db_connector_utils temporarily
#     # or ensure MOCK_DB_SECRET_BAD does not lead to mock credentials.
#     # For now, let's assume this leads to conn = None in the SP:
#     # result_fail_conn = analyze_database_sp(mock_session, "postgresql", json.dumps(mock_fail_connection_details))
#     # print(f"SP Result (Connection Fail):\n{json.dumps(json.loads(result_fail_conn), indent=4)}\n")
    
#     print("--- Test Case 3: Invalid JSON input ---")
#     result_invalid_json = analyze_database_sp(mock_session, "postgresql", "not_a_json_string")
#     print(f"SP Result (Invalid JSON):\n{json.dumps(json.loads(result_invalid_json), indent=4)}\n")

#     print("--- Test Case 4: Unsupported DB Type ---")
#     # result_unsupported_db = analyze_database_sp(mock_session, "nosuchdb", json.dumps(mock_pg_connection_details))
#     # print(f"SP Result (Unsupported DB):\n{json.dumps(json.loads(result_unsupported_db), indent=4)}\n") 