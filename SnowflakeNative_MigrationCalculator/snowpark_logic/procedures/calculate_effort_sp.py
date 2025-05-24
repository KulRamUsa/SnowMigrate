# snowpark_logic/procedures/calculate_effort_sp.py
# Snowpark Stored Procedure for Effort Calculation

import sys
import json
from typing import Dict, Any

# --- Dynamically add project root to sys.path for local simulation ---
import pathlib
PROCEDURE_FILE_PATH = pathlib.Path(__file__).resolve()
SNOWPARK_LOGIC_DIR = PROCEDURE_FILE_PATH.parent.parent
NATIVE_CALCULATOR_DIR = SNOWPARK_LOGIC_DIR.parent
PROJECT_ROOT_FOR_SP = NATIVE_CALCULATOR_DIR.parent

if str(PROJECT_ROOT_FOR_SP) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_FOR_SP))
# --- End dynamic path addition ---

# Import from common module
from SnowflakeNative_MigrationCalculator.snowpark_logic.common import effort_calculator_logic

# Placeholder for Snowpark session type hint
class Session: pass

def calculate_effort_sp(session: Session, # Snowpark session object
                          source_db_type: str,
                          analysis_results_json: str, # JSON string from analyze_database_sp
                          target_db_type: str = "snowflake"
                          ) -> str:
    """
    Snowpark Stored Procedure to calculate migration effort based on analysis results.
    Args:
        session: The Snowpark session.
        source_db_type: Type of the source database.
        analysis_results_json: JSON string containing object counts from the analysis phase.
                               Expected to contain at least a key like "total_objects"
                               which is a dict of object_type: count.
        target_db_type: Type of the target database (default 'snowflake').
    Returns:
        JSON string containing the detailed effort estimation.
    """
    try:
        print(f"[SP_CALCULATE] Received analysis_results_json: {analysis_results_json}")
        analysis_results = json.loads(analysis_results_json)
        
        objects_counts = analysis_results.get("total_objects")
        if objects_counts is None or not isinstance(objects_counts, dict):
            if isinstance(analysis_results.get("tables"), int): 
                 objects_counts = analysis_results
                 print("[SP_CALCULATE] Used root of analysis_results as object_counts.")
            else:
                print("[SP_CALCULATE] Error: 'total_objects' key missing or invalid in analysis_results_json.")
                raise ValueError("Invalid analysis_results_json: 'total_objects' key with object counts dictionary is missing or not a dict.")
        else:
            print(f"[SP_CALCULATE] Extracted object_counts: {objects_counts}")

        effort_details = effort_calculator_logic.calculate_migration_effort(
            source_type=source_db_type,
            target_type=target_db_type,
            objects=objects_counts
        )
        print(f"[SP_CALCULATE] Calculated effort_details: {effort_details}")

        # Optionally, store effort_details in a Snowflake table
        # df = session.create_dataframe([effort_details]) 
        # df.write.mode("append").save_as_table("MIGRATION_CALCULATOR_DB.APP_SCHEMA.EFFORT_ESTIMATES")

        return json.dumps(effort_details)

    except json.JSONDecodeError as e:
        print(f"[SP_CALCULATE] JSONDecodeError: {str(e)}")
        return json.dumps({"error": f"Invalid JSON in analysis_results_json: {str(e)}"})
    except ValueError as e:
        print(f"[SP_CALCULATE] ValueError: {str(e)}")
        return json.dumps({"error": str(e)})
    except Exception as e:
        print(f"[SP_CALCULATE] An unexpected error occurred: {str(e)}")
        return json.dumps({"error": f"An unexpected error occurred in calculate_effort_sp: {str(e)}"})

# Example of how this might be registered (conceptual):
# if __name__ == "__main__":
#     mock_session = Session()
#     print("--- Test Case 1: Calculate Effort for PostgreSQL (Simulated) ---")
#     mock_analysis_data_pg = {
#         "total_objects": {"tables": 70, "views": 30, "procedures": 5, "functions": 15},
#         "schemas_summary": [{"name": "public", "tables": 70, "views": 30, "procedures": 5, "functions": 15}]
#     }
#     result_pg_effort = calculate_effort_sp(mock_session, "postgresql", json.dumps(mock_analysis_data_pg))
#     print(f"SP Result (PostgreSQL Effort):\n{json.dumps(json.loads(result_pg_effort), indent=4)}\n")

#     print("--- Test Case 2: Analysis results as direct counts ---")
#     mock_analysis_data_direct = {"tables": 100, "views": 50, "procedures": 20, "functions": 10}
#     result_direct = calculate_effort_sp(mock_session, "sqlserver", json.dumps(mock_analysis_data_direct))
#     print(f"SP Result (Direct Counts SQL Server):\n{json.dumps(json.loads(result_direct), indent=4)}\n")

#     print("--- Test Case 3: Invalid JSON for analysis results ---")
#     error_result = calculate_effort_sp(mock_session, "oracle", "not_valid_json_again")
#     print(f"SP Error Result (Invalid JSON):\n{json.dumps(json.loads(error_result), indent=4)}\n") 