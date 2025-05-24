# SnowflakeNative_MigrationCalculator/streamlit_app/app.py
import streamlit as st
import json
import pandas as pd # Moved pandas import to the top
import pathlib # Added for robust path handling
import sys
# from snowflake.snowpark.exceptions import SnowparkSQLException
# from snowflake.snowpark.functions import call_udf # For calling UDFs if needed directly

# --- Dynamically add project root to sys.path ---
APP_FILE_PATH = pathlib.Path(__file__).resolve()
STREAMLIT_APP_DIR = APP_FILE_PATH.parent
SNOWFLAKE_NATIVE_DIR = STREAMLIT_APP_DIR.parent # This is SnowflakeNative_MigrationCalculator dir
PROJECT_ROOT = SNOWFLAKE_NATIVE_DIR.parent      # This is AIPoweredCalcualator dir

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# --- End dynamic path addition ---

# --- Page Configuration ---
st.set_page_config(
    page_title="Snowflake Migration Effort Calculator",
    page_icon="❄️",
    layout="wide"
)

# --- Constants & Mock Data ---
# In a real app, the template would be read from a stage using session.file.get()
APP_DIR = pathlib.Path(__file__).resolve().parent # Gets the directory of app.py
MIGRATION_DOCUMENT_TEMPLATE_PATH = APP_DIR / ".." / "templates" / "migration_document.md.j2"
# For local dev, you might read it directly if the app runs where it can access this path
try:
    # Ensure the path is resolved to avoid issues with relative parts if any remain
    with open(MIGRATION_DOCUMENT_TEMPLATE_PATH.resolve(), "r") as f:
        DEFAULT_TEMPLATE_STRING = f.read()
except FileNotFoundError:
    # st.error(f"FATAL: Template file not found at {MIGRATION_DOCUMENT_TEMPLATE_PATH}. Placeholders will be used.")
    # More informative error with the resolved path:
    st.error(f"FATAL: Template file not found at {str(MIGRATION_DOCUMENT_TEMPLATE_PATH.resolve())}. Placeholders will be used.")
    DEFAULT_TEMPLATE_STRING = "# {{ source_type_display }} to Snowflake Migration Document (Template Missing)"

# --- Helper Functions (Conceptual - will interact with SPs) ---

def call_analyze_database_sp(session, source_db_type: str, connection_details: dict) -> dict:
    """Placeholder to call the SP_ANALYZE_DATABASE."""
    st.info(f"Simulating call to SP_ANALYZE_DATABASE for {source_db_type}...")
    connection_details_json = json.dumps(connection_details)
    try:
        # In real app:
        # result_json = session.call("MIGRATION_CALCULATOR_DB.APP_SCHEMA.SP_ANALYZE_DATABASE", 
        #                            source_db_type, 
        #                            connection_details_json)
        # For now, using placeholder from analyze_database_sp.py
        from SnowflakeNative_MigrationCalculator.snowpark_logic.procedures.analyze_database_sp import analyze_database_sp
        # Mock session object for placeholder
        class MockSession: pass
        result_json = analyze_database_sp(MockSession(), source_db_type, connection_details_json)

        result = json.loads(result_json)
        if "error" in result:
            st.error(f"Analysis Error: {result['error']}")
            return None
        st.success("Analysis successful (simulated)!")
        return result
    except Exception as e: # Catches SnowparkSQLException in real app
        st.error(f"Error calling analyze_database_sp: {e}")
        return None

def call_calculate_effort_sp(session, source_db_type: str, analysis_results: dict) -> dict:
    """Placeholder to call the SP_CALCULATE_EFFORT."""
    st.info(f"Simulating call to SP_CALCULATE_EFFORT for {source_db_type}...")
    analysis_results_json = json.dumps(analysis_results)
    try:
        # In real app:
        # result_json = session.call("MIGRATION_CALCULATOR_DB.APP_SCHEMA.SP_CALCULATE_EFFORT", 
        #                            source_db_type, 
        #                            analysis_results_json)
        from SnowflakeNative_MigrationCalculator.snowpark_logic.procedures.calculate_effort_sp import calculate_effort_sp
        class MockSession: pass
        result_json = calculate_effort_sp(MockSession(), source_db_type, analysis_results_json)
        
        result = json.loads(result_json)
        if "error" in result:
            st.error(f"Calculation Error: {result['error']}")
            return None
        st.success("Effort calculation successful (simulated)!")
        return result
    except Exception as e:
        st.error(f"Error calling calculate_effort_sp: {e}")
        return None

def call_generate_document_sp(session, source_db_type: str, effort_results: dict, analysis_results: dict, connection_details: dict) -> str:
    """Placeholder to call the SP_GENERATE_DOCUMENT."""
    st.info("Simulating call to SP_GENERATE_DOCUMENT...")
    effort_results_json = json.dumps(effort_results)
    analysis_results_json = json.dumps(analysis_results)
    connection_details_json = json.dumps(connection_details)
    
    # template_string: In a real app, read from stage or use a UDF to get it.
    # For now, using the one read at the start of this script.
    template_string = DEFAULT_TEMPLATE_STRING

    try:
        # In real app:
        # document_md = session.call("MIGRATION_CALCULATOR_DB.APP_SCHEMA.SP_GENERATE_DOCUMENT",
        #                            effort_results_json,
        #                            analysis_results_json,
        #                            source_db_type,
        #                            connection_details_json,
        #                            template_string) # This assumes template_string is passed
        from SnowflakeNative_MigrationCalculator.snowpark_logic.procedures.generate_document_sp import generate_document_sp
        class MockSession: pass
        document_md = generate_document_sp(MockSession(), effort_results_json, analysis_results_json, source_db_type, connection_details_json, template_string)

        if document_md.startswith('{\"error\":'): # Check if returned JSON is an error
             error_detail = json.loads(document_md)
             st.error(f"Documentation Generation Error: {error_detail['error']}")
             return None
        st.success("Document generation successful (simulated)!")
        return document_md
    except Exception as e:
        st.error(f"Error calling generate_document_sp: {e}")
        return None

# --- Initialize session state ---
if 'connection_details' not in st.session_state:
    st.session_state.connection_details = {}
if 'analysis_result' not in st.session_state:
    st.session_state.analysis_result = None
if 'effort_result' not in st.session_state:
    st.session_state.effort_result = None
if 'generated_document' not in st.session_state:
    st.session_state.generated_document = None
if 'current_step' not in st.session_state:
    st.session_state.current_step = 1 

# Initialize keys for input fields if they are accessed by on_change or other logic before widget creation,
# OR if the widget itself doesn't consistently set an initial value via its `value` param.

# Ensured keys for selectboxes/inputs that might be read by `st.session_state.get()` before render or by on_change logic.
# Default values here are fallbacks if widget rendering logic fails to set them (which shouldn't happen).
if 'db_type_input' not in st.session_state: # Default value is set in st.selectbox
    st.session_state.db_type_input = "postgresql" 
# For text_input fields where value is NOT explicitly set in the widget call, initialize here.
# For those where value IS set (e.g. port_input from port_val), no need to init its value here.
if 'host_input' not in st.session_state: 
    st.session_state.host_input = "" # value is not set in st.text_input, so default here is fine
# port_input has its `value` dynamically set in st.number_input, so avoid re-initializing its value here.
# if 'port_input' not in st.session_state: st.session_state.port_input = 0 
if 'database_input' not in st.session_state:
    st.session_state.database_input = ""
if 'schema_input' not in st.session_state:
    st.session_state.schema_input = ""
# secret_name_input has its `value` dynamically set, so avoid re-initializing its value here.
# if 'secret_name_input' not in st.session_state: st.session_state.secret_name_input = "_LOCAL_POSTGRES_" 
if 'sf_account_input' not in st.session_state:
    st.session_state.sf_account_input = ""
if 'dbx_http_path' not in st.session_state: 
    st.session_state.dbx_http_path = "" # value is not set in st.text_input initially
if 'td_logmech_input' not in st.session_state: # Default value is set in st.selectbox
    st.session_state.td_logmech_input = "Default"

# --- UI Layout ---
st.title("❄️ Snowflake Migration Effort Calculator ❄️")
st.markdown("Streamlit Application for assessing migration effort to Snowflake.")

# --- Establish Snowpark Connection (Conceptual) ---
# try:
#     session = st.connection("snowpark").session # Establishes the Snowpark session
#     st.success("Snowpark session established!")
# except Exception as e:
#     st.error(f"Failed to establish Snowpark session: {e}. Application will run in simulation mode.")
#     session = None # Fallback to simulation mode
session = None # For now, always simulate as SPs are not deployed

# Helper to clear downstream state. Referenced by on_change in input widgets.
def clear_downstream_state(*state_keys_to_clear):
    for key in state_keys_to_clear:
        if key in st.session_state:
            st.session_state[key] = None
    st.session_state.current_step = 1 # Reset to first step

# --- Step 1: Database Connection Form ---
with st.expander("Step 1: Connect to Source Database", expanded=st.session_state.current_step == 1):
    st.subheader("Source Database Details")
    
    source_db_types = ["postgresql", "teradata", "databricks", "sqlserver", "oracle", "snowflake", "mysql"]
    # Ensure on_change clears relevant states when DB type changes
    db_type = st.selectbox("Source Database Type", options=source_db_types, index=0, key="db_type_input", 
                           on_change=clear_downstream_state, args=(("analysis_result", "effort_result", "generated_document", "dbx_http_path")))

    cols = st.columns(2)
    host = cols[0].text_input("Host", key="host_input", 
                              on_change=clear_downstream_state, args=(("analysis_result", "effort_result", "generated_document")))
    port_default_map = {"oracle": 1521, "sqlserver": 1433, "postgresql": 5432, "mysql": 3306, "teradata": 1025}
    # Adjust port default based on selected db_type dynamically if possible or ensure user checks
    port_val = port_default_map.get(st.session_state.get("db_type_input", "postgresql"), 0)
    port = cols[1].number_input("Port", value=port_val, step=1, format="%d", key="port_input", 
                                on_change=clear_downstream_state, args=(("analysis_result", "effort_result", "generated_document")))
    
    db_name_label = "Database Name"
    if st.session_state.get("db_type_input") == "oracle":
        db_name_label = "Service Name / SID"
    elif st.session_state.get("db_type_input") == "databricks":
        db_name_label = "Databricks Schema (Database)" # Changed from Catalog
        
    database = st.text_input(db_name_label, key="database_input", 
                               on_change=clear_downstream_state, args=(("analysis_result", "effort_result", "generated_document")))
    schema = st.text_input("Schema (Optional, for focused analysis)", key="schema_input", 
                             on_change=clear_downstream_state, args=(("analysis_result", "effort_result", "generated_document")))

    # --- Source-Specific Fields (using st.session_state.get("db_type_input")) ---
    if st.session_state.get("db_type_input") == "snowflake":
        st.text_input("Snowflake Account Identifier (Source)", help="e.g., xy12345.east-us-2.azure", key="sf_account_input", 
                      on_change=clear_downstream_state, args=(("analysis_result", "effort_result", "generated_document")))
    
    if st.session_state.get("db_type_input") == "databricks":
        st.text_input("Databricks HTTP Path", 
                      key="dbx_http_path", 
                      placeholder="/sql/1.0/warehouses/your_warehouse_id", 
                      help="Get this from your Databricks SQL Warehouse connection details.", 
                      on_change=clear_downstream_state, 
                      args=(("analysis_result", "effort_result", "generated_document")))
    
    if st.session_state.get("db_type_input") == "teradata":
        st.selectbox("Teradata Logon Mechanism", options=["Default", "LDAP", "TD2"], index=0, key="td_logmech_input", 
                     on_change=clear_downstream_state, args=(("analysis_result", "effort_result", "generated_document")))
    
    secret_name_default = f"_LOCAL_{st.session_state.get("db_type_input","postgresql").upper()}_" # default db_type if not yet set
    secret_name = st.text_input("Snowflake Secret Name (for DB credentials)", 
                                value=secret_name_default, 
                                help="For local testing, use format _LOCAL_DBTYPE_. In Snowflake, format: MY_SCHEMA.MY_SECRET_NAME.",
                                key="secret_name_input", 
                                on_change=clear_downstream_state, args=(("analysis_result", "effort_result", "generated_document")))

    if st.button("1. Analyze Database", type="primary"):
        # Basic validation using correct session state keys
        current_db_type = st.session_state.get("db_type_input")
        print(f"[STREAMLIT_APP] Current DB Type from UI: '{current_db_type}'") # DEBUG
        current_host = st.session_state.get("host_input")
        current_port = st.session_state.get("port_input")
        current_db_name = st.session_state.get("database_input")
        current_secret_name = st.session_state.get("secret_name_input")
        current_dbx_http_path = st.session_state.get("dbx_http_path")

        required_fields = [current_db_type, current_host, current_port, current_db_name, current_secret_name]
        if current_db_type == "Databricks" and not current_dbx_http_path:
            st.warning("Databricks HTTP Path is required when Databricks is selected.")
            st.stop()
        
        if not all(required_fields):
            st.warning("Please fill in all required connection fields (Type, Host, Port, Database/Service, Secret Name).")
            st.stop()

        st.session_state.analysis_result = None
        st.session_state.effort_result = None
        st.session_state.generated_document = None
        
        connection_params = {
            "host": current_host,
            "port": current_port,
            "database": current_db_name,
            "schema": st.session_state.get("schema_input") if st.session_state.get("schema_input") else None,
            "secret_name": current_secret_name 
        }
        if current_db_type == "Databricks":
            connection_params["http_path"] = current_dbx_http_path
        if current_db_type == "snowflake":
            connection_params["account"] = st.session_state.get("sf_account_input")
        if current_db_type == "teradata":
            connection_params["logmech"] = st.session_state.get("td_logmech_input")

        # Store details for subsequent steps that might need them (like document generation)
        st.session_state.connection_details = {
            "dbType": current_db_type,
            **connection_params # Add all other params
        }

        with st.spinner("Analyzing source database..."):
            analysis_res = call_analyze_database_sp(session, current_db_type, connection_params)
            
            if analysis_res:
                st.session_state.analysis_result = analysis_res
                st.session_state.effort_result = None 
                st.session_state.generated_document = None
                st.session_state.current_step = 2
                st.rerun()

# --- Step 2: View Analysis & Calculate Effort ---
if st.session_state.analysis_result:
    with st.expander("Step 2: Review Analysis & Calculate Effort", expanded=st.session_state.current_step == 2):
        st.subheader("Database Analysis Results")
        st.json(st.session_state.analysis_result) # Display raw JSON from analysis

        if st.button("2. Calculate Migration Effort", type="primary"):
            if session or True: # Allow simulation even if session is None
                effort_res = call_calculate_effort_sp(session, 
                                                       st.session_state.connection_details.get("dbType", "unknown"), 
                                                       st.session_state.analysis_result)
                if effort_res:
                    st.session_state.effort_result = effort_res
                    st.session_state.generated_document = None # Reset document
                    st.session_state.current_step = 3
                    st.rerun()
            else:
                st.error("Snowpark session not available. Cannot calculate effort.")

# --- Step 3: View Effort & Generate Document ---
if st.session_state.effort_result:
    with st.expander("Step 3: Review Effort & Generate Document", expanded=st.session_state.current_step == 3):
        st.subheader("Migration Effort Estimation")
        # Display formatted effort results (example)
        st.markdown(f"**Overall Complexity:** `{st.session_state.effort_result.get('complexity', 'N/A')}`")
        st.markdown(f"**Total Estimated Hours:** `{st.session_state.effort_result.get('total_hours', 0)}`")
        
        st.write("**Effort by Object Type:**")
        if "object_efforts" in st.session_state.effort_result:
            st.table(pd.DataFrame.from_dict(st.session_state.effort_result["object_efforts"], orient="index"))
        else:
            st.markdown("_No detailed object efforts available._")

        st.write("**Business Value Add:**")
        for item in st.session_state.effort_result.get("business_value_add", []): st.markdown(f"- {item}")
        
        st.write("**Potential Risks:**")
        for item in st.session_state.effort_result.get("risks", []): st.markdown(f"- {item}")

        st.write("**Recommendations:**")
        for item in st.session_state.effort_result.get("recommendations", []): st.markdown(f"- {item}")
        
        # Raw JSON for full details
        with st.popover("View Full Effort JSON"):
            st.json(st.session_state.effort_result)

        if st.button("3. Generate Migration Strategy Document", type="primary"):
            if session or True: # Allow simulation
                 doc_md = call_generate_document_sp(session,
                                                   st.session_state.connection_details.get("dbType", "unknown"),
                                                   st.session_state.effort_result,
                                                   st.session_state.analysis_result,
                                                   st.session_state.connection_details)
                 if doc_md:
                     st.session_state.generated_document = doc_md
                     # st.session_state.current_step = 4 # Or just display below
                     st.rerun()
            else:
                st.error("Snowpark session not available. Cannot generate document.")

if st.session_state.generated_document:
    st.subheader("Generated Migration Strategy Document")
    st.markdown(st.session_state.generated_document)
    st.download_button(
        label="Download Document as Markdown",
        data=st.session_state.generated_document,
        file_name=f"migration_strategy_{st.session_state.connection_details.get('dbType', 'db')}_to_snowflake.md",
        mime="text/markdown"
    )

# --- Footer & Notes ---
st.markdown("---")
st.markdown("**Note:** This application currently runs in **simulation mode**. "
            "Actual database connections and Stored Procedure calls are mocked. "
            "Full functionality requires deployment within a Snowflake environment with configured Snowpark, Stored Procedures, Network Rules, and Secrets.")

# For table display of object efforts, pandas is nice. Add to requirements if not already there.
# import pandas as pd # Add this import at the top if using st.table with DataFrame


