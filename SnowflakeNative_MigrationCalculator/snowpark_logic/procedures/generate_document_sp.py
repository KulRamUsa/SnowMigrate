# snowpark_logic/procedures/generate_document_sp.py
# Snowpark Stored Procedure for Generating Migration Document

import sys
import json
import datetime
from typing import Dict, Any, List

# For deploying to Snowflake, Jinja2 needs to be available.
# It can be specified in the CREATE PROCEDURE ... PACKAGES = ('jinja2') statement.
# The template file also needs to be accessible, e.g., by reading from a stage.
import jinja2 

# Add the common directory to sys.path to import utility modules
# sys.path.append("../common") # For local understanding
# from common import effort_calculator_logic # For _format_object_efforts if we refactor it here

# Helper Jinja2 filter functions (similar to DocumentationGenerator in the original backend)
# These could also live in a common utility file if used by multiple UDFs/SPs.
def _format_object_efforts_filter(object_efforts_dict):
    if not object_efforts_dict or not isinstance(object_efforts_dict, dict):
        return "No specific object efforts available."
    lines = []
    lines.append("| Object Type | Count         | Hours/Object | Total Hours   |")
    lines.append("|-------------|---------------|--------------|---------------|")
    for obj_type, effort_details in object_efforts_dict.items():
        if effort_details.get("count", 0) > 0:
            lines.append(f"| {obj_type.capitalize():<11} | {effort_details.get('count', 0):<13} | {effort_details.get('hours_per_object', 0):<12} | {effort_details.get('total_hours', 0):<13} |")
    if len(lines) == 2: # Only headers
        return "No specific object efforts with count > 0 available."
    return "\n".join(lines)

def _format_list_filter(items_list):
    if not items_list or not isinstance(items_list, list):
        return "- Not applicable or no specific items provided."
    return "\n".join([f"- {item}" for item in items_list])


def generate_document_sp(session, # Snowpark session object
                           effort_calculation_json: str, # JSON from calculate_effort_sp
                           analysis_results_json: str,   # JSON from analyze_database_sp (for object counts)
                           source_db_type: str,
                           connection_details_json: str, # For source display in doc
                           template_string: str # The actual Jinja2 template content as a string
                           ) -> str:
    """
    Snowpark Stored Procedure to generate a migration strategy document using Jinja2.

    Args:
        session: The Snowpark session.
        effort_calculation_json: JSON string of the effort calculation results.
        analysis_results_json: JSON string of the analysis results (contains object counts).
        source_db_type: The source database type (e.g., 'oracle').
        connection_details_json: JSON string of the original connection parameters for display.
        template_string: The migration document Jinja2 template content itself.
                         In practice, this might be read from a file in a stage.

    Returns:
        A string containing the rendered markdown document or an error message.
    """
    try:
        effort_data = json.loads(effort_calculation_json)
        analysis_data = json.loads(analysis_results_json)
        connection_details = json.loads(connection_details_json)

        # Prepare context for Jinja2 template
        # Extract object counts from analysis_data. It could be nested under "total_objects".
        objects_found = analysis_data.get("total_objects", {})
        if not objects_found and isinstance(analysis_data.get("tables"), int): # Fallback
            objects_found = analysis_data
        
        # Extract schema summary if available
        all_schemas_summary = analysis_data.get("schemas_summary", []) 

        context = {
            "source_type": source_db_type,
            "source_type_display": source_db_type.replace("_", " ").capitalize(),
            "generation_date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "connection_details": connection_details,
            "objects": objects_found,
            "all_schemas_summary": all_schemas_summary,
            **effort_data # Merges total_hours, complexity, object_efforts, risks, etc.
        }

        # Initialize Jinja2 environment
        # If template is read from a file, FileSystemLoader would be used.
        # For a string template, use DictLoader or just pass the string.
        env = jinja2.Environment(
            loader=jinja2.BaseLoader(), # Using BaseLoader for direct string loading
            autoescape=jinja2.select_autoescape(['html', 'xml', 'md'])
        )
        env.filters['_format_object_efforts'] = _format_object_efforts_filter
        env.filters['_format_list'] = _format_list_filter
        
        template = env.from_string(template_string)
        rendered_document = template.render(context)

        # Optionally, save the document to a Snowflake table or stage
        # e.g., session.sql(f"PUT 'file:///tmp/migration_doc.md' @MIGRATION_CALCULATOR_DB.STAGES.DOCUMENTS_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE").collect()
        # (Requires writing to a local temp file first if using PUT from SP, or insert into table)

        return rendered_document

    except json.JSONDecodeError as e:
        return json.dumps({"error": f"Invalid JSON input: {str(e)}"})
    except jinja2.TemplateError as e:
        return json.dumps({"error": f"Jinja2 template error: {str(e)}"})
    except Exception as e:
        return json.dumps({"error": f"An unexpected error occurred in generate_document_sp: {str(e)}"})


# Conceptual local testing (won't run directly in Snowflake this way)
# if __name__ == "__main__":
#     # Mock data for testing
#     mock_effort_json = json.dumps({
#         "total_hours": 150,
#         "complexity": "Medium",
#         "object_efforts": {"tables": {"count": 50, "hours_per_object": 2, "total_hours": 100}},
#         "risks": ["Sample risk 1", "Sample risk 2"],
#         "recommendations": ["Sample rec 1"],
#         "business_value_add": ["Sample value 1"]
#     })
#     mock_analysis_json = json.dumps({
#         "total_objects": {"tables": 50, "views": 20, "procedures": 10, "functions": 5},
#         "schemas_summary": [{"name": "PUBLIC", "tables": 50, "views": 20, "procedures": 10, "functions": 5}]
#     })
#     mock_conn_details_json = json.dumps({
#         "host": "myhost.example.com", "port": 1521, "database": "ORCL", "dbType": "oracle"
#     })
#     mock_source_db_type = "oracle"

#     # Read the template file content (ensure path is correct for local test)
#     try:
#         with open("../../templates/migration_document.md.j2", "r") as f:
#             mock_template_content = f.read()
#     except FileNotFoundError:
#         print("Error: Template file not found for local test. Create dummy template or adjust path.")
#         mock_template_content = """# {{ source_type_display }} Migration\nTotal Hours: {{ total_hours }}
# Risks:\n{{ risks | _format_list }} """ # Fixed: Removed trailing space that might cause issues


#     class MockSession: pass
#     mock_session = MockSession()

#     if mock_template_content:
#         document = generate_document_sp(
#             mock_session, 
#             mock_effort_json, 
#             mock_analysis_json,
#             mock_source_db_type, 
#             mock_conn_details_json,
#             mock_template_content
#         )
#         # Check if it's an error JSON
#         try:
#             error_check = json.loads(document)
#             if 'error' in error_check:
#                 print(f"\nSP Error Result:\n{json.dumps(error_check, indent=4)}")
#             else:
#                 print("\nGenerated Document (or unexpected JSON):")
#                 print(document)
#         except json.JSONDecodeError:
#             # Assume it's the rendered document
#             print("\nGenerated Document:")
#             print(document) 