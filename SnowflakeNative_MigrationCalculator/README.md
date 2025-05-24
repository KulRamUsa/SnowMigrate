# Snowflake-Native Migration Effort Calculator

This project is a version of the Migration Effort Calculator built to run natively within the Snowflake ecosystem.

## Overview

The application consists of:
- A **Streamlit in Snowflake** frontend for user interaction.
- **Snowpark (Python) Stored Procedures and UDFs** for backend logic, including:
    - Connecting to external source databases (Oracle, SQL Server, PostgreSQL, Teradata, Databricks).
    - Analyzing source database metadata (object counts).
    - Calculating migration effort, complexity, risks, and recommendations.
    - Generating source-specific business value propositions.
    - Creating a preliminary migration strategy document in Markdown.

## Components

- `streamlit_app/`: Contains the Streamlit UI code.
- `snowpark_logic/`: Houses the Snowpark Python code for backend processing.
- `templates/`: Jinja2 templates for document generation.
- `snowflake_setup/`: SQL scripts for setting up necessary Snowflake objects (database, schema, warehouse, roles, network rules, secrets, deploying Snowpark objects, and the Streamlit app).

## Prerequisites

- Access to a Snowflake account with appropriate permissions to create objects, network rules, secrets, and deploy Streamlit/Snowpark applications.
- Python database drivers for the source systems you intend to connect to (must be available in Snowflake's Anaconda channel or uploaded as packages).
- Network connectivity from Snowflake (via External Network Access) to the source databases.

*(Further setup and deployment instructions will be added here.)*
