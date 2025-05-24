from typing import Dict, Any, List

# --- Constants (Mirrors EffortCalculator class attributes) ---
EFFORT_MULTIPLIERS = {
    "tables": 2,      # 2 hours per table
    "views": 1,       # 1 hour per view
    "procedures": 4,  # 4 hours per procedure
    "functions": 3    # 3 hours per function
    # Potentially add other object types if they become relevant for Teradata/Databricks
}

COMPLEXITY_THRESHOLDS = {
    "Low": 50,      # Less than 50 hours
    "Medium": 100,  # 50-100 hours
    "High": 200     # More than 100 hours
}

# --- Helper Functions (Ported from EffortCalculator methods) ---

def generate_business_value_add(source_type: str) -> List[str]:
    """Generate dynamic business value points for migrating to Snowflake from a specific source."""
    common_snowflake_benefits = [
        "Enhanced Scalability & Performance: Independently scale compute and storage for optimal performance with any data volume, potentially reducing query times by X-Y% for key workloads.",
        "Simplified Data Management: Consolidate data, streamline administration, and improve data governance, possibly reducing DBA overhead by Z hours/month.",
        "Faster Analytics & Insights: Accelerate data-driven decisions with high-performance SQL queries, enabling new analytical use cases previously unfeasible.",
        "Direct Cost Savings (TCO): Potential for lower Total Cost of Ownership through pay-per-second compute and separate competitive storage pricing. Estimate potential savings of A-B% annually compared to current infrastructure/licensing.",
        "Optimized Resource Usage: Eliminate over-provisioning with precise resource allocation, reducing compute and storage wastage, potentially reclaiming C-D% of current spend.",
        "Seamless Data Sharing: Securely share live data internally and externally without ETL, fostering collaboration and new data monetization opportunities.",
        "Reduced Operational Overhead: Fully managed SaaS minimizes administrative tasks (patching, upgrades, backups), freeing up IT resources for value-added activities, contributing to E-F% operational cost reduction."
    ]

    source_specific_benefits = []

    if source_type == "oracle":
        source_specific_benefits = [
            "Significant reduction in licensing costs: Transition from Oracle's expensive proprietary licenses (potentially saving G-H% of annual Oracle spend) and complex audit processes.",
            "Move away from complex and costly Oracle-specific features like RAC towards Snowflake's simpler, elastic architecture, reducing specialized skill dependency.",
            "Improved agility and faster time-to-market for new data initiatives by leveraging Snowflake's cloud-native capabilities and CI/CD-friendly environment."
        ]
    elif source_type == "sqlserver":
        source_specific_benefits = [
            "Eliminate SQL Server licensing costs (especially for Enterprise features) and reduce dependency on Microsoft-specific ecosystem, leading to I-J% license cost savings.",
            "Transition from potentially restrictive on-premises hardware to Snowflake's flexible cloud infrastructure, reducing capital expenditure (CapEx).",
            "Gain access to broader data integration and analytics capabilities beyond the traditional SQL Server stack, supporting modern data science and ML workloads."
        ]
    elif source_type == "postgresql":
        source_specific_benefits = [
            "Achieve higher levels of scalability and concurrency often challenging with self-managed or even some managed PostgreSQL instances, supporting K-L times more concurrent users/queries.",
            "Reduce operational burden of managing, tuning, vacuuming, and upgrading PostgreSQL databases, potentially saving M-N hours/week of specialized DBA time.",
            "Leverage Snowflake's robust security and governance features (e.g., dynamic data masking, row-level security out-of-the-box) which may require more manual setup or commercial extensions in PostgreSQL, potentially reducing compliance costs by O-P%."
        ]
    elif source_type == "teradata":
        source_specific_benefits = [
            "Modernize from a legacy, often expensive, Teradata appliance to a flexible cloud-native architecture, avoiding costly hardware refresh cycles (saving Q-R $M).",
            "Significant cost savings by moving away from Teradata's hardware and software licensing model, potentially reducing annual spend by S-T%.",
            "Improve data democratizaion and support for diverse workloads (Data Engineering, Data Science, Ad-hoc analysis) beyond traditional BI, unlocking new revenue streams or operational efficiencies of U-V%."
        ]
    elif source_type == "databricks": # Assuming migration *from* Databricks DWH/SQL capabilities to Snowflake
         source_specific_benefits = [
            "Simplify data warehousing and BI workloads with Snowflake's optimized SQL engine and user-friendly interface for analysts, potentially improving analyst productivity by W-X%.",
            "Potentially reduce TCO by consolidating data lake and data warehouse capabilities within Snowflake where appropriate, leveraging its storage and compute pricing models, potentially saving Y-Z% on specific workloads.",
            "Benefit from Snowflake's strong governance, security (e.g., granular access controls, end-to-end encryption), and data sharing features for structured and semi-structured data, simplifying compliance."
        ]
    elif source_type == "snowflake": # Consolidating Snowflake accounts or re-platforming
         source_specific_benefits = [
            "Consolidate multiple Snowflake accounts for better cost management (e.g., volume discounts, optimized warehouse usage) and centralized governance, potentially reducing overall Snowflake spend by AA-BB%.",
            "Optimize existing Snowflake workloads by re-evaluating warehouse sizing, query performance, and data clustering strategies, improving cost-efficiency by CC-DD% for targeted query patterns.",
            "Leverage latest Snowflake features (e.g., Snowpark, Unistore, Native Apps) and best practices for enhanced performance and new capabilities in the target account."
        ]
    else: # Generic legacy system
        source_specific_benefits = [
            "Modernize your data stack by moving from a potentially end-of-life or unsupported legacy system to a leading cloud data platform, reducing risk and improving access to innovation.",
            "Improve data accessibility and self-service capabilities for business users, potentially reducing report generation time by EE-FF%."
        ]
    
    # Combine benefits: Prioritize source-specific ones, then add common ones to fill up to a certain number (e.g., 7 total)
    # For example, take top 3 specific and top 4 common, or adjust as needed.
    combined_benefits = source_specific_benefits[:3] + [b for b in common_snowflake_benefits if b not in source_specific_benefits][:4]
    
    return combined_benefits

def generate_risks(source_type: str, target_type: str, complexity: str) -> List[str]:
    """Generate potential risks based on source, target, and complexity."""
    risks = [
        "Data loss during migration",
        "Potential downtime during cutover",
        "Performance impact during migration",
        "Data type compatibility issues",
        "Function and procedure syntax differences"
    ]

    if source_type == "oracle":
        risks.extend([
            "PL/SQL to Snowflake SQL conversion complexity",
            "Oracle-specific features (e.g., AQ, Spatial) may not have direct equivalents",
            "Sequence and identity column handling differences"
        ])
    elif source_type == "sqlserver":
        risks.extend([
            "T-SQL to Snowflake SQL conversion complexity",
            "SQL Server-specific features (e.g., linked servers, Service Broker) may not have direct equivalents",
            "Identity column and sequence handling differences"
        ])
    elif source_type == "postgresql":
        risks.extend([
            "PostgreSQL-specific data types (e.g., PostGIS) may need complex conversion or workarounds",
            "PL/pgSQL to Snowflake SQL/JavaScript UDF conversion",
            "Extension and custom function compatibility and rewrite effort"
        ])
    elif source_type == "teradata":
        risks.extend([
            "BTEQ/TPT script conversion to SnowSQL/Python scripts or ETL tools.",
            "Teradata-specific SQL extensions and utilities (e.g., FastLoad, MultiLoad) require redesign.",
            "Handling of Teradata-specific data types and indexing strategies (e.g., PPI)."
        ])
    elif source_type == "databricks":
        risks.extend([
            "Converting Delta Lake specific features (e.g., time travel, MERGE on Delta) to Snowflake equivalents.",
            "Rewriting Spark SQL/Python/Scala UDFs and jobs for Snowflake (SQL, Snowpark, or external compute).",
            "Managing data consistency and schema evolution differences between Delta Lake and Snowflake tables."
        ])

    if complexity == "High":
        risks.extend([
            "Extended migration timeline and higher resource allocation",
            "Increased complexity in data validation and reconciliation",
            "More intricate rollback procedures and contingency planning"
        ])
    return risks

def generate_recommendations(source_type: str, target_type: str, complexity: str) -> List[str]:
    """Generate recommendations based on source, target, and complexity."""
    recommendations = [
        "Create comprehensive backup of source data before migration begins.",
        "Implement a dedicated testing environment for thorough migration validation.",
        "Document all custom functions, procedures, and complex business logic from the source.",
        "Plan for an adequate downtime window for the final cutover, or explore phased migration.",
        "Develop and test a detailed rollback plan in case of critical issues."
    ]

    if source_type == "oracle":
        recommendations.extend([
            "Utilize schema conversion tools and thoroughly review Oracle-specific features for Snowflake compatibility.",
            "Focus on PL/SQL to Snowflake SQL/JavaScript UDF conversion strategy and testing.",
            "Address sequence generation and identity column behavior early in the planning phase."
        ])
    elif source_type == "sqlserver":
        recommendations.extend([
            "Analyze T-SQL code for constructs that need rewriting for Snowflake SQL.",
            "Plan for handling SQL Server Agent jobs and SSIS packages, potentially migrating to Snowflake tasks or other ETL tools.",
            "Document and test identity column and sequence migration strategies."
        ])
    elif source_type == "postgresql":
        recommendations.extend([
            "Carefully map PostgreSQL data types and extensions to Snowflake equivalents or alternatives.",
            "Develop a strategy for converting PL/pgSQL functions and procedures.",
            "Assess and plan for migration of custom functions and any heavily used PostgreSQL extensions."
        ])
    elif source_type == "teradata":
        recommendations.extend([
            "Plan for Teradata script (BTEQ, FastLoad, etc.) conversion or replacement with Snowflake-compatible tools.",
            "Analyze Teradata SQL for proprietary features and plan for rewrite.",
            "Develop a data migration strategy focusing on efficient export from Teradata and import into Snowflake (e.g., using Snowpipe or COPY command)."
        ])
    elif source_type == "databricks":
        recommendations.extend([
            "Identify and plan for migrating Delta Lake table features and optimizations to Snowflake.",
            "Assess Spark jobs for logic that can be translated to Snowpark or Snowflake SQL, or determine if external processing is still needed.",
            "Define a strategy for data extraction from Delta Lake (e.g., Parquet export) and ingestion into Snowflake."
        ])

    if complexity == "High":
        recommendations.extend([
            "Strongly consider a phased migration approach, starting with less critical workloads.",
            "Implement comprehensive automated testing and data validation suites.",
            "Establish clear communication channels and a dedicated migration team for complex projects."
        ])
    return recommendations

def calculate_migration_effort(source_type: str, target_type: str, objects: Dict[str, int]) -> Dict[str, Any]:
    """
    Calculate migration effort, complexity, risks, and recommendations.
    This function will be called by the main Snowpark Stored Procedure.
    """
    object_efforts: Dict[str, Any] = {}
    total_hours = 0

    for obj_type, count in objects.items():
        if obj_type in EFFORT_MULTIPLIERS:
            effort = count * EFFORT_MULTIPLIERS[obj_type]
            object_efforts[obj_type] = {
                "count": count,
                "hours_per_object": EFFORT_MULTIPLIERS[obj_type],
                "total_hours": effort
            }
            total_hours += effort

    complexity = "Low"
    if total_hours >= COMPLEXITY_THRESHOLDS["High"]:
        complexity = "High"
    elif total_hours >= COMPLEXITY_THRESHOLDS["Medium"]:
        complexity = "Medium"

    risks_list = generate_risks(source_type, target_type, complexity)
    recommendations_list = generate_recommendations(source_type, target_type, complexity)
    business_value_list = generate_business_value_add(source_type)

    return {
        "total_hours": total_hours,
        "complexity": complexity,
        "object_efforts": object_efforts,
        "risks": risks_list,
        "recommendations": recommendations_list,
        "business_value_add": business_value_list
    }

# Example usage (for local testing if needed):
# if __name__ == '__main__':
#     sample_objects = {"tables": 100, "views": 50, "procedures": 20, "functions": 10}
#     results = calculate_migration_effort("oracle", "snowflake", sample_objects)
#     import json
#     print(json.dumps(results, indent=4))
