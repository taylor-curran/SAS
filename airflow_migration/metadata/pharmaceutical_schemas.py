"""
Pharmaceutical Data Schema Definitions for OpenMetadata

This module defines the metadata schemas for clinical trial datasets
following CDISC standards (SDTM and ADaM).
"""

from typing import Dict, List

SDTM_SCHEMAS = {
    "lb": {
        "description": "Laboratory Test Results",
        "domain": "Laboratory",
        "standard": "CDISC SDTM",
        "columns": [
            {"name": "studyid", "type": "string", "description": "Study Identifier"},
            {"name": "usubjid", "type": "string", "description": "Unique Subject Identifier"},
            {"name": "lbtest", "type": "string", "description": "Laboratory Test Name"},
            {"name": "lborres", "type": "string", "description": "Result or Finding in Original Units"},
            {"name": "lborresu", "type": "string", "description": "Original Units"},
            {"name": "lbstresc", "type": "string", "description": "Character Result/Finding in Std Format"},
            {"name": "lbstresn", "type": "numeric", "description": "Numeric Result/Finding in Standard Units"},
            {"name": "lbstresu", "type": "string", "description": "Standard Units"},
            {"name": "visitnum", "type": "numeric", "description": "Visit Number"},
            {"name": "visit", "type": "string", "description": "Visit Name"},
        ]
    }
}

ADAM_SCHEMAS = {
    "adlbc": {
        "description": "Analysis Dataset for Laboratory Data Chemistry",
        "domain": "Laboratory Chemistry",
        "standard": "CDISC ADaM",
        "columns": [
            {"name": "studyid", "type": "string", "description": "Study Identifier"},
            {"name": "usubjid", "type": "string", "description": "Unique Subject Identifier"},
            {"name": "param", "type": "string", "description": "Parameter"},
            {"name": "paramcd", "type": "string", "description": "Parameter Code"},
            {"name": "aval", "type": "numeric", "description": "Analysis Value"},
            {"name": "avalu", "type": "string", "description": "Analysis Value Unit"},
            {"name": "base", "type": "numeric", "description": "Baseline Value"},
            {"name": "chg", "type": "numeric", "description": "Change from Baseline"},
            {"name": "pchg", "type": "numeric", "description": "Percent Change from Baseline"},
            {"name": "trtp", "type": "string", "description": "Planned Treatment"},
            {"name": "trtpn", "type": "numeric", "description": "Planned Treatment (N)"},
            {"name": "avisitn", "type": "numeric", "description": "Analysis Visit (N)"},
            {"name": "avisit", "type": "string", "description": "Analysis Visit"},
            {"name": "saffl", "type": "string", "description": "Safety Population Flag"},
            {"name": "anl01fl", "type": "string", "description": "Analysis Flag 01"},
        ]
    }
}

PROCESSED_SCHEMAS = {
    "lab_data_filtered": {
        "description": "Filtered Laboratory Data for Analysis",
        "domain": "Processed Laboratory Data",
        "standard": "Internal",
        "columns": [
            {"name": "studyid", "type": "string", "description": "Study Identifier"},
            {"name": "usubjid", "type": "string", "description": "Unique Subject Identifier"},
            {"name": "param", "type": "string", "description": "Parameter"},
            {"name": "aval", "type": "numeric", "description": "Analysis Value"},
            {"name": "trtp", "type": "string", "description": "Planned Treatment"},
            {"name": "avisit", "type": "string", "description": "Analysis Visit"},
            {"name": "outlier_flag", "type": "string", "description": "Outlier Detection Flag"},
        ]
    },
    "outliers_detected": {
        "description": "Detected Outliers in Laboratory Data",
        "domain": "Data Quality",
        "standard": "Internal",
        "columns": [
            {"name": "usubjid", "type": "string", "description": "Unique Subject Identifier"},
            {"name": "param", "type": "string", "description": "Parameter"},
            {"name": "aval", "type": "numeric", "description": "Analysis Value"},
            {"name": "outlier_type", "type": "string", "description": "Type of Outlier"},
            {"name": "outlier_score", "type": "numeric", "description": "Outlier Score"},
        ]
    }
}

ANALYSIS_SCHEMAS = {
    "summary_statistics": {
        "description": "Summary Statistics for Laboratory Parameters",
        "domain": "Statistical Analysis",
        "standard": "Internal",
        "columns": [
            {"name": "param", "type": "string", "description": "Parameter"},
            {"name": "trtp", "type": "string", "description": "Treatment"},
            {"name": "avisit", "type": "string", "description": "Visit"},
            {"name": "n", "type": "numeric", "description": "Number of Observations"},
            {"name": "mean", "type": "numeric", "description": "Mean Value"},
            {"name": "std", "type": "numeric", "description": "Standard Deviation"},
            {"name": "median", "type": "numeric", "description": "Median Value"},
            {"name": "q1", "type": "numeric", "description": "First Quartile"},
            {"name": "q3", "type": "numeric", "description": "Third Quartile"},
        ]
    },
    "outlier_analysis": {
        "description": "Outlier Analysis Results",
        "domain": "Statistical Analysis",
        "standard": "Internal",
        "columns": [
            {"name": "param", "type": "string", "description": "Parameter"},
            {"name": "total_subjects", "type": "numeric", "description": "Total Number of Subjects"},
            {"name": "outlier_count", "type": "numeric", "description": "Number of Outliers"},
            {"name": "outlier_percentage", "type": "numeric", "description": "Percentage of Outliers"},
        ]
    }
}

REPORT_SCHEMAS = {
    "box_plots": {
        "description": "Box Plot Visualizations",
        "domain": "Reporting",
        "standard": "Internal",
        "columns": [
            {"name": "plot_id", "type": "string", "description": "Plot Identifier"},
            {"name": "param", "type": "string", "description": "Parameter"},
            {"name": "file_path", "type": "string", "description": "Plot File Path"},
            {"name": "created_date", "type": "timestamp", "description": "Creation Date"},
        ]
    },
    "statistical_summaries": {
        "description": "Statistical Summary Reports",
        "domain": "Reporting",
        "standard": "Internal",
        "columns": [
            {"name": "report_id", "type": "string", "description": "Report Identifier"},
            {"name": "report_type", "type": "string", "description": "Type of Report"},
            {"name": "file_path", "type": "string", "description": "Report File Path"},
            {"name": "created_date", "type": "timestamp", "description": "Creation Date"},
        ]
    }
}

def get_all_schemas() -> Dict[str, Dict]:
    """Return all schema definitions organized by domain."""
    return {
        "sdtm": SDTM_SCHEMAS,
        "adam": ADAM_SCHEMAS,
        "processed": PROCESSED_SCHEMAS,
        "analysis": ANALYSIS_SCHEMAS,
        "reports": REPORT_SCHEMAS
    }

def get_schema_by_table(table_name: str) -> Dict:
    """Get schema definition for a specific table."""
    all_schemas = get_all_schemas()
    for domain, schemas in all_schemas.items():
        if table_name in schemas:
            return schemas[table_name]
    return None
