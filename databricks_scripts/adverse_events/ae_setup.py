"""
Adverse Events Setup Script for Databricks

Converts SAS ae_setup.sas (lines 1-300) to PySpark.
Merges AE, DM, and EX domains to create base dataset for adverse events analysis.

Key functions:
1. Validate required variables exist
2. Determine which arm variable to use (ACTARM vs ARM)
3. Create safety population from DM + EX merge
4. Merge AE with safety population
5. Assign arm numbers

Usage in Databricks notebook:
    %run ./ae_setup
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Tuple
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loaders.databricks_adam_loader import DatabricksAdamLoader
from utils.ae_utils import get_arm_variable, create_safety_population


dbutils.widgets.text("source_type", "delta", "Data Source Type")
dbutils.widgets.text("catalog", "clinical_dev", "Catalog Name")
dbutils.widgets.text("schema", "sdtm", "Schema Name")
dbutils.widgets.text("file_path", "", "File Path (for XPT/SAS7BDAT)")
dbutils.widgets.text("output_catalog", "clinical_dev", "Output Catalog")
dbutils.widgets.text("output_schema", "results", "Output Schema")


def validate_required_variables(
    ae_df: DataFrame,
    dm_df: DataFrame,
    ex_df: DataFrame
) -> Tuple[bool, str]:
    """
    Validate that required variables exist in each domain.
    
    Equivalent to SAS ae_setup.sas lines 48-89.
    
    Args:
        ae_df: Adverse events DataFrame
        dm_df: Demographics DataFrame
        ex_df: Exposure DataFrame
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    errors = []
    
    ae_required = ["AEBODSYS", "AEDECOD", "USUBJID"]
    for var in ae_required:
        if var not in ae_df.columns:
            errors.append(f"AE: Missing required variable {var}")
    
    dm_required = ["USUBJID"]
    for var in dm_required:
        if var not in dm_df.columns:
            errors.append(f"DM: Missing required variable {var}")
    
    if "ACTARM" not in dm_df.columns and "ARM" not in dm_df.columns:
        errors.append("DM: Must have either ACTARM or ARM variable")
    
    ex_required = ["USUBJID"]
    for var in ex_required:
        if var not in ex_df.columns:
            errors.append(f"EX: Missing required variable {var}")
    
    if errors:
        return False, "; ".join(errors)
    
    return True, ""


def merge_ae_dm_ex(
    ae_df: DataFrame,
    dm_df: DataFrame,
    ex_df: DataFrame
) -> Tuple[DataFrame, DataFrame]:
    """
    Merge AE, DM, and EX domains to create base analysis dataset.
    
    Equivalent to SAS ae_setup.sas lines 120-300.
    
    Returns:
        Tuple of (base_dataset, arm_mapping)
    """
    is_valid, error_msg = validate_required_variables(ae_df, dm_df, ex_df)
    if not is_valid:
        raise ValueError(f"Variable validation failed: {error_msg}")
    
    arm_var = get_arm_variable(dm_df)
    safety_pop, arm_mapping = create_safety_population(dm_df, ex_df, arm_var)
    
    ae_base = ae_df.join(
        safety_pop.select("USUBJID", "ARM", "ARM_NUM"),
        on="USUBJID",
        how="inner"
    )
    
    return ae_base, arm_mapping


def create_base_dataset_by_subject_term(
    ae_base: DataFrame
) -> DataFrame:
    """
    Create dataset with one record per subject per term (USUBJID + AEDECOD).
    
    This is used for subject-level counting in aggregations.
    
    Args:
        ae_base: Base AE dataset with ARM_NUM
        
    Returns:
        DataFrame with one record per subject per term
    """
    base_by_subj_term = ae_base.select(
        "USUBJID", "ARM_NUM", "ARM", "AEBODSYS", "AEDECOD",
        *([col for col in ["AESER", "AESEV", "AETOXGR"] if col in ae_base.columns])
    ).distinct()
    
    return base_by_subj_term


def main():
    """Main execution function."""
    spark = SparkSession.builder.getOrCreate()
    
    source_type = dbutils.widgets.get("source_type")
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    file_path = dbutils.widgets.get("file_path") or None
    output_catalog = dbutils.widgets.get("output_catalog")
    output_schema = dbutils.widgets.get("output_schema")
    
    loader = DatabricksAdamLoader(
        spark=spark,
        source_type=source_type,
        catalog=catalog,
        schema=schema,
        file_path=file_path
    )
    
    print("Loading AE, DM, and EX domains...")
    ae_df = loader.load_domain("ae")
    dm_df = loader.load_domain("dm")
    ex_df = loader.load_domain("ex")
    
    print("Merging domains...")
    ae_base, arm_mapping = merge_ae_dm_ex(ae_df, dm_df, ex_df)
    
    print("Creating by-subject-term dataset...")
    base_by_subj_term = create_base_dataset_by_subject_term(ae_base)
    
    output_loader = DatabricksAdamLoader(
        spark=spark,
        source_type="delta",
        catalog=output_catalog,
        schema=output_schema
    )
    
    print("Writing base datasets...")
    output_loader.write_delta_table(
        ae_base,
        "ae_base",
        mode="overwrite"
    )
    
    output_loader.write_delta_table(
        base_by_subj_term,
        "ae_base_by_subj_term",
        mode="overwrite"
    )
    
    output_loader.write_delta_table(
        arm_mapping,
        "arm_mapping",
        mode="overwrite"
    )
    
    print("Setup complete!")
    print(f"Results written to {output_catalog}.{output_schema}")
    print("\nArm Summary:")
    arm_mapping.show()
    
    return ae_base, arm_mapping


if __name__ == "__main__":
    ae_base, arm_mapping = main()
    display(ae_base)
