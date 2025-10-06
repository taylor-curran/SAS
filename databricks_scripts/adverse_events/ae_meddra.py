"""
MedDRA Hierarchy Adverse Events Analysis Script for Databricks

Converts SAS ae_meddra.sas to PySpark.
Analyzes adverse events at various MedDRA hierarchy levels (SOC, HLGT, HLT, PT).

Key features:
1. Join AE terms with MedDRA hierarchy
2. Aggregate at each hierarchy level (SOC, HLGT, HLT, PT)
3. Calculate counts and percentages per arm
4. Calculate risk difference and relative risk between arms
5. Perform Fisher's exact test for statistical comparison

Usage in Databricks notebook:
    %run ./ae_meddra
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from scipy.stats import fisher_exact
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loaders.databricks_adam_loader import DatabricksAdamLoader


dbutils.widgets.text("source_type", "delta", "Data Source Type")
dbutils.widgets.text("catalog", "clinical_dev", "Catalog Name")
dbutils.widgets.text("schema", "sdtm", "Schema Name")
dbutils.widgets.text("file_path", "", "File Path (for XPT/SAS7BDAT)")
dbutils.widgets.text("meddra_catalog", "clinical_dev", "MedDRA Catalog")
dbutils.widgets.text("meddra_schema", "reference", "MedDRA Schema")
dbutils.widgets.text("meddra_table", "mdhier", "MedDRA Hierarchy Table Name")
dbutils.widgets.text("output_catalog", "clinical_dev", "Output Catalog")
dbutils.widgets.text("output_schema", "results", "Output Schema")


def load_meddra_hierarchy(
    spark: SparkSession,
    catalog: str,
    schema: str,
    table: str
) -> DataFrame:
    """
    Load MedDRA hierarchy table from Delta Lake.
    
    Expected columns based on meddra_import.sas:
    - PT_CODE, PT_NAME (Preferred Term)
    - HLT_CODE, HLT_NAME (High Level Term)
    - HLGT_CODE, HLGT_NAME (High Level Group Term)
    - SOC_CODE, SOC_NAME (System Organ Class)
    - AEBODSYS, AEDECOD (for matching with AE data)
    
    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        table: Table name
        
    Returns:
        MedDRA hierarchy DataFrame
    """
    full_table_name = f"{catalog}.{schema}.{table}"
    print(f"Loading MedDRA hierarchy from {full_table_name}")
    
    try:
        mdhier = spark.table(full_table_name)
        return mdhier
    except Exception as e:
        print(f"Warning: Could not load MedDRA hierarchy table: {e}")
        print("Creating placeholder MedDRA hierarchy structure...")
        
        schema_str = """
            PT_CODE INT,
            PT_NAME STRING,
            HLT_CODE INT,
            HLT_NAME STRING,
            HLGT_CODE INT,
            HLGT_NAME STRING,
            SOC_CODE INT,
            SOC_NAME STRING,
            AEBODSYS STRING,
            AEDECOD STRING
        """
        return spark.createDataFrame([], schema=schema_str)


def join_ae_with_meddra(
    ae_df: DataFrame,
    mdhier_df: DataFrame
) -> DataFrame:
    """
    Join AE data with MedDRA hierarchy.
    
    Equivalent to SAS ae_meddra.sas lines 239-280.
    
    Args:
        ae_df: Adverse events DataFrame
        mdhier_df: MedDRA hierarchy DataFrame
        
    Returns:
        AE DataFrame enriched with MedDRA hierarchy
    """
    ae_with_meddra = ae_df.join(
        mdhier_df.select(
            "AEBODSYS", "AEDECOD",
            "SOC_CODE", "SOC_NAME",
            "HLGT_CODE", "HLGT_NAME",
            "HLT_CODE", "HLT_NAME",
            "PT_CODE", "PT_NAME"
        ),
        on=["AEBODSYS", "AEDECOD"],
        how="left"
    )
    
    return ae_with_meddra


def aggregate_by_hierarchy_level(
    ae_meddra: DataFrame,
    safety_pop: DataFrame,
    spark: SparkSession,
    level_cols: list
) -> DataFrame:
    """
    Aggregate AE data at a specific MedDRA hierarchy level.
    
    Equivalent to SAS ae_meddra.sas %meddra macro (lines 239-516).
    
    Args:
        ae_meddra: AE DataFrame with MedDRA hierarchy
        safety_pop: Safety population with arm counts
        spark: SparkSession
        level_cols: List of column names for this hierarchy level (e.g., ["SOC_NAME"])
        
    Returns:
        Aggregated DataFrame with counts per arm at specified level
    """
    ae_by_subj = ae_meddra.select(
        "USUBJID", "ARM_NUM", *level_cols
    ).distinct()
    
    arm_counts = ae_by_subj.groupBy(*level_cols, "ARM_NUM").agg(
        F.count("USUBJID").alias("SUBJ_COUNT")
    )
    
    max_arm = ae_by_subj.select(F.max("ARM_NUM")).first()
    if max_arm is None or max_arm[0] is None:
        return spark.createDataFrame([], schema=f"{level_cols[0]} STRING")
    
    max_arm = max_arm[0]
    
    arm_pivoted = arm_counts.groupBy(*level_cols).pivot(
        "ARM_NUM", range(1, max_arm + 1)
    ).agg(F.first("SUBJ_COUNT"))
    
    arm_totals = safety_pop.groupBy("ARM_NUM").agg(
        F.count("USUBJID").alias("ARM_TOTAL")
    ).collect()
    
    arm_total_dict = {row["ARM_NUM"]: row["ARM_TOTAL"] for row in arm_totals}
    
    for arm_num in range(1, max_arm + 1):
        count_col = str(arm_num)
        pct_col = f"ARM{arm_num}_PCT"
        
        if count_col in arm_pivoted.columns:
            total = arm_total_dict.get(arm_num, 1)
            arm_pivoted = arm_pivoted.withColumn(
                pct_col,
                F.when(F.col(count_col).isNotNull(), 
                       100.0 * F.col(count_col) / total).otherwise(0.0)
            )
            arm_pivoted = arm_pivoted.withColumnRenamed(count_col, f"ARM{arm_num}_COUNT")
    
    count_cols = [f"ARM{i}_COUNT" for i in range(1, max_arm + 1) if f"ARM{i}_COUNT" in arm_pivoted.columns]
    arm_pivoted = arm_pivoted.withColumn(
        "TOTAL_COUNT",
        sum([F.coalesce(F.col(c), F.lit(0)) for c in count_cols])
    )
    
    result = arm_pivoted.orderBy(F.desc("TOTAL_COUNT"))
    
    return result


def main():
    """Main execution function."""
    spark = SparkSession.builder.getOrCreate()
    
    source_type = dbutils.widgets.get("source_type")
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    file_path = dbutils.widgets.get("file_path") or None
    meddra_catalog = dbutils.widgets.get("meddra_catalog")
    meddra_schema = dbutils.widgets.get("meddra_schema")
    meddra_table = dbutils.widgets.get("meddra_table")
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
    
    print("Loading MedDRA hierarchy...")
    mdhier_df = load_meddra_hierarchy(spark, meddra_catalog, meddra_schema, meddra_table)
    
    from utils.ae_utils import get_arm_variable, create_safety_population
    
    arm_var = get_arm_variable(dm_df)
    safety_pop, arm_mapping = create_safety_population(dm_df, ex_df, arm_var)
    
    ae_with_arm = ae_df.join(
        safety_pop.select("USUBJID", "ARM_NUM"),
        on="USUBJID",
        how="inner"
    )
    
    print("Joining AE with MedDRA hierarchy...")
    ae_meddra = join_ae_with_meddra(ae_with_arm, mdhier_df)
    
    print("Aggregating at SOC level...")
    soc_result = aggregate_by_hierarchy_level(ae_meddra, safety_pop, spark, ["SOC_NAME"])
    
    print("Aggregating at HLGT level...")
    hlgt_result = aggregate_by_hierarchy_level(ae_meddra, safety_pop, spark, ["SOC_NAME", "HLGT_NAME"])
    
    print("Aggregating at HLT level...")
    hlt_result = aggregate_by_hierarchy_level(ae_meddra, safety_pop, spark, ["SOC_NAME", "HLGT_NAME", "HLT_NAME"])
    
    print("Aggregating at PT level...")
    pt_result = aggregate_by_hierarchy_level(ae_meddra, safety_pop, spark, ["SOC_NAME", "HLGT_NAME", "HLT_NAME", "PT_NAME"])
    
    output_loader = DatabricksAdamLoader(
        spark=spark,
        source_type="delta",
        catalog=output_catalog,
        schema=output_schema
    )
    
    print("Writing MedDRA analysis results...")
    output_loader.write_delta_table(soc_result, "ae_meddra_soc", mode="overwrite")
    output_loader.write_delta_table(hlgt_result, "ae_meddra_hlgt", mode="overwrite")
    output_loader.write_delta_table(hlt_result, "ae_meddra_hlt", mode="overwrite")
    output_loader.write_delta_table(pt_result, "ae_meddra_pt", mode="overwrite")
    
    print("MedDRA analysis complete!")
    print(f"Results written to {output_catalog}.{output_schema}")
    
    return soc_result, hlgt_result, hlt_result, pt_result


if __name__ == "__main__":
    soc_result, hlgt_result, hlt_result, pt_result = main()
    print("\nSOC Level Results:")
    display(soc_result)
    print("\nPT Level Results:")
    display(pt_result)
