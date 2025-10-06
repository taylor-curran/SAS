"""
Adverse Events Aggregation Script for Databricks

Converts SAS ae_aggregate.sas to PySpark.
Performs two main aggregations:
1. Analysis A/B: Adverse events by preferred term and system organ class
2. Analysis C/D: Adverse events by severity level

Usage in Databricks notebook:
    %run ./ae_aggregate
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loaders.databricks_adam_loader import DatabricksAdamLoader
from utils.ae_utils import map_severity_to_numeric


dbutils.widgets.text("source_type", "delta", "Data Source Type (delta/xpt/sas7bdat)")
dbutils.widgets.text("catalog", "clinical_dev", "Catalog Name")
dbutils.widgets.text("schema", "sdtm", "Schema Name")
dbutils.widgets.text("file_path", "", "File Path (for XPT/SAS7BDAT sources)")
dbutils.widgets.text("output_catalog", "clinical_dev", "Output Catalog")
dbutils.widgets.text("output_schema", "results", "Output Schema")
dbutils.widgets.dropdown("serious_only", "No", ["No", "Yes"], "Filter to Serious AEs Only")
dbutils.widgets.text("percent_threshold", "2.0", "Percentage Threshold for Analysis A")


def aggregate_by_preferred_term(
    ae_df: DataFrame,
    safety_pop: DataFrame,
    serious_only: bool = False,
    pct_threshold: float = 2.0
) -> DataFrame:
    """
    Aggregate adverse events by preferred term (AEDECOD) and SOC (AEBODSYS).
    
    Equivalent to SAS %ab macro (lines 9-72 of ae_aggregate.sas).
    
    Args:
        ae_df: Adverse events DataFrame with ARM_NUM
        safety_pop: Safety population with arm counts
        serious_only: If True, filter to AESER='Y' (Analysis B vs A)
        pct_threshold: Minimum percentage threshold (for Analysis A only)
        
    Returns:
        Aggregated DataFrame with counts and percentages per arm
    """
    if serious_only and "AESER" in ae_df.columns:
        ae_df = ae_df.filter(F.col("AESER") == "Y")
    
    ae_by_subj = ae_df.select(
        "USUBJID", "ARM_NUM", "AEBODSYS", "AEDECOD"
    ).distinct()
    
    arm_counts = ae_by_subj.groupBy("AEBODSYS", "AEDECOD", "ARM_NUM").agg(
        F.count("USUBJID").alias("SUBJ_COUNT")
    )
    
    max_arm = ae_by_subj.select(F.max("ARM_NUM")).first()[0]
    
    arm_pivoted = arm_counts.groupBy("AEBODSYS", "AEDECOD").pivot(
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
    
    count_cols = [str(i) for i in range(1, max_arm + 1) if str(i) in arm_pivoted.columns]
    arm_pivoted = arm_pivoted.withColumn(
        "TOTAL_COUNT",
        sum([F.coalesce(F.col(c), F.lit(0)) for c in count_cols])
    )
    
    total_subjects = sum(arm_total_dict.values())
    arm_pivoted = arm_pivoted.withColumn(
        "TOTAL_PCT",
        100.0 * F.col("TOTAL_COUNT") / total_subjects
    )
    
    if not serious_only and pct_threshold > 0:
        pct_cols = [f"ARM{i}_PCT" for i in range(1, max_arm + 1)]
        threshold_condition = None
        for col in pct_cols:
            if col in arm_pivoted.columns:
                if threshold_condition is None:
                    threshold_condition = (F.col(col) > pct_threshold)
                else:
                    threshold_condition = threshold_condition | (F.col(col) > pct_threshold)
        
        if threshold_condition is not None:
            arm_pivoted = arm_pivoted.filter(threshold_condition)
    
    result = arm_pivoted.orderBy("AEBODSYS", F.desc("TOTAL_PCT"))
    
    return result


def aggregate_by_severity(
    ae_df: DataFrame,
    safety_pop: DataFrame,
    spark: SparkSession,
    serious_only: bool = False
) -> DataFrame:
    """
    Aggregate adverse events by severity level (AESEV).
    
    Equivalent to SAS %cd macro (lines 77-251 of ae_aggregate.sas).
    
    Args:
        ae_df: Adverse events DataFrame
        safety_pop: Safety population with arm counts
        spark: SparkSession
        serious_only: If True, filter to AESER='Y' (Analysis D vs C)
        
    Returns:
        Aggregated DataFrame with counts per severity level per arm
    """
    if serious_only and "AESER" in ae_df.columns:
        ae_df = ae_df.filter(F.col("AESER") == "Y")
    
    if "AESEV" not in ae_df.columns:
        print("Warning: AESEV column not found, returning empty DataFrame")
        return spark.createDataFrame([], schema="AEBODSYS STRING, AEDECOD STRING")
    
    ae_with_sev_num = ae_df.withColumn(
        "SEV_NUM",
        map_severity_to_numeric("AESEV")
    )
    
    window_spec = Window.partitionBy("USUBJID", "AEBODSYS", "AEDECOD").orderBy(F.desc("SEV_NUM"))
    
    ae_highest_sev = ae_with_sev_num.withColumn(
        "rank",
        F.row_number().over(window_spec)
    ).filter(F.col("rank") == 1).drop("rank")
    
    severity_counts = ae_highest_sev.groupBy(
        "AEBODSYS", "AEDECOD", "ARM_NUM", "SEV_NUM"
    ).agg(
        F.count("USUBJID").alias("COUNT")
    )
    
    max_arm = ae_highest_sev.select(F.max("ARM_NUM")).first()[0]
    
    result = None
    for arm_num in range(1, max_arm + 1):
        arm_data = severity_counts.filter(F.col("ARM_NUM") == arm_num)
        arm_pivot = arm_data.groupBy("AEBODSYS", "AEDECOD").pivot(
            "SEV_NUM", [1, 2, 3, 4, 5, 100]
        ).agg(F.first("COUNT"))
        
        for sev in [1, 2, 3, 4, 5, 100]:
            old_name = str(sev)
            new_name = f"ARM{arm_num}_SEV{sev}" if sev != 100 else f"ARM{arm_num}_MISSING"
            if old_name in arm_pivot.columns:
                arm_pivot = arm_pivot.withColumnRenamed(old_name, new_name)
        
        if result is None:
            result = arm_pivot
        else:
            result = result.join(arm_pivot, on=["AEBODSYS", "AEDECOD"], how="outer")
    
    sev_cols = [c for c in result.columns if c.startswith("ARM") and "_SEV" in c]
    result = result.withColumn(
        "TOTAL_COUNT",
        sum([F.coalesce(F.col(c), F.lit(0)) for c in sev_cols])
    )
    
    result = result.orderBy(F.desc("TOTAL_COUNT"))
    
    return result


def main():
    """Main execution function."""
    spark = SparkSession.builder.getOrCreate()
    
    source_type = dbutils.widgets.get("source_type")
    catalog = dbutils.widgets.get("catalog")
    schema = dbutils.widgets.get("schema")
    file_path = dbutils.widgets.get("file_path") or None
    output_catalog = dbutils.widgets.get("output_catalog")
    output_schema = dbutils.widgets.get("output_schema")
    serious_only = dbutils.widgets.get("serious_only") == "Yes"
    pct_threshold = float(dbutils.widgets.get("percent_threshold"))
    
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
    
    from utils.ae_utils import get_arm_variable, create_safety_population
    
    arm_var = get_arm_variable(dm_df)
    safety_pop, arm_mapping = create_safety_population(dm_df, ex_df, arm_var)
    
    ae_with_arm = ae_df.join(
        safety_pop.select("USUBJID", "ARM_NUM"),
        on="USUBJID",
        how="inner"
    )
    
    print("Running aggregation by preferred term...")
    analysis_suffix = "B" if serious_only else "A"
    pt_result = aggregate_by_preferred_term(
        ae_with_arm, safety_pop, serious_only, pct_threshold
    )
    
    print("Running aggregation by severity...")
    sev_analysis_suffix = "D" if serious_only else "C"
    sev_result = aggregate_by_severity(
        ae_with_arm, safety_pop, spark, serious_only
    )
    
    output_loader = DatabricksAdamLoader(
        spark=spark,
        source_type="delta",
        catalog=output_catalog,
        schema=output_schema
    )
    
    print(f"Writing analysis_{analysis_suffix} results...")
    output_loader.write_delta_table(
        pt_result,
        f"ae_analysis_{analysis_suffix.lower()}",
        mode="overwrite"
    )
    
    print(f"Writing analysis_{sev_analysis_suffix} results...")
    output_loader.write_delta_table(
        sev_result,
        f"ae_analysis_{sev_analysis_suffix.lower()}",
        mode="overwrite"
    )
    
    print("Aggregation complete!")
    print(f"Results written to {output_catalog}.{output_schema}")
    
    return pt_result, sev_result


if __name__ == "__main__":
    pt_result, sev_result = main()
    display(pt_result)
    display(sev_result)
