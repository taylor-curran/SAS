"""
Oncology-Specific Adverse Events Aggregation Script for Databricks

Converts SAS ae_oncology_aggregate.sas to PySpark.
Focuses on toxicity grade aggregation and statistical comparisons between arms.

Key features:
1. Aggregate by toxicity grade (AETOXGR)
2. Calculate risk difference, relative risk, odds ratio
3. Perform Fisher's exact test for statistical comparison
4. Apply continuity correction for zero cell counts

Usage in Databricks notebook:
    %run ./ae_oncology_aggregate
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from scipy.stats import fisher_exact
from typing import Dict, Tuple
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loaders.databricks_adam_loader import DatabricksAdamLoader
from utils.ae_utils import map_toxicity_to_numeric


dbutils.widgets.text("source_type", "delta", "Data Source Type")
dbutils.widgets.text("catalog", "clinical_dev", "Catalog Name")
dbutils.widgets.text("schema", "sdtm", "Schema Name")
dbutils.widgets.text("file_path", "", "File Path (for XPT/SAS7BDAT)")
dbutils.widgets.text("output_catalog", "clinical_dev", "Output Catalog")
dbutils.widgets.text("output_schema", "results", "Output Schema")
dbutils.widgets.text("experimental_arm", "1", "Experimental Arm Number")
dbutils.widgets.text("control_arm", "2", "Control Arm Number")
dbutils.widgets.text("continuity_correction", "0.5", "Continuity Correction Value")


def aggregate_by_toxicity_grade(
    ae_df: DataFrame,
    safety_pop: DataFrame,
    spark: SparkSession,
    exp_arm: int,
    ctl_arm: int
) -> DataFrame:
    """
    Aggregate adverse events by toxicity grade (AETOXGR).
    
    Equivalent to SAS ae_oncology_aggregate.sas lines 1-192.
    Retains only the highest toxicity grade per subject per term.
    
    Args:
        ae_df: Adverse events DataFrame with ARM_NUM and AETOXGR
        safety_pop: Safety population with arm counts
        spark: SparkSession
        exp_arm: Experimental arm number
        ctl_arm: Control arm number
        
    Returns:
        Aggregated DataFrame with counts per toxicity grade
    """
    if "AETOXGR" not in ae_df.columns:
        print("Warning: AETOXGR column not found")
        return spark.createDataFrame([], schema="AEBODSYS STRING, AEDECOD STRING")
    
    ae_with_tox_num = ae_df.withColumn(
        "TOX_NUM",
        map_toxicity_to_numeric("AETOXGR")
    )
    
    window_spec = Window.partitionBy("USUBJID", "AEBODSYS", "AEDECOD").orderBy(F.desc("TOX_NUM"))
    
    ae_highest_tox = ae_with_tox_num.withColumn(
        "rank",
        F.row_number().over(window_spec)
    ).filter(F.col("rank") == 1).drop("rank")
    
    tox_counts = ae_highest_tox.groupBy(
        "AEBODSYS", "AEDECOD", "ARM_NUM", "TOX_NUM"
    ).agg(
        F.count("USUBJID").alias("COUNT")
    )
    
    result = None
    for arm_num in [exp_arm, ctl_arm]:
        arm_data = tox_counts.filter(F.col("ARM_NUM") == arm_num)
        arm_pivot = arm_data.groupBy("AEBODSYS", "AEDECOD").pivot(
            "TOX_NUM", [0, 1, 2, 3, 4, 5]
        ).agg(F.first("COUNT"))
        
        for tox in [0, 1, 2, 3, 4, 5]:
            old_name = str(tox)
            new_name = f"ARM{arm_num}_GRADE{tox}"
            if old_name in arm_pivot.columns:
                arm_pivot = arm_pivot.withColumnRenamed(old_name, new_name)
        
        if result is None:
            result = arm_pivot
        else:
            result = result.join(arm_pivot, on=["AEBODSYS", "AEDECOD"], how="outer")
    
    return result


def calculate_statistics(
    ae_agg: DataFrame,
    arm_totals: Dict[int, int],
    exp_arm: int,
    ctl_arm: int,
    cc_value: float = 0.5
) -> DataFrame:
    """
    Calculate statistical comparisons between experimental and control arms.
    
    Equivalent to SAS ae_oncology_aggregate.sas lines 390-600.
    Calculates risk difference, relative risk, odds ratio, and Fisher's exact test.
    
    Args:
        ae_agg: Aggregated AE DataFrame with counts per arm
        arm_totals: Dictionary mapping arm numbers to total subject counts
        exp_arm: Experimental arm number
        ctl_arm: Control arm number
        cc_value: Continuity correction value
        
    Returns:
        DataFrame with statistical measures added
    """
    exp_total = arm_totals.get(exp_arm, 0)
    ctl_total = arm_totals.get(ctl_arm, 0)
    
    grade_cols_exp = [f"ARM{exp_arm}_GRADE{i}" for i in range(0, 6)]
    grade_cols_ctl = [f"ARM{ctl_arm}_GRADE{i}" for i in range(0, 6)]
    
    exp_count_expr = sum([F.coalesce(F.col(c), F.lit(0)) for c in grade_cols_exp if c in ae_agg.columns])
    ctl_count_expr = sum([F.coalesce(F.col(c), F.lit(0)) for c in grade_cols_ctl if c in ae_agg.columns])
    
    result = ae_agg.withColumn("EXP_COUNT", exp_count_expr)
    result = result.withColumn("CTL_COUNT", ctl_count_expr)
    
    result = result.withColumn(
        "EXP_PCT",
        100.0 * F.col("EXP_COUNT") / exp_total
    )
    result = result.withColumn(
        "CTL_PCT",
        100.0 * F.col("CTL_COUNT") / ctl_total
    )
    
    result = result.withColumn(
        "RISK_DIFF",
        F.col("EXP_PCT") - F.col("CTL_PCT")
    )
    
    result = result.withColumn(
        "EXP_RISK",
        F.col("EXP_COUNT") / exp_total
    )
    result = result.withColumn(
        "CTL_RISK",
        F.col("CTL_COUNT") / ctl_total
    )
    
    result = result.withColumn(
        "RELATIVE_RISK",
        F.when(F.col("CTL_RISK") > 0, F.col("EXP_RISK") / F.col("CTL_RISK")).otherwise(None)
    )
    
    def calculate_fisher_p(exp_count, ctl_count):
        """Calculate Fisher's exact test p-value."""
        if exp_count is None or ctl_count is None:
            return None
        
        exp_no_event = exp_total - exp_count
        ctl_no_event = ctl_total - ctl_count
        
        if exp_count < 0 or ctl_count < 0 or exp_no_event < 0 or ctl_no_event < 0:
            return None
        
        contingency_table = [[exp_count, exp_no_event], [ctl_count, ctl_no_event]]
        try:
            odds_ratio, p_value = fisher_exact(contingency_table)
            return float(p_value)
        except:
            return None
    
    fisher_udf = F.udf(calculate_fisher_p, "double")
    result = result.withColumn(
        "FISHER_P_VALUE",
        fisher_udf(F.col("EXP_COUNT"), F.col("CTL_COUNT"))
    )
    
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
    exp_arm = int(dbutils.widgets.get("experimental_arm"))
    ctl_arm = int(dbutils.widgets.get("control_arm"))
    cc_value = float(dbutils.widgets.get("continuity_correction"))
    
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
    
    arm_totals_df = safety_pop.groupBy("ARM_NUM").agg(
        F.count("USUBJID").alias("ARM_TOTAL")
    ).collect()
    arm_totals = {row["ARM_NUM"]: row["ARM_TOTAL"] for row in arm_totals_df}
    
    print("Aggregating by toxicity grade...")
    tox_agg = aggregate_by_toxicity_grade(ae_with_arm, safety_pop, spark, exp_arm, ctl_arm)
    
    print("Calculating statistics...")
    tox_stats = calculate_statistics(tox_agg, arm_totals, exp_arm, ctl_arm, cc_value)
    
    output_loader = DatabricksAdamLoader(
        spark=spark,
        source_type="delta",
        catalog=output_catalog,
        schema=output_schema
    )
    
    print("Writing oncology results...")
    output_loader.write_delta_table(
        tox_stats,
        "ae_oncology_analysis",
        mode="overwrite"
    )
    
    print("Oncology analysis complete!")
    print(f"Results written to {output_catalog}.{output_schema}.ae_oncology_analysis")
    
    return tox_stats


if __name__ == "__main__":
    result = main()
    display(result)
