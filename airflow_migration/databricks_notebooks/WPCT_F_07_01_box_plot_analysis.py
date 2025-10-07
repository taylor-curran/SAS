
# Databricks notebook source
# MAGIC %md
# MAGIC # WPCT-F.07.01: Box Plot Analysis
# MAGIC Migrated from SAS script WPCT-F.07.01.sas

# COMMAND ----------

dbutils.widgets.text("dataset", "ADLBC", "Dataset Name")
dbutils.widgets.text("parameters", "ALB", "Parameters (comma-separated)")
dbutils.widgets.text("visits", "0,2,4,6", "Visit Numbers (comma-separated)")
dbutils.widgets.text("measurement_var", "aval", "Measurement Variable")
dbutils.widgets.text("treatment_var", "trtp_short", "Treatment Variable")
dbutils.widgets.text("treatment_num_var", "trtpn", "Treatment Number Variable")
dbutils.widgets.text("low_limit_var", "a1lo", "Lower Limit Variable")
dbutils.widgets.text("high_limit_var", "a1hi", "Upper Limit Variable")
dbutils.widgets.text("population_flag", "saffl", "Population Flag")
dbutils.widgets.text("analysis_flag", "anl01fl", "Analysis Flag")
dbutils.widgets.text("ref_lines", "UNIFORM", "Reference Lines Type")
dbutils.widgets.text("max_boxes_per_page", "20", "Max Boxes Per Page")

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from typing import Dict, List, Optional, Tuple
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from phuse_utils import (
    labels_from_var,
    count_unique_values,
    get_reference_lines,
    get_var_min_max,
    value_format,
    boxplot_block_ranges,
    axis_order,
    detect_outliers
)

# COMMAND ----------

def get_configuration() -> Dict:
    """
    Parse Databricks widgets into configuration dictionary.
    
    SAS equivalent: Macro variable definitions (lines 143-157)
    
    Returns:
        Dictionary containing all configuration parameters
    """
    return {
        "data_sources": {
            "dataset": dbutils.widgets.get("dataset"),
            "parameters": [p.strip() for p in dbutils.widgets.get("parameters").split(",")],
            "visits": [int(v.strip()) for v in dbutils.widgets.get("visits").split(",") if v.strip()]
        },
        "variables": {
            "measurement_var": dbutils.widgets.get("measurement_var"),
            "treatment_var": dbutils.widgets.get("treatment_var"),
            "treatment_num_var": dbutils.widgets.get("treatment_num_var"),
            "low_limit_var": dbutils.widgets.get("low_limit_var"),
            "high_limit_var": dbutils.widgets.get("high_limit_var"),
            "population_flag": dbutils.widgets.get("population_flag"),
            "analysis_flag": dbutils.widgets.get("analysis_flag")
        },
        "plotting": {
            "ref_lines": dbutils.widgets.get("ref_lines"),
            "max_boxes_per_page": int(dbutils.widgets.get("max_boxes_per_page"))
        }
    }

# COMMAND ----------

def filter_and_prepare_data(df: DataFrame, config: Dict) -> DataFrame:
    """
    Filter dataset and create outlier detection variable.
    
    SAS equivalent: Lines 217-227 in WPCT-F.07.01.sas
    - DATA step: css_anadata
    - Restricts to safety population and analysis records
    - Creates normal range outlier variable (m_var_outlier)
    
    Args:
        df: Input PySpark DataFrame
        config: Configuration dictionary
        
    Returns:
        Filtered DataFrame with outlier column added
    """
    parameters = config["data_sources"]["parameters"]
    visits = config["data_sources"]["visits"]
    p_fl = config["variables"]["population_flag"]
    a_fl = config["variables"]["analysis_flag"]
    m_var = config["variables"]["measurement_var"]
    lo_var = config["variables"]["low_limit_var"]
    hi_var = config["variables"]["high_limit_var"]
    
    filtered_df = df.filter(
        (F.col("paramcd").isin(parameters)) &
        (F.col("avisitn").isin(visits)) &
        (F.col(p_fl) == "Y") &
        (F.col(a_fl) == "Y")
    )
    
    filtered_df = detect_outliers(filtered_df, m_var, lo_var, hi_var)
    
    return filtered_df

# COMMAND ----------

def create_treatment_abbreviations(df: DataFrame, config: Dict) -> DataFrame:
    """
    Create abbreviated treatment names.
    
    SAS equivalent: 
    - PROC FORMAT (lines 163-169): trt_short format definition
    - DATA step (lines 172-181): Apply format to create trtp_short variable
    
    Args:
        df: Input PySpark DataFrame
        config: Configuration dictionary
        
    Returns:
        DataFrame with trtp_short column added
    """
    tn_var = config["variables"]["treatment_num_var"]
    
    def abbreviate_treatment(trt_num):
        if trt_num == 0:
            return "P"
        elif trt_num == 54:
            return "X-high"
        elif trt_num == 81:
            return "X-low"
        else:
            return "UNEXPECTED"
    
    abbreviate_udf = F.udf(abbreviate_treatment, StringType())
    return df.withColumn("trtp_short", abbreviate_udf(F.col(tn_var)))

# COMMAND ----------

def calculate_statistics(df: DataFrame, config: Dict) -> DataFrame:
    """
    Calculate summary statistics by visit and treatment.
    
    SAS equivalent: PROC SUMMARY (lines 321-326)
    - Groups by: avisitn, trtpn, avisit, trtp_short
    - Statistics: n, mean, std, median, min, max, q1, q3
    
    Args:
        df: Input PySpark DataFrame
        config: Configuration dictionary
        
    Returns:
        DataFrame containing summary statistics
    """
    m_var = config["variables"]["measurement_var"]
    t_var = config["variables"]["treatment_var"]
    tn_var = config["variables"]["treatment_num_var"]
    
    stats_df = df.groupBy(
        "avisitn", 
        tn_var, 
        "avisit", 
        t_var, 
        "atptn", 
        "atpt", 
        "paramcd", 
        "param"
    ).agg(
        F.count(m_var).alias("n"),
        F.mean(m_var).alias("mean"),
        F.stddev(m_var).alias("std"),
        F.expr(f"percentile_approx({m_var}, 0.5)").alias("median"),
        F.min(m_var).alias("datamin"),
        F.max(m_var).alias("datamax"),
        F.expr(f"percentile_approx({m_var}, 0.25)").alias("q1"),
        F.expr(f"percentile_approx({m_var}, 0.75)").alias("q3")
    )
    
    return stats_df

# COMMAND ----------

def process_all_parameters_and_timepoints(
    df: DataFrame,
    config: Dict,
    output_path: str
) -> None:
    """
    Main processing loop: iterate through parameters and timepoints.
    
    SAS equivalent: Triple-nested macro (lines 274-411)
    - LOOP 1: Parameters (pdx = 1 to &paramcd_n) - lines 274-411
    - LOOP 2: Timepoints (tdx = 1 to &atptn_n) - lines 295-409
    - LOOP 3: Visit pages (vdx for pagination) - lines 369-403
    
    Note: LOOP 3 (pagination) is handled by downstream visualization,
    so this function focuses on LOOP 1 and LOOP 2.
    
    Args:
        df: Prepared PySpark DataFrame
        config: Configuration dictionary
        output_path: Base path for saving output Delta tables
    """
    parameters = config["data_sources"]["parameters"]
    
    for param in parameters:
        param_df = df.filter(F.col("paramcd") == param)
        
        if param_df.count() == 0:
            print(f"No data for parameter {param}, skipping...")
            continue
        
        param_label = param_df.select("param").first()[0]
        
        timepoints_df = param_df.select("atptn", "atpt").distinct().orderBy("atptn")
        timepoints = [(row["atptn"], row["atpt"]) for row in timepoints_df.collect()]
        
        for tp_num, tp_label in timepoints:
            tp_df = param_df.filter(F.col("atptn") == tp_num)
            
            stats_df = calculate_statistics(tp_df, config)
            
            output_dir = f"{output_path}/param_{param.lower()}/timepoint_{tp_num}"
            
            tp_df.write.mode("overwrite").format("delta").save(f"{output_dir}/data")
            
            stats_df.write.mode("overwrite").format("delta").save(f"{output_dir}/statistics")
            
            print(f"Processed: Parameter={param} ({param_label}), Timepoint={tp_num} ({tp_label})")

# COMMAND ----------

def main():
    """
    Main execution function - initializes configuration and displays parameters.
    
    Returns:
        Configuration dictionary
    """
    config = get_configuration()
    
    print("=" * 60)
    print("WPCT-F.07.01 Box Plot Analysis - Configuration")
    print("=" * 60)
    print(f"Dataset: {config['data_sources']['dataset']}")
    print(f"Parameters: {config['data_sources']['parameters']}")
    print(f"Visits: {config['data_sources']['visits']}")
    print(f"Measurement Variable: {config['variables']['measurement_var']}")
    print(f"Treatment Variable: {config['variables']['treatment_var']}")
    print(f"Reference Lines: {config['plotting']['ref_lines']}")
    print("=" * 60)
    
    return config

config = main()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Usage Example
# MAGIC ```python
# MAGIC input_df = spark.read.format("delta").load("/path/to/data")
# MAGIC filtered_df = filter_and_prepare_data(input_df, config)
# MAGIC prepared_df = create_treatment_abbreviations(filtered_df, config)
# MAGIC process_all_parameters_and_timepoints(prepared_df, config, "/path/to/output")
# MAGIC ```
