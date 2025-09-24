"""
Statistical Analysis Script

This script performs statistical calculations on preprocessed clinical trial data,
including summary statistics and outlier detection.
"""

import os
from typing import Dict, List, Optional, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.phuse_utils import labels_from_var, count_unique_values


def create_spark_session(app_name: str = "Clinical Data Statistical Analysis") -> SparkSession:
    """
    Create and configure a Spark session for clinical data statistical analysis.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Configured SparkSession
    """
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())


def calculate_statistics(
    df: DataFrame,
    config: Dict
) -> DataFrame:
    """
    Calculate summary statistics for the clinical data.
    
    Equivalent to SAS PROC SUMMARY:
    ```
    proc summary data=css_nexttimept noprint;
      by avisitn &tn_var avisit &t_var;
      var &m_var;
      output out=css_stats (drop=_type_ _freq_)
             n=n mean=mean std=std median=median min=datamin max=datamax q1=q1 q3=q3;
    run;
    ```
    
    Args:
        df: PySpark DataFrame containing the preprocessed data
        config: Configuration dictionary
        
    Returns:
        DataFrame with calculated statistics
    """
    measure_var = config.get("variables", {}).get("measurement_var", "aval")
    treatment_var = config.get("variables", {}).get("treatment_var", "trtp_short")
    treatment_num_var = config.get("variables", {}).get("treatment_num_var", "trtpn")
    
    stats_df = df.groupBy("avisitn", treatment_num_var, "avisit", treatment_var).agg(
        F.count(measure_var).alias("n"),
        F.mean(measure_var).alias("mean"),
        F.stddev(measure_var).alias("std"),
        F.expr(f"percentile_approx({measure_var}, 0.5)").alias("median"),
        F.min(measure_var).alias("datamin"),
        F.max(measure_var).alias("datamax"),
        F.expr(f"percentile_approx({measure_var}, 0.25)").alias("q1"),
        F.expr(f"percentile_approx({measure_var}, 0.75)").alias("q3")
    )
    
    return stats_df


def analyze_by_parameter_timepoint(
    spark: SparkSession,
    config: Dict,
    input_path: str,
    output_path: str
) -> None:
    """
    Perform statistical analysis for each parameter and timepoint.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        input_path: Path to the preprocessed data
        output_path: Path to save the analysis results
    """
    parameters = config.get("data_sources", {}).get("parameters", ["ALB"])
    
    for param in parameters:
        param_df = spark.read.parquet(os.path.join(input_path, f"param_{param.lower()}.parquet"))
        
        timepoints_df = param_df.select("atptn", "atpt").distinct()
        timepoints = [(row["atptn"], row["atpt"]) for row in timepoints_df.collect()]
        
        for tp_num, tp_label in timepoints:
            tp_df = param_df.filter(F.col("atptn") == tp_num)
            
            stats_df = calculate_statistics(tp_df, config)
            
            plot_df = tp_df.select("avisitn", "avisit", config.get("variables", {}).get("treatment_num_var", "trtpn"), 
                                  config.get("variables", {}).get("treatment_var", "trtp_short"),
                                  config.get("variables", {}).get("measurement_var", "aval"),
                                  f"{config.get('variables', {}).get('measurement_var', 'aval')}_outlier")
            
            output_dir = os.path.join(output_path, f"param_{param.lower()}", f"timepoint_{tp_num}")
            
            tp_df.write.mode("overwrite").parquet(os.path.join(output_dir, "data.parquet"))
            
            stats_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stats.parquet"))
            
            plot_df.write.mode("overwrite").parquet(os.path.join(output_dir, "plot_data.parquet"))
            
            metadata = {
                "parameter_code": param,
                "parameter_label": param_df.select("param").distinct().first()[0],
                "timepoint_num": tp_num,
                "timepoint_label": tp_label
            }
            
            import json
            with open(os.path.join(output_dir, "metadata.json"), "w") as f:
                json.dump(metadata, f)
    
    print(f"Statistical analysis completed successfully. Results saved to {output_path}")


def main(config_path: str):
    """
    Main function to run the statistical analysis process.
    
    Args:
        config_path: Path to the configuration file
    """
    import yaml
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    spark = create_spark_session()
    
    try:
        input_path = config.get("paths", {}).get("preprocessing_output_path", "/tmp/clinical_data_preprocessed")
        output_path = config.get("paths", {}).get("analysis_output_path", "/tmp/clinical_data_analysis")
        
        analyze_by_parameter_timepoint(spark, config, input_path, output_path)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Perform statistical analysis on clinical trial data")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    
    args = parser.parse_args()
    main(args.config)
