"""
Data Preprocessing Script

This script handles the preprocessing of clinical trial data, including filtering,
transforming, and preparing the data for statistical analysis.
"""

import os
from typing import Dict, List, Optional, Union

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.phuse_utils import detect_outliers


def create_spark_session(app_name: str = "Clinical Data Preprocessing") -> SparkSession:
    """
    Create and configure a Spark session for clinical data preprocessing.
    
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


def filter_dataset(
    df: DataFrame,
    config: Dict
) -> DataFrame:
    """
    Filter the dataset based on configuration parameters.
    
    Equivalent to SAS WHERE clause: where (paramcd in (&param) &cond);
    
    Args:
        df: PySpark DataFrame containing the data
        config: Configuration dictionary
        
    Returns:
        Filtered DataFrame
    """
    parameters = config.get("data_sources", {}).get("parameters", ["ALB"])
    visits = config.get("data_sources", {}).get("visits", [0, 2, 4, 6])
    
    population_flag = config.get("variables", {}).get("population_flag", "saffl")
    analysis_flag = config.get("variables", {}).get("analysis_flag", "anl01fl")
    
    filtered_df = df
    
    if parameters:
        filtered_df = filtered_df.filter(F.col("paramcd").isin(parameters))
    
    if visits:
        filtered_df = filtered_df.filter(F.col("avisitn").isin(visits))
    
    if population_flag in df.columns:
        filtered_df = filtered_df.filter(F.col(population_flag) == "Y")
    
    if analysis_flag in df.columns:
        filtered_df = filtered_df.filter(F.col(analysis_flag) == "Y")
    
    return filtered_df


def create_treatment_abbreviations(
    df: DataFrame,
    config: Dict
) -> DataFrame:
    """
    Create abbreviated treatment names for display.
    
    Equivalent to SAS code:
    ```
    proc format;
        value trt_short
        0 = 'P'
        54 = 'X-high'
        81 = 'X-low'
        other = 'UNEXPECTED';
    run;
    ```
    
    Args:
        df: PySpark DataFrame containing the data
        config: Configuration dictionary
        
    Returns:
        DataFrame with abbreviated treatment names
    """
    treatment_num_var = config.get("variables", {}).get("treatment_num_var", "trtpn")
    
    def abbreviate_treatment(trt_num):
        if trt_num == 0:
            return "P"
        elif trt_num == 54:
            return "X-high"
        elif trt_num == 81:
            return "X-low"
        else:
            return "UNEXPECTED"
    
    abbreviate_treatment_udf = F.udf(abbreviate_treatment, StringType())
    
    return df.withColumn("trtp_short", abbreviate_treatment_udf(F.col(treatment_num_var)))


def preprocess_data(
    spark: SparkSession,
    config: Dict,
    input_path: str,
    output_path: str
) -> None:
    """
    Preprocess clinical data for analysis.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        input_path: Path to the input data
        output_path: Path to save the preprocessed data
    """
    df = spark.read.parquet(os.path.join(input_path, "main_dataset.parquet"))
    
    filtered_df = filter_dataset(df, config)
    
    preprocessed_df = create_treatment_abbreviations(filtered_df, config)
    
    preprocessed_df.write.mode("overwrite").parquet(os.path.join(output_path, "preprocessed_data.parquet"))
    
    parameters = config.get("data_sources", {}).get("parameters", ["ALB"])
    
    for param in parameters:
        param_df = preprocessed_df.filter(F.col("paramcd") == param)
        param_df.write.mode("overwrite").parquet(os.path.join(output_path, f"param_{param.lower()}.parquet"))
    
    print(f"Data preprocessing completed successfully. Data saved to {output_path}")


def main(config_path: str):
    """
    Main function to run the data preprocessing process.
    
    Args:
        config_path: Path to the configuration file
    """
    import yaml
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    spark = create_spark_session()
    
    try:
        input_path = config.get("paths", {}).get("ingestion_output_path", "/tmp/clinical_data")
        output_path = config.get("paths", {}).get("preprocessing_output_path", "/tmp/clinical_data_preprocessed")
        
        preprocess_data(spark, config, input_path, output_path)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Preprocess clinical trial data")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    
    args = parser.parse_args()
    main(args.config)
