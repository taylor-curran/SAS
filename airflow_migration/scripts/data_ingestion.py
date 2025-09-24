"""
Data Ingestion Script

This script handles the ingestion of clinical trial data from SDTM and ADaM datasets,
converting SAS data formats to PySpark DataFrame structures.
"""

import os
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.phuse_utils import detect_outliers


def create_spark_session(app_name: str = "Clinical Data Analysis") -> SparkSession:
    """
    Create and configure a Spark session for clinical data analysis.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Configured SparkSession
    """
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .getOrCreate())


def read_sas_dataset(
    spark: SparkSession,
    file_path: str,
    dataset_format: str = "sas7bdat"
) -> DataFrame:
    """
    Read a SAS dataset into a Spark DataFrame.
    
    Args:
        spark: SparkSession
        file_path: Path to the SAS dataset
        dataset_format: Format of the dataset (sas7bdat or xpt)
        
    Returns:
        PySpark DataFrame containing the data
    """
    if dataset_format.lower() == "sas7bdat":
        return spark.read.format("com.github.saurfang.sas.spark").load(file_path)
    elif dataset_format.lower() == "xpt":
        return spark.read.format("com.github.saurfang.sas.spark").option("format", "xpt").load(file_path)
    else:
        raise ValueError(f"Unsupported dataset format: {dataset_format}")


def ingest_clinical_data(
    spark: SparkSession,
    config: Dict,
    data_path: str
) -> Dict[str, DataFrame]:
    """
    Ingest clinical data from SDTM and ADaM datasets.
    
    Args:
        spark: SparkSession
        config: Configuration dictionary
        data_path: Base path to the data directory
        
    Returns:
        Dictionary of DataFrames containing the ingested data
    """
    adam_path = os.path.join(data_path, "adam")
    sdtm_path = os.path.join(data_path, "sdtm")
    
    dataset_name = config.get("data_sources", {}).get("dataset", "ADLBC")
    
    dataset_path = os.path.join(adam_path, f"{dataset_name.lower()}.sas7bdat")
    df = read_sas_dataset(spark, dataset_path)
    
    df = df.toDF(*[c.lower() for c in df.columns])
    
    if "atptn" not in df.columns:
        df = df.withColumn("atptn", F.lit(1))
    
    if "atpt" not in df.columns:
        df = df.withColumn("atpt", F.lit("TimePoint unknown"))
    
    measure_var = config.get("variables", {}).get("measurement_var", "aval")
    low_var = config.get("variables", {}).get("low_limit_var", "a1lo")
    high_var = config.get("variables", {}).get("high_limit_var", "a1hi")
    
    df = detect_outliers(df, measure_var, low_var, high_var)
    
    return {"main_dataset": df}


def main(config_path: str):
    """
    Main function to run the data ingestion process.
    
    Args:
        config_path: Path to the configuration file
    """
    import yaml
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    spark = create_spark_session()
    
    try:
        data_path = config.get("paths", {}).get("data_path", "/data")
        datasets = ingest_clinical_data(spark, config, data_path)
        
        output_path = config.get("paths", {}).get("output_path", "/tmp/clinical_data")
        
        for name, df in datasets.items():
            output_file = os.path.join(output_path, f"{name}.parquet")
            df.write.mode("overwrite").parquet(output_file)
            
        print(f"Data ingestion completed successfully. Data saved to {output_path}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest clinical trial data")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    
    args = parser.parse_args()
    main(args.config)
