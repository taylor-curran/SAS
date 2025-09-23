"""
Data Ingestion Module
Replaces SAS %util_access_test_data functionality

This module handles loading ADLBC dataset and initial data preparation,
equivalent to the initialization logic in example_call_wpct-f.07.01.sas
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StringType
import logging

logger = logging.getLogger(__name__)

def ingest_adlbc_data(config, **context):
    """
    Ingest ADLBC dataset and perform initial data preparation
    
    Replaces SAS logic:
    - %util_access_test_data(&ds, local=&phuse/data/adam/cdisc-split/)
    - DATA &ds; SET &ds; ATPTN = 1; ATPT = "TimePoint unknown"; RUN;
    
    Args:
        config (dict): Pipeline configuration parameters
        **context: Airflow context
        
    Returns:
        str: Path to processed dataset
    """
    logger.info("Starting data ingestion for ADLBC dataset")
    
    spark = SparkSession.builder \
        .appName("ClinicalDataIngestion") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        dataset_path = "/home/ubuntu/repos/SAS/data/adam/adlbc.sas7bdat"
        
        pandas_df = pd.read_sas(dataset_path)
        logger.info(f"Loaded {len(pandas_df)} records from ADLBC dataset")
        
        spark_df = spark.createDataFrame(pandas_df)
        
        processed_df = spark_df.withColumn("ATPTN", lit(1)) \
                              .withColumn("ATPT", lit("TimePoint unknown"))
        
        processed_df = processed_df.withColumn(
            "trtp_short",
            when(col("TRTPN") == 0, "P")
            .when(col("TRTPN") == 54, "X-high") 
            .when(col("TRTPN") == 81, "X-low")
            .otherwise("UNEXPECTED")
        )
        
        processed_df.cache()
        
        output_path = f"{config['output_path']}/ingested_data"
        processed_df.write.mode("overwrite").parquet(output_path)
        
        logger.info(f"Data ingestion completed. Output written to: {output_path}")
        logger.info(f"Total records processed: {processed_df.count()}")
        
        context['task_instance'].xcom_push(key='ingested_data_path', value=output_path)
        context['task_instance'].xcom_push(key='record_count', value=processed_df.count())
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error during data ingestion: {str(e)}")
        raise
    finally:
        spark.stop()

def validate_dataset_structure(df, required_columns):
    """
    Validate that the dataset contains required columns for analysis
    
    Args:
        df: Spark DataFrame
        required_columns (list): List of required column names
        
    Returns:
        bool: True if all required columns are present
    """
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    return True
