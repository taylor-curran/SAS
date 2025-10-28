import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import logging

def get_spark_session():
    """Initialize Spark session for clinical data processing"""
    spark = SparkSession.builder \
        .appName("ClinicalDataAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    return spark

def read_sas_dataset(file_path, dataset_name):
    """
    Read SAS7BDAT file using pandas and convert to Spark DataFrame
    Replaces %util_access_test_data functionality
    """
    try:
        pandas_df = pd.read_sas(file_path)
        
        spark = get_spark_session()
        spark_df = spark.createDataFrame(pandas_df)
        
        logging.info(f"Successfully loaded {dataset_name} with {spark_df.count()} rows and {len(spark_df.columns)} columns")
        
        return spark_df
        
    except Exception as e:
        logging.error(f"Error reading SAS dataset {dataset_name}: {str(e)}")
        raise

def add_timepoint_variables(df):
    """
    Add dummy timepoint variables as done in original SAS code
    Replicates the logic from example_call_wpct-f.07.01.sas lines 56-61
    """
    from pyspark.sql.functions import lit
    
    df_with_timepoints = df.withColumn("ATPTN", lit(1)) \
                          .withColumn("ATPT", lit("TimePoint unknown"))
    
    return df_with_timepoints

def ingest_clinical_data(config):
    """
    Main data ingestion function
    
    Args:
        config (dict): Configuration containing dataset name, paths, etc.
    
    Returns:
        dict: Status and metadata about ingested data
    """
    dataset_name = config['dataset']
    data_path = config['data_path']
    output_path = config['output_path']
    
    file_path = os.path.join(data_path, f"{dataset_name.lower()}.sas7bdat")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Dataset file not found: {file_path}")
    
    os.makedirs(output_path, exist_ok=True)
    
    df = read_sas_dataset(file_path, dataset_name)
    
    df_with_timepoints = add_timepoint_variables(df)
    
    output_file = os.path.join(output_path, "raw_data")
    df_with_timepoints.write.mode("overwrite").parquet(output_file)
    
    result = {
        'status': 'success',
        'dataset': dataset_name,
        'row_count': df_with_timepoints.count(),
        'column_count': len(df_with_timepoints.columns),
        'output_path': output_file
    }
    
    logging.info(f"Data ingestion completed: {result}")
    return result

if __name__ == "__main__":
    test_config = {
        'dataset': 'ADLBC',
        'data_path': '/home/ubuntu/repos/SAS/data/adam/',
        'output_path': '/tmp/clinical_analysis/'
    }
    
    result = ingest_clinical_data(test_config)
    print(f"Ingestion result: {result}")
