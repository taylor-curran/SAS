"""
Data Preprocessing Module
Replaces SAS data filtering and subset logic

This module handles data filtering equivalent to the WHERE clauses
and data subset operations in WPCT-F.07.01.sas (lines 240-249)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull
import logging

logger = logging.getLogger(__name__)

def preprocess_clinical_data(config, **context):
    """
    Preprocess clinical data with filtering and outlier detection
    
    Replaces SAS logic from WPCT-F.07.01.sas lines 240-249:
    - where paramcd = "&m_var" and not missing(aval) and not missing(visit) 
      and not missing(trta) and anl01fl = 'Y' and trta ^= '' and avisitn ^= 99
    - Normal range outlier variable creation (lines 245-248)
    
    Args:
        config (dict): Pipeline configuration parameters
        **context: Airflow context
        
    Returns:
        str: Path to preprocessed dataset
    """
    logger.info("Starting data preprocessing and filtering")
    
    spark = SparkSession.builder \
        .appName("ClinicalDataPreprocessing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        input_path = context['task_instance'].xcom_pull(
            task_ids='data_ingestion', 
            key='ingested_data_path'
        )
        
        df = spark.read.parquet(input_path)
        logger.info(f"Loaded {df.count()} records for preprocessing")
        
        param_filtered_df = df.filter(col("PARAMCD") == config['parameter'])
        logger.info(f"After parameter filter ({config['parameter']}): {param_filtered_df.count()} records")
        
        visit_filtered_df = param_filtered_df.filter(col("AVISITN").isin(config['visit_filter']))
        logger.info(f"After visit filter: {visit_filtered_df.count()} records")
        
        filtered_df = visit_filtered_df.filter(
            (col(config['population_flag']) == 'Y') &
            (col(config['analysis_flag']) == 'Y') &
            (col(config['measurement_var']).isNotNull()) &
            (~isnan(col(config['measurement_var']))) &
            (col("AVISIT").isNotNull()) &
            (col("TRTA").isNotNull()) &
            (col("TRTA") != "") &
            (col("AVISITN") != 99)
        )
        
        logger.info(f"After comprehensive filtering: {filtered_df.count()} records")
        
        outlier_df = filtered_df.withColumn(
            "m_var_outlier",
            when(
                ((col(config['measurement_var']).isNotNull()) & 
                 (col(config['low_limit_var']).isNotNull()) & 
                 (col(config['measurement_var']) < col(config['low_limit_var']))) |
                ((col(config['measurement_var']).isNotNull()) & 
                 (col(config['high_limit_var']).isNotNull()) & 
                 (col(config['measurement_var']) > col(config['high_limit_var']))),
                col(config['measurement_var'])
            ).otherwise(None)
        )
        
        outlier_df.cache()
        
        output_path = f"{config['output_path']}/preprocessed_data"
        outlier_df.write.mode("overwrite").parquet(output_path)
        
        logger.info(f"Data preprocessing completed. Output written to: {output_path}")
        
        context['task_instance'].xcom_push(key='preprocessed_data_path', value=output_path)
        context['task_instance'].xcom_push(key='filtered_record_count', value=outlier_df.count())
        
        total_outliers = outlier_df.filter(col("m_var_outlier").isNotNull()).count()
        logger.info(f"Identified {total_outliers} outlier measurements outside normal range")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error during data preprocessing: {str(e)}")
        raise
    finally:
        spark.stop()

def get_unique_values_summary(df, column_name):
    """
    Get summary of unique values in a column (replaces util_count_unique_values)
    
    Args:
        df: Spark DataFrame
        column_name (str): Column to analyze
        
    Returns:
        dict: Summary statistics
    """
    unique_count = df.select(column_name).distinct().count()
    return {
        'column': column_name,
        'unique_count': unique_count,
        'total_records': df.count()
    }
