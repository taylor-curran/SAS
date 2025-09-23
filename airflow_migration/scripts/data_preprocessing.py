import argparse
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, lit
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def filter_clinical_data(df, config):
    """
    Filter clinical data based on analysis criteria.
    Replaces SAS WHERE clause from WPCT-F.07.01.sas lines 240-249
    """
    logger.info("Applying clinical data filters")
    
    parameter = config['analysis']['parameter']
    visits = config['analysis']['visits']
    measurement_var = config['variables']['measurement_var']
    population_flag = config['variables']['population_flag']
    analysis_flag = config['variables']['analysis_flag']
    
    
    filtered_df = df.filter(
        (col("PARAMCD") == parameter) &
        col(measurement_var).isNotNull() &
        (~isnan(col(measurement_var))) &
        col("AVISIT").isNotNull() &
        col("TRTA").isNotNull() &
        (col(analysis_flag) == "Y") &
        (col("TRTA") != "") &
        (col("AVISITN") != 99) &
        (col(population_flag) == "Y")  # Safety population filter
    )
    
    if visits:
        filtered_df = filtered_df.filter(col("AVISITN").isin(visits))
    
    logger.info(f"Filtered data: {filtered_df.count()} records remaining")
    return filtered_df

def add_outlier_detection(df, config):
    """
    Add outlier detection for normal range.
    Replaces outlier logic from WPCT-F.07.01.sas lines 245-248
    """
    logger.info("Adding outlier detection")
    
    measurement_var = config['variables']['measurement_var']
    low_ref_var = config['variables']['low_ref_var']
    high_ref_var = config['variables']['high_ref_var']
    
    
    outlier_condition = (
        (col(measurement_var).isNotNull() & col(low_ref_var).isNotNull() & 
         (col(measurement_var) < col(low_ref_var))) |
        (col(measurement_var).isNotNull() & col(high_ref_var).isNotNull() & 
         (col(measurement_var) > col(high_ref_var)))
    )
    
    df_with_outliers = df.withColumn(
        "m_var_outlier",
        when(outlier_condition, col(measurement_var)).otherwise(lit(None))
    )
    
    return df_with_outliers

def validate_required_variables(df, config):
    """
    Validate that all required variables are present in the dataset.
    """
    required_vars = [
        "STUDYID", "USUBJID", 
        config['variables']['population_flag'],
        config['variables']['analysis_flag'],
        config['variables']['treatment_var'],
        config['variables']['treatment_num_var'],
        "PARAM", "PARAMCD",
        config['variables']['measurement_var'],
        config['variables']['low_ref_var'],
        config['variables']['high_ref_var'],
        "AVISIT", "AVISITN", "ATPT", "ATPTN"
    ]
    
    missing_vars = [var for var in required_vars if var not in df.columns]
    
    if missing_vars:
        raise ValueError(f"Missing required variables: {missing_vars}")
    
    logger.info("All required variables are present")

def main():
    parser = argparse.ArgumentParser(description='Clinical Data Preprocessing')
    parser.add_argument('--config-path', required=True, help='Path to configuration file')
    parser.add_argument('--input-table', required=True, help='Input table name')
    parser.add_argument('--output-table', required=True, help='Output table name')
    
    args = parser.parse_args()
    
    with open(args.config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    spark = SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .config("spark.executor.memory", config['spark']['executor_memory']) \
        .config("spark.driver.memory", config['spark']['driver_memory']) \
        .getOrCreate()
    
    try:
        df = spark.table(args.input_table)
        logger.info(f"Loaded {df.count()} records from {args.input_table}")
        
        validate_required_variables(df, config)
        
        filtered_df = filter_clinical_data(df, config)
        
        processed_df = add_outlier_detection(filtered_df, config)
        
        processed_df.write.format("delta").mode("overwrite").saveAsTable(args.output_table)
        
        logger.info(f"Data preprocessing completed. Output: {processed_df.count()} records")
        
        logger.info("Sample of processed data:")
        processed_df.select(
            "USUBJID", "PARAMCD", config['variables']['measurement_var'],
            "AVISITN", config['variables']['treatment_var'], "m_var_outlier"
        ).show(10)
        
    except Exception as e:
        logger.error(f"Data preprocessing failed: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
