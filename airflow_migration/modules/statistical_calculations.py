"""
Statistical Calculations Module
Replaces SAS PROC SUMMARY operations

This module handles statistical aggregations equivalent to the 
PROC SUMMARY operations in WPCT-F.07.01.sas (lines 343-348)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, mean, stddev, min as spark_min, max as spark_max,
    expr, round as spark_round
)
import logging

logger = logging.getLogger(__name__)

def calculate_statistics(config, **context):
    """
    Calculate summary statistics for clinical measurements
    
    Replaces SAS PROC SUMMARY from WPCT-F.07.01.sas lines 343-348:
    proc summary data=css_nexttimept noprint;
      by avisitn &tn_var avisit &t_var;
      var &m_var;
      output out=css_stats (drop=_type_ _freq_)
             n=n mean=mean std=std median=median min=datamin max=datamax q1=q1 q3=q3;
    run;
    
    Args:
        config (dict): Pipeline configuration parameters
        **context: Airflow context
        
    Returns:
        str: Path to statistics dataset
    """
    logger.info("Starting statistical calculations")
    
    spark = SparkSession.builder \
        .appName("ClinicalStatistics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        input_path = context['task_instance'].xcom_pull(
            task_ids='data_preprocessing', 
            key='preprocessed_data_path'
        )
        
        df = spark.read.parquet(input_path)
        logger.info(f"Loaded {df.count()} records for statistical analysis")
        
        stats_df = df.groupBy(
            "AVISITN", 
            config['treatment_num_var'],
            "AVISIT", 
            config['treatment_var']
        ).agg(
            count(config['measurement_var']).alias("n"),
            mean(config['measurement_var']).alias("mean"),
            stddev(config['measurement_var']).alias("std"),
            spark_min(config['measurement_var']).alias("datamin"),
            spark_max(config['measurement_var']).alias("datamax"),
            expr(f"percentile_approx({config['measurement_var']}, 0.5)").alias("median"),
            expr(f"percentile_approx({config['measurement_var']}, 0.25)").alias("q1"),
            expr(f"percentile_approx({config['measurement_var']}, 0.75)").alias("q3")
        )
        
        formatted_stats_df = stats_df.withColumn(
            "mean", spark_round(col("mean"), 2)
        ).withColumn(
            "std", spark_round(col("std"), 3)
        ).withColumn(
            "median", spark_round(col("median"), 1)
        )
        
        sorted_stats_df = formatted_stats_df.orderBy("AVISITN", config['treatment_num_var'])
        
        sorted_stats_df.cache()
        
        output_path = f"{config['output_path']}/statistics_data"
        sorted_stats_df.write.mode("overwrite").parquet(output_path)
        
        logger.info(f"Statistical calculations completed. Output written to: {output_path}")
        
        stats_count = sorted_stats_df.count()
        logger.info(f"Generated statistics for {stats_count} visit/treatment combinations")
        
        context['task_instance'].xcom_push(key='statistics_data_path', value=output_path)
        context['task_instance'].xcom_push(key='statistics_count', value=stats_count)
        
        plot_data_path = create_plot_dataset(df, sorted_stats_df, config)
        context['task_instance'].xcom_push(key='plot_data_path', value=plot_data_path)
        
        return output_path
        
    except Exception as e:
        logger.error(f"Error during statistical calculations: {str(e)}")
        raise
    finally:
        spark.stop()

def create_plot_dataset(raw_df, stats_df, config):
    """
    Create combined dataset for plotting (equivalent to SAS css_plot)
    
    Replaces SAS logic from lines 355-360:
    data css_plot;
      set css_nexttimept
          css_stats;
      format mean %scan(&util_value_format, 1, %str( )) std %scan(&util_value_format, 2, %str( ));
    run;
    
    Args:
        raw_df: Raw measurement data
        stats_df: Statistical summaries
        config: Pipeline configuration
        
    Returns:
        str: Path to plot dataset
    """
    logger.info("Creating combined plot dataset")
    
    plot_data_path = f"{config['output_path']}/plot_data"
    
    raw_df.write.mode("overwrite").parquet(f"{plot_data_path}/raw_data")
    stats_df.write.mode("overwrite").parquet(f"{plot_data_path}/stats_data")
    
    logger.info(f"Plot dataset created at: {plot_data_path}")
    return plot_data_path

def get_reference_lines(df, config):
    """
    Calculate reference lines for normal range (replaces util_get_reference_lines)
    
    Args:
        df: Spark DataFrame with measurement data
        config: Pipeline configuration
        
    Returns:
        dict: Reference line values
    """
    ref_lines_type = config.get('ref_lines', 'UNIFORM')
    
    if ref_lines_type == 'NONE':
        return {}
    elif ref_lines_type == 'UNIFORM':
        low_values = df.select(config['low_limit_var']).distinct().collect()
        high_values = df.select(config['high_limit_var']).distinct().collect()
        
        if len(low_values) == 1 and len(high_values) == 1:
            return {
                'low_ref': low_values[0][0],
                'high_ref': high_values[0][0]
            }
    
    return {}
