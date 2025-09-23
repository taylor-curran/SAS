import argparse
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev, min as spark_min, max as spark_max, expr
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def calculate_summary_statistics(df, config):
    """
    Calculate summary statistics for clinical measurements.
    Replaces PROC SUMMARY from WPCT-F.07.01.sas lines 343-348
    
    Original SAS code:
    proc summary data=css_nexttimept noprint;
      by avisitn &tn_var avisit &t_var;
      var &m_var;
      output out=css_stats (drop=_type_ _freq_)
             n=n mean=mean std=std median=median min=datamin max=datamax q1=q1 q3=q3;
    run;
    """
    logger.info("Calculating summary statistics")
    
    measurement_var = config['variables']['measurement_var']
    treatment_var = config['variables']['treatment_var']
    treatment_num_var = config['variables']['treatment_num_var']
    
    stats_df = df.groupBy("AVISITN", treatment_num_var, "AVISIT", treatment_var) \
                 .agg(
                     count(measurement_var).alias("n"),
                     mean(measurement_var).alias("mean"),
                     stddev(measurement_var).alias("std"),
                     expr(f"percentile_approx({measurement_var}, 0.5)").alias("median"),
                     spark_min(measurement_var).alias("datamin"),
                     spark_max(measurement_var).alias("datamax"),
                     expr(f"percentile_approx({measurement_var}, 0.25)").alias("q1"),
                     expr(f"percentile_approx({measurement_var}, 0.75)").alias("q3")
                 ) \
                 .orderBy("AVISITN", treatment_num_var)
    
    logger.info(f"Calculated statistics for {stats_df.count()} visit/treatment combinations")
    return stats_df

def calculate_additional_metrics(df, config):
    """
    Calculate additional metrics needed for box plots and analysis.
    """
    logger.info("Calculating additional metrics")
    
    measurement_var = config['variables']['measurement_var']
    
    enhanced_stats = df.withColumn("iqr", col("q3") - col("q1")) \
                       .withColumn("lower_fence", col("q1") - 1.5 * (col("q3") - col("q1"))) \
                       .withColumn("upper_fence", col("q3") + 1.5 * (col("q3") - col("q1")))
    
    return enhanced_stats

def format_statistics(df, config):
    """
    Format statistics with appropriate precision.
    Replaces %util_value_format functionality from SAS
    """
    logger.info("Formatting statistics")
    
    formatted_df = df.withColumn("mean_formatted", expr("round(mean, 2)")) \
                     .withColumn("std_formatted", expr("round(std, 3)")) \
                     .withColumn("median_formatted", expr("round(median, 1)")) \
                     .withColumn("q1_formatted", expr("round(q1, 1)")) \
                     .withColumn("q3_formatted", expr("round(q3, 1)"))
    
    return formatted_df

def validate_statistics(df, config):
    """
    Validate calculated statistics for data quality.
    """
    logger.info("Validating statistics")
    
    zero_n_count = df.filter(col("n") == 0).count()
    if zero_n_count > 0:
        logger.warning(f"Found {zero_n_count} groups with zero observations")
    
    null_stats = df.filter(
        col("mean").isNull() | 
        col("std").isNull() | 
        col("median").isNull()
    ).count()
    
    if null_stats > 0:
        logger.warning(f"Found {null_stats} groups with null statistics")
    
    total_groups = df.count()
    logger.info(f"Statistics calculated for {total_groups} visit/treatment groups")
    
    logger.info("Sample statistics:")
    df.select("AVISITN", "AVISIT", config['variables']['treatment_var'], 
              "n", "mean_formatted", "std_formatted", "median_formatted").show(10)

def main():
    parser = argparse.ArgumentParser(description='Clinical Data Statistical Analysis')
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
        
        stats_df = calculate_summary_statistics(df, config)
        
        enhanced_stats_df = calculate_additional_metrics(stats_df, config)
        
        formatted_stats_df = format_statistics(enhanced_stats_df, config)
        
        validate_statistics(formatted_stats_df, config)
        
        formatted_stats_df.write.format("delta").mode("overwrite").saveAsTable(args.output_table)
        
        logger.info("Statistical analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Statistical analysis failed: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
