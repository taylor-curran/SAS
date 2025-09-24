from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev, expr, min as spark_min, max as spark_max
import os
import logging

def get_spark_session():
    """Get or create Spark session"""
    return SparkSession.builder.getOrCreate()

def calculate_summary_statistics(df, config):
    """
    Calculate summary statistics using PySpark groupBy().agg()
    Replaces PROC SUMMARY from WPCT-F.07.01.sas lines 343-348
    
    Original SAS code:
    proc summary data=css_nexttimept noprint;
      by avisitn &tn_var avisit &t_var;
      var &m_var;
      output out=css_stats (drop=_type_ _freq_)
             n=n mean=mean std=std median=median min=datamin max=datamax q1=q1 q3=q3;
    run;
    """
    treatment_var = config['treatment_var']
    treatment_num_var = config['treatment_num_var']
    measurement_var = config['measurement_var']
    visit_var = config['visit_var']
    
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
    
    return stats_df

def calculate_reference_lines(df, config):
    """
    Calculate reference lines based on normal range limits
    Replicates util_get_reference_lines functionality
    """
    reference_lines_type = config.get('reference_lines', 'UNIFORM')
    
    if reference_lines_type == 'NONE':
        return []
    elif reference_lines_type == 'UNIFORM':
        lo_values = df.select("A1LO").distinct().collect()
        hi_values = df.select("A1HI").distinct().collect()
        
        if len(lo_values) == 1 and len(hi_values) == 1:
            lo_val = lo_values[0]['A1LO']
            hi_val = hi_values[0]['A1HI']
            if lo_val is not None and hi_val is not None:
                return [lo_val, hi_val]
    elif reference_lines_type == 'NARROW':
        lo_val = df.agg(spark_max("A1LO")).collect()[0][0]
        hi_val = df.agg(spark_min("A1HI")).collect()[0][0]
        if lo_val is not None and hi_val is not None:
            return [lo_val, hi_val]
    
    return []

def get_axis_range(df, measurement_var, reference_lines=None):
    """
    Calculate Y-axis range for plotting
    Replicates util_get_var_min_max functionality
    """
    min_val = df.agg(spark_min(measurement_var)).collect()[0][0]
    max_val = df.agg(spark_max(measurement_var)).collect()[0][0]
    
    if reference_lines:
        min_val = min(min_val, min(reference_lines))
        max_val = max(max_val, max(reference_lines))
    
    range_val = max_val - min_val
    padding = range_val * 0.1
    
    return {
        'min': min_val - padding,
        'max': max_val + padding,
        'range': range_val
    }

def format_statistics(stats_df, measurement_var):
    """
    Format statistical values for display
    Replicates util_value_format functionality
    """
    from pyspark.sql.functions import round as spark_round, format_number
    
    formatted_df = stats_df.withColumn("mean_formatted", spark_round("mean", 2)) \
                           .withColumn("std_formatted", spark_round("std", 3)) \
                           .withColumn("median_formatted", spark_round("median", 1)) \
                           .withColumn("q1_formatted", spark_round("q1", 1)) \
                           .withColumn("q3_formatted", spark_round("q3", 1))
    
    return formatted_df

def calculate_statistics(config):
    """
    Main statistical analysis function
    
    Args:
        config (dict): Configuration containing analysis parameters
    
    Returns:
        dict: Status and metadata about statistical calculations
    """
    spark = get_spark_session()
    
    input_path = config['input_path']
    output_path = config['output_path']
    
    df = spark.read.parquet(input_path)
    
    stats_df = calculate_summary_statistics(df, config)
    
    reference_lines = calculate_reference_lines(df, config)
    
    axis_range = get_axis_range(df, config['measurement_var'], reference_lines)
    
    formatted_stats = format_statistics(stats_df, config['measurement_var'])
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    formatted_stats.write.mode("overwrite").parquet(output_path)
    
    metadata = {
        'reference_lines': reference_lines,
        'axis_range': axis_range,
        'parameter_count': df.select("PARAMCD").distinct().count(),
        'treatment_count': df.select(config['treatment_var']).distinct().count(),
        'visit_count': df.select("AVISITN").distinct().count()
    }
    
    metadata_df = spark.createDataFrame([metadata])
    metadata_df.write.mode("overwrite").parquet(output_path + "_metadata")
    
    result = {
        'status': 'success',
        'statistics_rows': formatted_stats.count(),
        'reference_lines': reference_lines,
        'axis_range': axis_range,
        'output_path': output_path
    }
    
    logging.info(f"Statistical analysis completed: {result}")
    return result

if __name__ == "__main__":
    test_config = {
        'treatment_var': 'trtp_short',
        'treatment_num_var': 'trtpn',
        'measurement_var': 'aval',
        'visit_var': 'avisitn',
        'reference_lines': 'UNIFORM',
        'input_path': '/tmp/clinical_analysis/preprocessed_data',
        'output_path': '/tmp/clinical_analysis/statistics'
    }
    
    result = calculate_statistics(test_config)
    print(f"Statistical analysis result: {result}")
