from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull
import logging

def get_spark_session():
    """Get or create Spark session"""
    return SparkSession.builder.getOrCreate()

def detect_iqr_outliers(df, measurement_col, group_cols=None):
    """
    Detect IQR-based outliers (values beyond 1.5 * IQR from Q1/Q3)
    This replicates the box plot outlier detection logic from SAS
    """
    from pyspark.sql.functions import expr
    from pyspark.sql.window import Window
    
    if group_cols is None:
        group_cols = []
    
    if group_cols:
        window_spec = Window.partitionBy(*group_cols)
    else:
        window_spec = Window.partitionBy()
    
    df_with_quartiles = df.withColumn(
        "q1", expr(f"percentile_approx({measurement_col}, 0.25) OVER ({window_spec})")
    ).withColumn(
        "q3", expr(f"percentile_approx({measurement_col}, 0.75) OVER ({window_spec})")
    ).withColumn(
        "iqr", col("q3") - col("q1")
    ).withColumn(
        "lower_fence", col("q1") - 1.5 * col("iqr")
    ).withColumn(
        "upper_fence", col("q3") + 1.5 * col("iqr")
    )
    
    df_with_outliers = df_with_quartiles.withColumn(
        "is_iqr_outlier",
        when(
            (col(measurement_col) < col("lower_fence")) | 
            (col(measurement_col) > col("upper_fence")),
            True
        ).otherwise(False)
    ).withColumn(
        "outlier_value",
        when(col("is_iqr_outlier"), col(measurement_col)).otherwise(None)
    )
    
    return df_with_outliers

def detect_normal_range_outliers(df, measurement_col, low_limit_col, high_limit_col):
    """
    Detect normal range outliers (values outside reference range)
    Replicates the normal range outlier logic from WPCT-F.07.01.sas lines 245-248
    """
    df_with_normal_outliers = df.withColumn(
        "is_normal_range_outlier",
        when(
            ((col(measurement_col).isNotNull()) & (col(low_limit_col).isNotNull()) & 
             (col(measurement_col) < col(low_limit_col))) |
            ((col(measurement_col).isNotNull()) & (col(high_limit_col).isNotNull()) & 
             (col(measurement_col) > col(high_limit_col))),
            True
        ).otherwise(False)
    ).withColumn(
        "normal_range_outlier_value",
        when(col("is_normal_range_outlier"), col(measurement_col)).otherwise(None)
    )
    
    return df_with_normal_outliers

def comprehensive_outlier_detection(df, config):
    """
    Perform comprehensive outlier detection combining IQR and normal range methods
    
    Args:
        df: PySpark DataFrame with clinical data
        config: Configuration dictionary with outlier detection parameters
    
    Returns:
        DataFrame with outlier flags and values added
    """
    measurement_col = config.get('measurement_col', 'AVAL')
    low_limit_col = config.get('low_limit_col', 'A1LO')
    high_limit_col = config.get('high_limit_col', 'A1HI')
    group_cols = config.get('group_cols', ['PARAMCD', 'AVISITN', 'TRTP'])
    
    df_with_iqr_outliers = detect_iqr_outliers(df, measurement_col, group_cols)
    
    df_with_all_outliers = detect_normal_range_outliers(
        df_with_iqr_outliers, 
        measurement_col, 
        low_limit_col, 
        high_limit_col
    )
    
    df_final = df_with_all_outliers.withColumn(
        "any_outlier",
        col("is_iqr_outlier") | col("is_normal_range_outlier")
    ).withColumn(
        "outlier_type",
        when(col("is_iqr_outlier") & col("is_normal_range_outlier"), "BOTH")
        .when(col("is_iqr_outlier"), "IQR")
        .when(col("is_normal_range_outlier"), "NORMAL_RANGE")
        .otherwise("NONE")
    )
    
    return df_final

def get_outlier_summary(df):
    """
    Generate summary statistics about outliers detected
    """
    from pyspark.sql.functions import sum as spark_sum, count
    
    summary = df.agg(
        count("*").alias("total_observations"),
        spark_sum(col("is_iqr_outlier").cast("int")).alias("iqr_outliers"),
        spark_sum(col("is_normal_range_outlier").cast("int")).alias("normal_range_outliers"),
        spark_sum(col("any_outlier").cast("int")).alias("total_outliers")
    ).collect()[0]
    
    return {
        'total_observations': summary['total_observations'],
        'iqr_outliers': summary['iqr_outliers'],
        'normal_range_outliers': summary['normal_range_outliers'],
        'total_outliers': summary['total_outliers'],
        'outlier_percentage': (summary['total_outliers'] / summary['total_observations']) * 100
    }

if __name__ == "__main__":
    spark = get_spark_session()
    
    test_config = {
        'measurement_col': 'AVAL',
        'low_limit_col': 'A1LO',
        'high_limit_col': 'A1HI',
        'group_cols': ['PARAMCD', 'AVISITN', 'TRTP']
    }
    
    print("Outlier detection module ready for use")
