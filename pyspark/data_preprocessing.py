from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, lit, format_string
from pyspark.sql.types import StringType
import os
import logging

def get_spark_session():
    """Get or create Spark session"""
    return SparkSession.builder.getOrCreate()

def apply_data_filters(df, config):
    """
    Apply data filtering logic converted from SAS WHERE clauses
    Replicates WPCT-F.07.01.sas lines 240-249
    
    Original SAS logic:
    where paramcd = "&m_var" and 
          not missing(aval) and
          not missing(visit) and
          not missing(trta) and
          anl01fl = 'Y' and
          trta ^= '' and
          avisitn ^= 99
    """
    measurement_var = config['measurement_variable']
    population_flag = config['population_flag']
    analysis_flag = config['analysis_flag']
    
    filtered_df = df.filter(
        (col("PARAMCD") == measurement_var) &
        (col("AVAL").isNotNull()) &
        (~isnan(col("AVAL"))) &
        (col("AVISIT").isNotNull()) &
        (col("TRTP").isNotNull()) &
        (col(analysis_flag) == "Y") &
        (col("TRTP") != "") &
        (col("AVISITN") != 99)
    )
    
    if population_flag in df.columns:
        filtered_df = filtered_df.filter(col(population_flag) == "Y")
    
    return filtered_df

def create_treatment_format(df):
    """
    Create short treatment labels as done in original SAS code
    Replicates the PROC FORMAT and treatment mapping from WPCT-F.07.01.sas lines 186-201
    """
    treatment_mapping = {
        0: 'P',
        54: 'X-high', 
        81: 'X-low'
    }
    
    df_with_short_trtp = df.withColumn(
        "trtp_short",
        when(col("TRTPN") == 0, "P")
        .when(col("TRTPN") == 54, "X-high")
        .when(col("TRTPN") == 81, "X-low")
        .otherwise("UNEXPECTED")
    )
    
    return df_with_short_trtp

def create_outlier_variable(df, config):
    """
    Create normal range outlier variable for scatter plot overlay
    Replicates WPCT-F.07.01.sas lines 245-248
    
    Original SAS logic:
    if (2 = n(&m_var, &lo_var) and &m_var < &lo_var) or
       (2 = n(&m_var, &hi_var) and &m_var > &hi_var) then m_var_outlier = &m_var;
    else m_var_outlier = .;
    """
    measurement_var = config['measurement_var']
    lo_var = "A1LO"  
    hi_var = "A1HI"  
    
    df_with_outliers = df.withColumn(
        "m_var_outlier",
        when(
            ((col("AVAL").isNotNull()) & (col(lo_var).isNotNull()) & (col("AVAL") < col(lo_var))) |
            ((col("AVAL").isNotNull()) & (col(hi_var).isNotNull()) & (col("AVAL") > col(hi_var))),
            col("AVAL")
        ).otherwise(None)
    )
    
    return df_with_outliers

def apply_visit_conditions(df, config):
    """
    Apply visit-specific filtering conditions
    Handles the visit condition from config (e.g., "and AVISITN in (0 2 4 6)")
    """
    visit_condition = config.get('visit_condition', '')
    
    if 'AVISITN in (0 2 4 6)' in visit_condition or 'AVISITN in (0,2,4,6)' in visit_condition:
        df = df.filter(col("AVISITN").isin([0, 2, 4, 6]))
    
    return df

def preprocess_clinical_data(config):
    """
    Main data preprocessing function
    
    Args:
        config (dict): Configuration containing filtering parameters
    
    Returns:
        dict: Status and metadata about preprocessed data
    """
    spark = get_spark_session()
    
    input_path = config['input_path']
    output_path = config['output_path']
    
    df = spark.read.parquet(input_path)
    
    df_filtered = apply_data_filters(df, config)
    
    df_with_treatments = create_treatment_format(df_filtered)
    
    df_with_outliers = create_outlier_variable(df_with_treatments, config)
    
    df_final = apply_visit_conditions(df_with_outliers, config)
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_final.write.mode("overwrite").parquet(output_path)
    
    result = {
        'status': 'success',
        'input_rows': df.count(),
        'output_rows': df_final.count(),
        'columns': len(df_final.columns),
        'output_path': output_path
    }
    
    logging.info(f"Data preprocessing completed: {result}")
    return result

if __name__ == "__main__":
    test_config = {
        'measurement_variable': 'ALB',
        'visit_condition': 'and AVISITN in (0 2 4 6)',
        'population_flag': 'SAFFL',
        'analysis_flag': 'ANL01FL',
        'measurement_var': 'aval',
        'input_path': '/tmp/clinical_analysis/raw_data',
        'output_path': '/tmp/clinical_analysis/preprocessed_data'
    }
    
    result = preprocess_clinical_data(test_config)
    print(f"Preprocessing result: {result}")
