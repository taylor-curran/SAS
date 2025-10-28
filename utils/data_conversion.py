"""
Data Conversion Utilities
Handle conversion between SAS7BDAT files and PySpark DataFrames
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import logging

def get_spark_session():
    """Get or create Spark session"""
    return SparkSession.builder.getOrCreate()

def read_sas7bdat_file(file_path):
    """
    Read SAS7BDAT file using pandas
    
    Args:
        file_path (str): Path to SAS7BDAT file
    
    Returns:
        pandas.DataFrame: Data from SAS file
    """
    try:
        df = pd.read_sas(file_path, encoding='utf-8')
        logging.info(f"Successfully read SAS file: {file_path}")
        logging.info(f"Shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error reading SAS file {file_path}: {str(e)}")
        raise

def pandas_to_spark(pandas_df, spark_session=None):
    """
    Convert pandas DataFrame to PySpark DataFrame
    
    Args:
        pandas_df (pandas.DataFrame): Input pandas DataFrame
        spark_session: Optional Spark session
    
    Returns:
        pyspark.sql.DataFrame: Converted Spark DataFrame
    """
    if spark_session is None:
        spark_session = get_spark_session()
    
    try:
        spark_df = spark_session.createDataFrame(pandas_df)
        logging.info(f"Converted pandas DataFrame to Spark DataFrame")
        logging.info(f"Spark DataFrame schema: {spark_df.schema}")
        return spark_df
    except Exception as e:
        logging.error(f"Error converting pandas to Spark DataFrame: {str(e)}")
        raise

def infer_sas_schema(pandas_df):
    """
    Infer appropriate Spark schema from pandas DataFrame with SAS data
    
    Args:
        pandas_df (pandas.DataFrame): Input pandas DataFrame from SAS
    
    Returns:
        StructType: Spark schema
    """
    fields = []
    
    for col_name, dtype in pandas_df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            spark_type = IntegerType()
        elif pd.api.types.is_float_dtype(dtype):
            spark_type = DoubleType()
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            spark_type = TimestampType()
        elif pd.api.types.is_bool_dtype(dtype):
            spark_type = BooleanType()
        else:
            spark_type = StringType()
        
        fields.append(StructField(col_name, spark_type, True))
    
    return StructType(fields)

def convert_sas_to_spark(file_path, spark_session=None):
    """
    Complete conversion from SAS7BDAT file to PySpark DataFrame
    
    Args:
        file_path (str): Path to SAS7BDAT file
        spark_session: Optional Spark session
    
    Returns:
        pyspark.sql.DataFrame: Converted Spark DataFrame
    """
    if spark_session is None:
        spark_session = get_spark_session()
    
    pandas_df = read_sas7bdat_file(file_path)
    
    pandas_df = clean_sas_data(pandas_df)
    
    spark_df = pandas_to_spark(pandas_df, spark_session)
    
    return spark_df

def clean_sas_data(pandas_df):
    """
    Clean SAS data for better compatibility with Spark
    
    Args:
        pandas_df (pandas.DataFrame): Input pandas DataFrame from SAS
    
    Returns:
        pandas.DataFrame: Cleaned DataFrame
    """
    df_cleaned = pandas_df.copy()
    
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'object':
            df_cleaned[col] = df_cleaned[col].astype(str)
            df_cleaned[col] = df_cleaned[col].replace('nan', None)
            df_cleaned[col] = df_cleaned[col].replace('', None)
    
    for col in df_cleaned.select_dtypes(include=['float64']).columns:
        df_cleaned[col] = df_cleaned[col].replace([float('inf'), float('-inf')], None)
    
    logging.info(f"Cleaned SAS data: {df_cleaned.shape}")
    return df_cleaned

def save_spark_to_parquet(spark_df, output_path, mode="overwrite"):
    """
    Save Spark DataFrame to Parquet format
    
    Args:
        spark_df: PySpark DataFrame
        output_path (str): Output path for Parquet files
        mode (str): Write mode (overwrite, append, etc.)
    """
    try:
        spark_df.write.mode(mode).parquet(output_path)
        logging.info(f"Saved Spark DataFrame to Parquet: {output_path}")
    except Exception as e:
        logging.error(f"Error saving to Parquet: {str(e)}")
        raise

def load_spark_from_parquet(input_path, spark_session=None):
    """
    Load Spark DataFrame from Parquet format
    
    Args:
        input_path (str): Input path for Parquet files
        spark_session: Optional Spark session
    
    Returns:
        pyspark.sql.DataFrame: Loaded Spark DataFrame
    """
    if spark_session is None:
        spark_session = get_spark_session()
    
    try:
        spark_df = spark_session.read.parquet(input_path)
        logging.info(f"Loaded Spark DataFrame from Parquet: {input_path}")
        return spark_df
    except Exception as e:
        logging.error(f"Error loading from Parquet: {str(e)}")
        raise

def convert_all_sas_files(input_directory, output_directory):
    """
    Convert all SAS7BDAT files in a directory to Parquet format
    
    Args:
        input_directory (str): Directory containing SAS files
        output_directory (str): Directory to save Parquet files
    
    Returns:
        list: List of converted files
    """
    converted_files = []
    
    os.makedirs(output_directory, exist_ok=True)
    
    for filename in os.listdir(input_directory):
        if filename.endswith('.sas7bdat'):
            input_path = os.path.join(input_directory, filename)
            output_name = filename.replace('.sas7bdat', '.parquet')
            output_path = os.path.join(output_directory, output_name)
            
            try:
                spark_df = convert_sas_to_spark(input_path)
                save_spark_to_parquet(spark_df, output_path)
                converted_files.append(output_path)
                logging.info(f"Converted {filename} to {output_name}")
            except Exception as e:
                logging.error(f"Failed to convert {filename}: {str(e)}")
    
    return converted_files

def get_sas_file_info(file_path):
    """
    Get information about a SAS7BDAT file without fully loading it
    
    Args:
        file_path (str): Path to SAS7BDAT file
    
    Returns:
        dict: File information
    """
    try:
        df_sample = pd.read_sas(file_path, chunksize=1)
        first_chunk = next(df_sample)
        
        df_full = pd.read_sas(file_path)
        
        info = {
            'file_path': file_path,
            'columns': list(first_chunk.columns),
            'column_count': len(first_chunk.columns),
            'row_count': len(df_full),
            'dtypes': dict(first_chunk.dtypes),
            'file_size_mb': os.path.getsize(file_path) / (1024 * 1024)
        }
        
        return info
    except Exception as e:
        logging.error(f"Error getting SAS file info: {str(e)}")
        return {'error': str(e)}

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    test_file = "/home/ubuntu/repos/SAS/data/adam/adlbc.sas7bdat"
    
    if os.path.exists(test_file):
        print("Testing SAS file conversion...")
        
        info = get_sas_file_info(test_file)
        print(f"File info: {info}")
        
        spark_df = convert_sas_to_spark(test_file)
        print(f"Converted to Spark DataFrame with {spark_df.count()} rows")
        print(f"Schema: {spark_df.schema}")
    else:
        print(f"Test file not found: {test_file}")
