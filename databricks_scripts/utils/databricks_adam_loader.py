from typing import Dict, Optional, Union
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, DoubleType


class DatabricksAdamLoader:
    
    def __init__(self, spark: SparkSession, catalog: str = "main", schema: str = "sdtm"):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
    
    def load_domain(self, domain: str, source_path: Optional[str] = None, 
                    source_type: str = "delta") -> DataFrame:
        if source_type == "delta":
            table_name = f"{self.catalog}.{self.schema}.{domain.lower()}"
            df = self.spark.read.table(table_name)
        elif source_type == "xpt":
            if not source_path:
                raise ValueError(f"source_path required for XPT file loading")
            df = self._read_xpt_file(source_path)
        else:
            raise ValueError(f"Unsupported source_type: {source_type}")
        
        return df
    
    def _read_xpt_file(self, path: str) -> DataFrame:
        pandas_df = self.spark.sparkContext.parallelize([path]).toDF()
        
        try:
            import pandas as pd
            from xport import to_dataframe
            
            with open(path, 'rb') as f:
                xpt_data = to_dataframe(f)
            
            df = self.spark.createDataFrame(xpt_data)
            return df
        except Exception as e:
            raise RuntimeError(f"Error reading XPT file {path}: {str(e)}")
    
    def load_ae(self, source_path: Optional[str] = None, source_type: str = "delta") -> DataFrame:
        return self.load_domain("ae", source_path, source_type)
    
    def load_dm(self, source_path: Optional[str] = None, source_type: str = "delta") -> DataFrame:
        return self.load_domain("dm", source_path, source_type)
    
    def load_ex(self, source_path: Optional[str] = None, source_type: str = "delta") -> DataFrame:
        return self.load_domain("ex", source_path, source_type)
    
    def load_meddra_hierarchy(self, meddra_version: str = "14_0", 
                               source_path: Optional[str] = None,
                               source_type: str = "delta") -> DataFrame:
        if source_type == "delta":
            table_name = f"{self.catalog}.{self.schema}.mdhier_{meddra_version}"
            df = self.spark.read.table(table_name)
        else:
            df = self._read_xpt_file(source_path)
        
        return df
