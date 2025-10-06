"""
DatabricksAdamLoader - Load SDTM/ADaM domains from Delta Lake tables or XPT files

This loader provides flexible data access for Databricks environments, supporting:
- Delta Lake tables using catalog.schema.table format
- XPT file formats from DBFS or mounted storage
- Automatic schema inference and validation
"""

from typing import Optional, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import os


class DatabricksAdamLoader:
    """
    Flexible loader for SDTM/ADaM datasets in Databricks environment.
    
    Supports loading from:
    1. Delta Lake tables (catalog.schema.table format)
    2. XPT files (SAS transport format)
    3. SAS7BDAT files
    
    Usage:
        loader = DatabricksAdamLoader(spark, source_type="delta", 
                                      catalog="clinical_dev", schema="sdtm")
        ae_df = loader.load_domain("ae")
        dm_df = loader.load_domain("dm")
    """
    
    def __init__(
        self, 
        spark: SparkSession,
        source_type: str = "delta",
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        file_path: Optional[str] = None
    ):
        """
        Initialize the loader.
        
        Args:
            spark: Active SparkSession
            source_type: "delta", "xpt", or "sas7bdat"
            catalog: Catalog name for Delta tables (required if source_type="delta")
            schema: Schema name for Delta tables (required if source_type="delta")
            file_path: Base path for file-based sources (required if source_type != "delta")
        """
        self.spark = spark
        self.source_type = source_type.lower()
        self.catalog = catalog
        self.schema = schema
        self.file_path = file_path
        
        if self.source_type == "delta":
            if not catalog or not schema:
                raise ValueError("catalog and schema are required for Delta source_type")
        elif self.source_type in ["xpt", "sas7bdat"]:
            if not file_path:
                raise ValueError("file_path is required for file-based source_type")
        else:
            raise ValueError(f"Invalid source_type: {source_type}. Must be 'delta', 'xpt', or 'sas7bdat'")
    
    def load_domain(
        self, 
        domain: str,
        columns: Optional[List[str]] = None,
        filter_condition: Optional[str] = None
    ) -> DataFrame:
        """
        Load a specific SDTM/ADaM domain.
        
        Args:
            domain: Domain name (e.g., "ae", "dm", "ex")
            columns: Optional list of columns to select
            filter_condition: Optional SQL WHERE clause filter
            
        Returns:
            DataFrame containing the domain data
        """
        domain_lower = domain.lower()
        
        if self.source_type == "delta":
            df = self._load_from_delta(domain_lower)
        elif self.source_type == "xpt":
            df = self._load_from_xpt(domain_lower)
        elif self.source_type == "sas7bdat":
            df = self._load_from_sas7bdat(domain_lower)
        
        df = df.toDF(*[c.upper() for c in df.columns])
        
        if columns:
            df = df.select([F.col(c.upper()) for c in columns])
        
        if filter_condition:
            df = df.filter(filter_condition)
        
        return df
    
    def _load_from_delta(self, domain: str) -> DataFrame:
        """Load domain from Delta Lake table."""
        table_name = f"{self.catalog}.{self.schema}.{domain}"
        return self.spark.table(table_name)
    
    def _load_from_xpt(self, domain: str) -> DataFrame:
        """Load domain from XPT file."""
        file_path = os.path.join(self.file_path, f"{domain}.xpt")
        return (self.spark.read
                .format("com.github.saurfang.sas.spark")
                .option("format", "xpt")
                .load(file_path))
    
    def _load_from_sas7bdat(self, domain: str) -> DataFrame:
        """Load domain from SAS7BDAT file."""
        file_path = os.path.join(self.file_path, f"{domain}.sas7bdat")
        return (self.spark.read
                .format("com.github.saurfang.sas.spark")
                .load(file_path))
    
    def write_delta_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None
    ) -> None:
        """
        Write DataFrame to Delta Lake table.
        
        Args:
            df: DataFrame to write
            table_name: Name of the table (will be prefixed with catalog.schema)
            mode: Write mode ("overwrite", "append", "error", "ignore")
            partition_by: Optional list of columns to partition by
        """
        full_table_name = f"{self.catalog}.{self.schema}.{table_name}"
        
        writer = df.write.format("delta").mode(mode)
        
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.saveAsTable(full_table_name)
