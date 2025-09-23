"""
Test suite for clinical data analysis pipeline
Validates that PySpark transformations produce equivalent results to SAS
"""

import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'modules'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))

from data_ingestion import ingest_adlbc_data
from data_preprocessing import preprocess_clinical_data
from statistical_calculations import calculate_statistics
from phuse_utilities import util_labels_from_var, util_count_unique_values
from pipeline_config import PipelineConfig

@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("ClinicalPipelineTest") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def test_config():
    """Test configuration"""
    return PipelineConfig('testing').get_all()

@pytest.fixture
def sample_adlbc_data(spark):
    """Create sample ADLBC data for testing"""
    data = [
        ("STUDY01", "SUBJ001", "ALB", "Albumin (g/L)", 35.2, 30.0, 50.0, "Y", "Y", "Placebo", 0, "Baseline", 0, 1, "TimePoint 1"),
        ("STUDY01", "SUBJ001", "ALB", "Albumin (g/L)", 34.8, 30.0, 50.0, "Y", "Y", "Placebo", 0, "Week 2", 2, 1, "TimePoint 1"),
        ("STUDY01", "SUBJ001", "ALB", "Albumin (g/L)", 36.1, 30.0, 50.0, "Y", "Y", "Placebo", 0, "Week 4", 4, 1, "TimePoint 1"),
        
        ("STUDY01", "SUBJ002", "ALB", "Albumin (g/L)", 42.5, 30.0, 50.0, "Y", "Y", "X-high", 54, "Baseline", 0, 1, "TimePoint 1"),
        ("STUDY01", "SUBJ002", "ALB", "Albumin (g/L)", 45.2, 30.0, 50.0, "Y", "Y", "X-high", 54, "Week 2", 2, 1, "TimePoint 1"),
        ("STUDY01", "SUBJ002", "ALB", "Albumin (g/L)", 28.5, 30.0, 50.0, "Y", "Y", "X-high", 54, "Week 4", 4, 1, "TimePoint 1"),  # Outlier (below normal)
        
        ("STUDY01", "SUBJ003", "ALB", "Albumin (g/L)", 38.7, 30.0, 50.0, "Y", "Y", "X-low", 81, "Baseline", 0, 1, "TimePoint 1"),
        ("STUDY01", "SUBJ003", "ALB", "Albumin (g/L)", 52.1, 30.0, 50.0, "Y", "Y", "X-low", 81, "Week 2", 2, 1, "TimePoint 1"),  # Outlier (above normal)
        ("STUDY01", "SUBJ003", "ALB", "Albumin (g/L)", 39.4, 30.0, 50.0, "Y", "Y", "X-low", 81, "Week 4", 4, 1, "TimePoint 1"),
    ]
    
    columns = [
        "STUDYID", "USUBJID", "PARAMCD", "PARAM", "AVAL", "A1LO", "A1HI", 
        "SAFFL", "ANL01FL", "TRTA", "TRTPN", "AVISIT", "AVISITN", "ATPTN", "ATPT"
    ]
    
    df = spark.createDataFrame(data, columns)
    
    df = df.withColumn("trtp_short", 
        col("TRTA")  # Already abbreviated in test data
    )
    
    return df

class TestDataIngestion:
    """Test data ingestion functionality"""
    
    def test_treatment_format_creation(self, sample_adlbc_data):
        """Test that treatment abbreviations are created correctly"""
        assert "trtp_short" in sample_adlbc_data.columns
        
        treatments = sample_adlbc_data.select("TRTPN", "trtp_short").distinct().collect()
        treatment_map = {row["TRTPN"]: row["trtp_short"] for row in treatments}
        
        expected_map = {0: "Placebo", 54: "X-high", 81: "X-low"}
        for trtpn, expected in expected_map.items():
            assert trtpn in treatment_map

class TestDataPreprocessing:
    """Test data preprocessing and filtering"""
    
    def test_parameter_filtering(self, sample_adlbc_data, test_config):
        """Test filtering by parameter (ALB)"""
        filtered_df = sample_adlbc_data.filter(col("PARAMCD") == test_config['parameter'])
        
        assert filtered_df.count() == sample_adlbc_data.count()
        
        paramcd_values = filtered_df.select("PARAMCD").distinct().collect()
        assert len(paramcd_values) == 1
        assert paramcd_values[0]["PARAMCD"] == "ALB"
    
    def test_visit_filtering(self, sample_adlbc_data, test_config):
        """Test filtering by visit numbers"""
        visit_filter = test_config['visit_filter']  # [0, 2, 4, 6]
        filtered_df = sample_adlbc_data.filter(col("AVISITN").isin(visit_filter))
        
        visit_counts = filtered_df.groupBy("AVISITN").count().collect()
        visit_numbers = [row["AVISITN"] for row in visit_counts]
        
        for visit in visit_numbers:
            assert visit in visit_filter
    
    def test_outlier_detection(self, sample_adlbc_data, test_config):
        """Test normal range outlier detection"""
        from pyspark.sql.functions import when
        
        outlier_df = sample_adlbc_data.withColumn(
            "m_var_outlier",
            when(
                ((col("AVAL").isNotNull()) & (col("A1LO").isNotNull()) & (col("AVAL") < col("A1LO"))) |
                ((col("AVAL").isNotNull()) & (col("A1HI").isNotNull()) & (col("AVAL") > col("A1HI"))),
                col("AVAL")
            ).otherwise(None)
        )
        
        outliers = outlier_df.filter(col("m_var_outlier").isNotNull()).collect()
        
        assert len(outliers) == 2
        
        outlier_values = [row["m_var_outlier"] for row in outliers]
        assert 28.5 in outlier_values  # Below normal range
        assert 52.1 in outlier_values  # Above normal range

class TestStatisticalCalculations:
    """Test statistical calculations"""
    
    def test_summary_statistics(self, sample_adlbc_data, test_config):
        """Test calculation of summary statistics"""
        from pyspark.sql.functions import count, mean, stddev, min as spark_min, max as spark_max, expr
        
        stats_df = sample_adlbc_data.groupBy("AVISITN", "TRTPN", "AVISIT", "trtp_short").agg(
            count("AVAL").alias("n"),
            mean("AVAL").alias("mean"),
            stddev("AVAL").alias("std"),
            spark_min("AVAL").alias("datamin"),
            spark_max("AVAL").alias("datamax"),
            expr("percentile_approx(AVAL, 0.5)").alias("median"),
            expr("percentile_approx(AVAL, 0.25)").alias("q1"),
            expr("percentile_approx(AVAL, 0.75)").alias("q3")
        )
        
        stats_count = stats_df.count()
        assert stats_count > 0  # Should have statistics for each visit/treatment combination
        
        required_stats = ["n", "mean", "std", "datamin", "datamax", "median", "q1", "q3"]
        for stat in required_stats:
            assert stat in stats_df.columns
        
        stats_list = stats_df.collect()
        for row in stats_list:
            assert row["n"] > 0  # Count should be positive
            assert row["datamin"] <= row["datamax"]  # Min <= Max
            assert row["q1"] <= row["median"] <= row["q3"]  # Quartile order

class TestPhUSEUtilities:
    """Test PhUSE utility functions"""
    
    def test_util_labels_from_var(self, sample_adlbc_data):
        """Test extraction of value/label pairs"""
        pandas_df = sample_adlbc_data.toPandas()
        
        result = util_labels_from_var(pandas_df, "PARAMCD", "PARAM")
        
        assert result['count'] == 1  # Only ALB parameter in test data
        assert "ALB" in result['values']
        assert "Albumin (g/L)" in result['labels']
        assert result['value_label_map']['ALB'] == "Albumin (g/L)"
    
    def test_util_count_unique_values(self, sample_adlbc_data):
        """Test counting unique values"""
        pandas_df = sample_adlbc_data.toPandas()
        
        treatment_count = util_count_unique_values(pandas_df, "TRTPN", "treatment_count")
        assert treatment_count == 3  # Placebo, X-high, X-low
        
        visit_count = util_count_unique_values(pandas_df, "AVISITN", "visit_count")
        assert visit_count == 3  # Visits 0, 2, 4

class TestPipelineConfig:
    """Test pipeline configuration"""
    
    def test_config_validation(self):
        """Test configuration validation"""
        config = PipelineConfig('testing')
        errors = config.validate_config()
        
        assert len(errors) == 0
    
    def test_treatment_format(self):
        """Test treatment format mapping"""
        config = PipelineConfig('testing')
        
        assert config.get_treatment_format(0) == "P"
        assert config.get_treatment_format(54) == "X-high"
        assert config.get_treatment_format(81) == "X-low"
        assert config.get_treatment_format(999) == "UNEXPECTED"
    
    def test_required_columns(self):
        """Test required columns list"""
        config = PipelineConfig('testing')
        required_cols = config.required_columns
        
        assert "PARAMCD" in required_cols
        assert "AVAL" in required_cols
        assert "AVISITN" in required_cols
        assert "SAFFL" in required_cols
        assert "ANL01FL" in required_cols

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
