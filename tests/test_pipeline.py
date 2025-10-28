import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import tempfile
import os
import sys

sys.path.append('/home/ubuntu/repos/SAS')

from pyspark.data_ingestion import ingest_clinical_data
from pyspark.data_preprocessing import preprocess_clinical_data
from pyspark.statistical_analysis import calculate_statistics
from utils.phuse_utilities import util_labels_from_var, util_count_unique_values
from utils.data_conversion import convert_sas_to_spark

@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("ClinicalDataAnalysisTest") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_clinical_data(spark_session):
    """Create sample clinical data for testing"""
    schema = StructType([
        StructField("STUDYID", StringType(), True),
        StructField("USUBJID", StringType(), True),
        StructField("PARAMCD", StringType(), True),
        StructField("PARAM", StringType(), True),
        StructField("AVAL", DoubleType(), True),
        StructField("TRTP", StringType(), True),
        StructField("TRTPN", IntegerType(), True),
        StructField("AVISITN", IntegerType(), True),
        StructField("AVISIT", StringType(), True),
        StructField("A1LO", DoubleType(), True),
        StructField("A1HI", DoubleType(), True),
        StructField("SAFFL", StringType(), True),
        StructField("ANL01FL", StringType(), True),
    ])
    
    data = [
        ("STUDY001", "SUBJ001", "ALB", "Albumin", 35.0, "Placebo", 0, 0, "Baseline", 30.0, 50.0, "Y", "Y"),
        ("STUDY001", "SUBJ001", "ALB", "Albumin", 37.0, "Placebo", 0, 2, "Week 2", 30.0, 50.0, "Y", "Y"),
        ("STUDY001", "SUBJ002", "ALB", "Albumin", 42.0, "Treatment High", 54, 0, "Baseline", 30.0, 50.0, "Y", "Y"),
        ("STUDY001", "SUBJ002", "ALB", "Albumin", 45.0, "Treatment High", 54, 2, "Week 2", 30.0, 50.0, "Y", "Y"),
        ("STUDY001", "SUBJ003", "ALB", "Albumin", 38.0, "Treatment Low", 81, 0, "Baseline", 30.0, 50.0, "Y", "Y"),
        ("STUDY001", "SUBJ003", "ALB", "Albumin", 40.0, "Treatment Low", 81, 2, "Week 2", 30.0, 50.0, "Y", "Y"),
        ("STUDY001", "SUBJ004", "ALB", "Albumin", 25.0, "Placebo", 0, 0, "Baseline", 30.0, 50.0, "Y", "Y"),  # Below normal
        ("STUDY001", "SUBJ005", "ALB", "Albumin", 55.0, "Treatment High", 54, 0, "Baseline", 30.0, 50.0, "Y", "Y"),  # Above normal
    ]
    
    df = spark_session.createDataFrame(data, schema)
    return df

@pytest.fixture
def temp_directory():
    """Create temporary directory for testing"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir

class TestDataIngestion:
    """Test data ingestion functionality"""
    
    def test_ingest_clinical_data_structure(self, sample_clinical_data, temp_directory):
        """Test that data ingestion preserves data structure"""
        input_path = os.path.join(temp_directory, "input")
        output_path = os.path.join(temp_directory, "output")
        
        sample_clinical_data.write.mode("overwrite").parquet(input_path)
        
        config = {
            'dataset': 'test_data',
            'data_path': temp_directory,
            'output_path': output_path
        }
        
        from pyspark.data_ingestion import add_timepoint_variables
        
        result_df = add_timepoint_variables(sample_clinical_data)
        
        assert "ATPTN" in result_df.columns
        assert "ATPT" in result_df.columns
        assert result_df.filter(result_df.ATPTN == 1).count() == sample_clinical_data.count()

class TestDataPreprocessing:
    """Test data preprocessing functionality"""
    
    def test_data_filtering(self, sample_clinical_data, temp_directory):
        """Test that data filtering works correctly"""
        input_path = os.path.join(temp_directory, "input")
        output_path = os.path.join(temp_directory, "output")
        
        sample_clinical_data.write.mode("overwrite").parquet(input_path)
        
        config = {
            'measurement_variable': 'ALB',
            'visit_condition': 'and AVISITN in (0 2)',
            'population_flag': 'SAFFL',
            'analysis_flag': 'ANL01FL',
            'measurement_var': 'AVAL',
            'input_path': input_path,
            'output_path': output_path
        }
        
        result = preprocess_clinical_data(config)
        
        assert result['status'] == 'success'
        assert result['output_rows'] > 0
        assert result['output_rows'] <= result['input_rows']
    
    def test_outlier_detection(self, sample_clinical_data):
        """Test outlier detection logic"""
        from pyspark.data_preprocessing import create_outlier_variable
        
        config = {'measurement_var': 'AVAL'}
        result_df = create_outlier_variable(sample_clinical_data, config)
        
        assert "m_var_outlier" in result_df.columns
        
        outliers = result_df.filter(result_df.m_var_outlier.isNotNull()).collect()
        assert len(outliers) >= 2  # We have values at 25.0 and 55.0 which are outside 30-50 range

class TestStatisticalAnalysis:
    """Test statistical analysis functionality"""
    
    def test_summary_statistics(self, sample_clinical_data, temp_directory):
        """Test statistical calculations"""
        input_path = os.path.join(temp_directory, "input")
        output_path = os.path.join(temp_directory, "output")
        
        from pyspark.data_preprocessing import create_treatment_format
        processed_df = create_treatment_format(sample_clinical_data)
        processed_df.write.mode("overwrite").parquet(input_path)
        
        config = {
            'treatment_var': 'trtp_short',
            'treatment_num_var': 'TRTPN',
            'measurement_var': 'AVAL',
            'visit_var': 'AVISITN',
            'reference_lines': 'UNIFORM',
            'input_path': input_path,
            'output_path': output_path
        }
        
        result = calculate_statistics(config)
        
        assert result['status'] == 'success'
        assert 'reference_lines' in result
        assert 'axis_range' in result
    
    def test_reference_lines_calculation(self, sample_clinical_data):
        """Test reference lines calculation"""
        from pyspark.statistical_analysis import calculate_reference_lines
        
        config = {'reference_lines': 'UNIFORM'}
        ref_lines = calculate_reference_lines(sample_clinical_data, config)
        
        assert len(ref_lines) == 2
        assert 30.0 in ref_lines
        assert 50.0 in ref_lines

class TestPhUSEUtilities:
    """Test PhUSE utility functions"""
    
    def test_util_labels_from_var(self, sample_clinical_data):
        """Test label extraction utility"""
        labels = util_labels_from_var(sample_clinical_data, "PARAMCD", "PARAM")
        
        assert "ALB" in labels
        assert labels["ALB"] == "Albumin"
    
    def test_util_count_unique_values(self, sample_clinical_data):
        """Test unique value counting utility"""
        count = util_count_unique_values(sample_clinical_data, "TRTP")
        
        assert count == 3  # Placebo, Treatment High, Treatment Low
    
    def test_util_get_reference_lines(self, sample_clinical_data):
        """Test reference lines utility"""
        from utils.phuse_utilities import util_get_reference_lines
        
        ref_lines = util_get_reference_lines(sample_clinical_data, "A1LO", "A1HI", "UNIFORM")
        
        assert len(ref_lines) == 2
        assert 30.0 in ref_lines
        assert 50.0 in ref_lines

class TestDataConversion:
    """Test data conversion utilities"""
    
    def test_pandas_to_spark_conversion(self, spark_session):
        """Test pandas to Spark DataFrame conversion"""
        from utils.data_conversion import pandas_to_spark
        
        pandas_df = pd.DataFrame({
            'PARAMCD': ['ALB', 'ALB'],
            'AVAL': [35.0, 42.0],
            'TRTP': ['Placebo', 'Treatment']
        })
        
        spark_df = pandas_to_spark(pandas_df, spark_session)
        
        assert spark_df.count() == 2
        assert len(spark_df.columns) == 3
        assert 'PARAMCD' in spark_df.columns

class TestIntegration:
    """Integration tests for the complete pipeline"""
    
    def test_end_to_end_pipeline(self, sample_clinical_data, temp_directory):
        """Test complete pipeline execution"""
        raw_path = os.path.join(temp_directory, "raw")
        preprocessed_path = os.path.join(temp_directory, "preprocessed")
        stats_path = os.path.join(temp_directory, "stats")
        
        sample_clinical_data.write.mode("overwrite").parquet(raw_path)
        
        preprocess_config = {
            'measurement_variable': 'ALB',
            'visit_condition': 'and AVISITN in (0 2)',
            'population_flag': 'SAFFL',
            'analysis_flag': 'ANL01FL',
            'measurement_var': 'AVAL',
            'input_path': raw_path,
            'output_path': preprocessed_path
        }
        
        preprocess_result = preprocess_clinical_data(preprocess_config)
        assert preprocess_result['status'] == 'success'
        
        stats_config = {
            'treatment_var': 'trtp_short',
            'treatment_num_var': 'TRTPN',
            'measurement_var': 'AVAL',
            'visit_var': 'AVISITN',
            'reference_lines': 'UNIFORM',
            'input_path': preprocessed_path,
            'output_path': stats_path
        }
        
        stats_result = calculate_statistics(stats_config)
        assert stats_result['status'] == 'success'
        
        spark = SparkSession.builder.getOrCreate()
        final_df = spark.read.parquet(stats_path)
        
        assert final_df.count() > 0
        assert 'mean' in final_df.columns
        assert 'std' in final_df.columns
        assert 'n' in final_df.columns

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
