# Extending SAS to Airflow Migration with OpenLineage and OpenMetadata

This guide explains how to extend the SAS-to-Airflow migration approach to other pharmaceutical workflows while maintaining comprehensive data lineage tracking and metadata management.

## Migration Process Overview

### 1. SAS Workflow Analysis

Before migrating a SAS workflow, analyze the following components:

- **Data Sources**: Identify all input datasets (SDTM, ADaM, derived datasets)
- **Transformations**: Map SAS procedures (PROC SUMMARY, PROC SQL, DATA steps)
- **Dependencies**: Understand data flow between SAS programs
- **Outputs**: Catalog all generated datasets, reports, and visualizations
- **Macros**: Identify reusable SAS macros that need Python equivalents

### 2. Airflow DAG Design

Structure your Airflow DAG to mirror the SAS workflow:

```python
# Example: Migrating a SAS efficacy analysis workflow
from airflow import DAG
from airflow.operators.bash import BashOperator
try:
    from airflow_provider_openmetadata.lineage.callback import success_callback, failure_callback
except ImportError:
    success_callback = None
    failure_callback = None

dag = DAG(
    'efficacy_analysis_pipeline',
    default_args={
        'on_failure_callback': failure_callback,
        'on_success_callback': success_callback,
    },
    tags=['efficacy', 'clinical', 'pharmaceutical'],
)

# Data ingestion with lineage tracking
ingest_efficacy_data = BashOperator(
    task_id='ingest_efficacy_data',
    bash_command='python scripts/ingest_efficacy.py',
    outlets={"tables": ["clinical_data.adam.adeff", "clinical_data.adam.adsl"]},
    dag=dag,
)

# Statistical analysis
efficacy_analysis = BashOperator(
    task_id='efficacy_analysis',
    bash_command='python scripts/efficacy_analysis.py',
    inlets={"tables": ["clinical_data.adam.adeff", "clinical_data.adam.adsl"]},
    outlets={"tables": ["clinical_data.analysis.efficacy_results"]},
    dag=dag,
)
```

### 3. Data Lineage Configuration

For each task, specify the data lineage using `inlets` and `outlets`:

- **Inlets**: Input datasets consumed by the task
- **Outlets**: Output datasets produced by the task
- **Naming Convention**: Use fully qualified names following the pattern:
  `{service}.{database}.{schema}.{table}`

### 4. Schema Definition

Add schema definitions for new datasets to `pharmaceutical_schemas.py`:

```python
# Add to ADAM_SCHEMAS dictionary
"adeff": {
    "description": "Analysis Dataset for Efficacy Endpoints",
    "domain": "Efficacy",
    "standard": "CDISC ADaM",
    "columns": [
        {"name": "studyid", "type": "string", "description": "Study Identifier"},
        {"name": "usubjid", "type": "string", "description": "Unique Subject Identifier"},
        {"name": "paramcd", "type": "string", "description": "Parameter Code"},
        {"name": "param", "type": "string", "description": "Parameter"},
        {"name": "aval", "type": "numeric", "description": "Analysis Value"},
        {"name": "avalc", "type": "string", "description": "Analysis Value (Character)"},
        # Add more columns as needed
    ]
}
```

### 5. Compliance Considerations

Ensure your migration maintains pharmaceutical compliance:

- **Audit Trails**: All data transformations are tracked via OpenLineage
- **Validation**: Include data quality checks in your pipeline
- **Documentation**: Maintain comprehensive documentation of changes
- **Testing**: Validate that results match original SAS outputs
- **Retention**: Configure appropriate data retention policies

## Common SAS to Python Patterns

### PROC SUMMARY → PySpark GroupBy
```python
# SAS: proc summary data=input_data;
#        by treatment visit;
#        var lab_value;
#        output out=summary_stats mean=mean_val std=std_val;
#      run;

# Python/PySpark equivalent:
from pyspark.sql import functions as F

summary_stats = df.groupBy("treatment", "visit").agg(
    F.mean("lab_value").alias("mean_val"),
    F.stddev("lab_value").alias("std_val")
)
```

### PROC SQL → PySpark SQL
```python
# SAS: proc sql;
#        create table filtered_data as
#        select * from input_data
#        where saffl = 'Y' and anl01fl = 'Y';
#      quit;

# Python/PySpark equivalent:
from pyspark.sql import functions as F

filtered_data = df.filter(
    (F.col("saffl") == "Y") & (F.col("anl01fl") == "Y")
)
```

### DATA Step → PySpark DataFrame Operations
```python
# SAS: data derived_data;
#        set input_data;
#        if param = 'GLUCOSE' then do;
#          if aval > 200 then high_flag = 'Y';
#          else high_flag = 'N';
#        end;
#      run;

# Python/PySpark equivalent:
from pyspark.sql import functions as F

derived_data = df.withColumn(
    "high_flag",
    F.when(
        (F.col("param") == "GLUCOSE") & (F.col("aval") > 200), "Y"
    ).otherwise("N")
)
```

## Testing and Validation

1. **Unit Tests**: Test individual transformation functions
2. **Integration Tests**: Validate end-to-end pipeline execution
3. **Data Validation**: Compare outputs with original SAS results
4. **Lineage Verification**: Confirm lineage data is captured correctly
5. **Performance Testing**: Ensure acceptable execution times

### Example Test Structure
```python
import unittest
from pyspark.sql import SparkSession
from scripts.statistical_analysis import calculate_summary_statistics

class TestStatisticalAnalysis(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("test").getOrCreate()
    
    def test_summary_statistics(self):
        # Create test data
        test_data = [
            ("STUDY001", "SUBJ001", "GLUCOSE", 150.0, "TRT", "WEEK 2"),
            ("STUDY001", "SUBJ002", "GLUCOSE", 180.0, "TRT", "WEEK 2"),
            ("STUDY001", "SUBJ003", "GLUCOSE", 120.0, "PBO", "WEEK 2"),
        ]
        
        df = self.spark.createDataFrame(
            test_data, 
            ["studyid", "usubjid", "param", "aval", "trtp", "avisit"]
        )
        
        # Test the function
        result = calculate_summary_statistics(df)
        
        # Validate results
        self.assertIsNotNone(result)
        self.assertTrue(result.count() > 0)
```

## Deployment Checklist

- [ ] DAG syntax validation
- [ ] Schema definitions updated
- [ ] Environment variables configured
- [ ] OpenMetadata schemas registered
- [ ] Lineage tracking verified
- [ ] Documentation updated
- [ ] Testing completed
- [ ] Compliance review passed

## Advanced Features

### Custom Lineage Extractors

For complex transformations, you can create custom lineage extractors:

```python
from openlineage.airflow.extractors.base import BaseExtractor
from openlineage.client.run import Dataset

class CustomSASExtractor(BaseExtractor):
    def extract(self):
        # Custom logic to extract lineage from SAS transformations
        inputs = [Dataset(namespace="clinical_data", name="input_table")]
        outputs = [Dataset(namespace="clinical_data", name="output_table")]
        return inputs, outputs
```

### Metadata Enrichment

Enhance your datasets with additional metadata:

```python
# Add custom properties to datasets
dataset_metadata = {
    "study_phase": "Phase III",
    "therapeutic_area": "Oncology",
    "data_cut_date": "2024-01-15",
    "regulatory_status": "FDA_submission_ready"
}
```

### Data Quality Integration

Integrate data quality checks with lineage tracking:

```python
def validate_data_quality(df, schema_definition):
    """Validate data quality and record results in lineage."""
    quality_results = {
        "completeness": calculate_completeness(df),
        "validity": validate_against_schema(df, schema_definition),
        "consistency": check_referential_integrity(df)
    }
    
    # Record quality metrics in lineage
    return quality_results
```

This comprehensive approach ensures that your SAS-to-Airflow migration maintains the highest standards of data governance and regulatory compliance while providing the scalability and flexibility of modern data processing platforms.
