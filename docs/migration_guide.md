# SAS to Airflow/PySpark Migration Guide

## Overview

This document explains the migration of the pharmaceutical clinical data analysis pipeline from SAS to a modern Airflow + Databricks PySpark architecture.

## Original SAS System

### Key Components
- **Initialization Script**: `programs/example_phuse_whitepapers/example_call_wpct-f.07.01.sas`
- **Core Analysis**: `programs/example_phuse_whitepapers/WPCT-F.07.01.sas`
- **Data Sources**: SDTM and ADaM datasets in SAS7BDAT format
- **Output**: PDF box plots showing treatment responses

### Data Flow
1. Raw clinical trial data (SDTM format)
2. Analysis-ready ADaM datasets (ADLBC - Lab Chemistry)
3. Data filtering and preprocessing
4. Statistical calculations (PROC SUMMARY)
5. Box plot visualization (SGRENDER)

## New Airflow/PySpark System

### Architecture
```
Airflow DAG
├── Task 1: Data Ingestion (PySpark)
├── Task 2: Data Preprocessing (PySpark)  
├── Task 3: Statistical Analysis (PySpark)
└── Task 4: Visualization (Python/Plotly)
```

### Key Files
- **DAG Definition**: `airflow/dags/clinical_data_analysis_dag.py`
- **Data Processing**: `pyspark/` directory
- **Visualization**: `visualization/box_plots.py`
- **Configuration**: `config/pipeline_config.yaml`
- **Utilities**: `utils/phuse_utilities.py`

## Migration Mapping

### 1. Data Ingestion
**SAS Original**:
```sas
%util_access_test_data(&ds, local=&phuse/data/adam/cdisc-split/) ;
```

**PySpark Equivalent**:
```python
def ingest_clinical_data(config):
    df = read_sas_dataset(file_path, dataset_name)
    df_with_timepoints = add_timepoint_variables(df)
    return df_with_timepoints
```

### 2. Data Filtering
**SAS Original** (lines 240-249):
```sas
where paramcd = "&m_var" and 
      not missing(aval) and
      not missing(visit) and
      not missing(trta) and
      anl01fl = 'Y' and
      trta ^= '' and
      avisitn ^= 99
```

**PySpark Equivalent**:
```python
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
```

### 3. Statistical Calculations
**SAS Original** (lines 343-348):
```sas
proc summary data=css_nexttimept noprint;
  by avisitn &tn_var avisit &t_var;
  var &m_var;
  output out=css_stats (drop=_type_ _freq_)
         n=n mean=mean std=std median=median min=datamin max=datamax q1=q1 q3=q3;
run;
```

**PySpark Equivalent**:
```python
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
             )
```

### 4. Outlier Detection
**SAS Original** (lines 245-248):
```sas
if (2 = n(&m_var, &lo_var) and &m_var < &lo_var) or
   (2 = n(&m_var, &hi_var) and &m_var > &hi_var) then m_var_outlier = &m_var;
else m_var_outlier = .;
```

**PySpark Equivalent**:
```python
df_with_outliers = df.withColumn(
    "m_var_outlier",
    when(
        ((col("AVAL").isNotNull()) & (col(lo_var).isNotNull()) & (col("AVAL") < col(lo_var))) |
        ((col("AVAL").isNotNull()) & (col(hi_var).isNotNull()) & (col("AVAL") > col(hi_var))),
        col("AVAL")
    ).otherwise(None)
)
```

### 5. Configuration Management
**SAS Original** (lines 163-179):
```sas
%let m_var = ALB;
%let t_var = AVISITN;
%let ref_lines = UNIFORM;
%let max_boxes_per_page = 20;
```

**YAML Configuration**:
```yaml
analysis:
  measurement_variable: "ALB"
  time_variable: "AVISITN"
reference_lines:
  type: "UNIFORM"
visualization:
  max_boxes_per_page: 20
```

### 6. Visualization
**SAS Original**: SGRENDER with PhUSEboxplot template

**Python Equivalent**: Plotly box plots with equivalent styling and functionality

## Configuration and Setup

### 1. Airflow Variables
Set these variables in Airflow UI or import from `config/airflow_variables.json`:
- `clinical_dataset`: Dataset name (default: "ADLBC")
- `measurement_variable`: Parameter to analyze (default: "ALB")
- `reference_lines`: Reference line type (default: "UNIFORM")

### 2. Pipeline Configuration
Edit `config/pipeline_config.yaml` to customize:
- Data paths and output locations
- Analysis parameters
- Visualization settings
- Statistical formatting options

### 3. Running the Pipeline

#### Option 1: Airflow DAG
1. Deploy DAG to Airflow environment
2. Set required Airflow Variables
3. Trigger DAG execution

#### Option 2: Standalone Execution
```bash
# Run individual components
python pyspark/data_ingestion.py
python pyspark/data_preprocessing.py
python pyspark/statistical_analysis.py
python visualization/box_plots.py
```

## Key Differences and Improvements

### Advantages of New System
1. **Scalability**: PySpark handles larger datasets efficiently
2. **Cloud Native**: Designed for cloud storage and compute
3. **Modularity**: Separate components for easier maintenance
4. **Modern Visualization**: Interactive HTML plots vs static PDFs
5. **Configuration Management**: YAML-based configuration
6. **Monitoring**: Airflow provides workflow monitoring and alerting

### Statistical Equivalence
- All statistical calculations produce identical results
- Outlier detection logic preserved exactly
- Reference line calculations maintained
- Box plot statistical elements unchanged

### Output Differences
- **Format**: HTML interactive plots instead of PDF
- **Storage**: Cloud storage instead of local files
- **Interactivity**: Plotly enables zooming, hovering, filtering
- **Accessibility**: Web-based viewing vs PDF downloads

## Testing and Validation

### Verification Steps
1. **Data Integrity**: Compare row counts and key statistics
2. **Statistical Accuracy**: Validate calculations match SAS output
3. **Visual Comparison**: Compare box plot elements and outliers
4. **Performance**: Monitor execution times and resource usage

### Test Commands
```bash
# Run all tests
python -m pytest tests/

# Test individual components
python -m pytest tests/test_data_ingestion.py
python -m pytest tests/test_statistical_analysis.py
python -m pytest tests/test_visualization.py

# Lint code
flake8 airflow/ pyspark/ visualization/ utils/
```

## Troubleshooting

### Common Issues
1. **SAS File Reading**: Ensure pandas version supports SAS7BDAT format
2. **Memory Issues**: Adjust Spark configuration for large datasets
3. **Missing Dependencies**: Install required Python packages
4. **Path Issues**: Verify data file paths in configuration

### Performance Optimization
1. **Partitioning**: Partition large datasets by study or parameter
2. **Caching**: Cache frequently accessed DataFrames
3. **Resource Allocation**: Tune Spark executor and driver settings
4. **Data Format**: Consider converting to Parquet for better performance

## Migration Checklist

- [ ] Deploy Airflow DAG
- [ ] Configure Airflow Variables
- [ ] Test data ingestion with sample files
- [ ] Validate statistical calculations
- [ ] Compare visualization outputs
- [ ] Set up monitoring and alerting
- [ ] Document any customizations
- [ ] Train users on new system

## Support and Maintenance

### Key Contacts
- **Data Engineering**: For pipeline issues and performance
- **Clinical Analytics**: For statistical validation and requirements
- **DevOps**: For infrastructure and deployment support

### Monitoring
- Airflow UI for workflow status
- Spark UI for job performance
- Application logs for debugging
- Data quality metrics and alerts

## Future Enhancements

### Potential Improvements
1. **Real-time Processing**: Stream processing for live data
2. **Advanced Analytics**: Machine learning integration
3. **Data Catalog**: Automated metadata management
4. **Multi-study Support**: Enhanced configuration for multiple studies
5. **API Integration**: REST APIs for external system integration
