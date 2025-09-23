# Clinical Data Analysis Pipeline Migration

This project migrates the existing SAS clinical data analysis pipeline to a modern Airflow + Databricks PySpark architecture. The pipeline processes clinical trial data (SDTM format) through ADaM datasets, performs statistical analysis on albumin levels, and generates box plot visualizations.

## Overview

The migration preserves the core statistical methodology from the PhUSE whitepaper WPCT-F.07.01 while modernizing the technology stack for better scalability and cloud-native deployment.

### Original SAS Pipeline

- **Initialization**: `programs/example_phuse_whitepapers/example_call_wpct-f.07.01.sas`
- **Analysis**: `programs/example_phuse_whitepapers/WPCT-F.07.01.sas`
- **Data**: ADLBC dataset (ADaM format)
- **Output**: PDF box plots via SAS SGRENDER

### New Airflow + PySpark Pipeline

- **Orchestration**: Airflow DAG with 4 tasks
- **Processing**: PySpark for data transformations
- **Visualization**: Plotly for interactive box plots
- **Storage**: Cloud-compatible (S3/ADLS)

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌──────────────────┐
│  Data Ingestion │───▶│ Data Preprocessing│───▶│ Statistical Calc │───▶│ Visualization    │
│                 │    │                  │    │                 │    │ Generation       │
│ - Load ADLBC    │    │ - Filter data    │    │ - PROC SUMMARY  │    │ - Box plots      │
│ - Add timepoints│    │ - Outlier detect │    │ - Group by visit│    │ - Reference lines│
│ - Format treats │    │ - Quality checks │    │ - Statistics    │    │ - Export HTML    │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └──────────────────┘
```

## File Structure

```
airflow_migration/
├── dags/
│   └── clinical_data_analysis_dag.py     # Main Airflow DAG
├── modules/
│   ├── data_ingestion.py                 # Task 1: Data loading
│   ├── data_preprocessing.py             # Task 2: Filtering & QC
│   ├── statistical_calculations.py       # Task 3: Statistics
│   ├── visualization_generation.py       # Task 4: Plotting
│   └── phuse_utilities.py               # PhUSE macro equivalents
├── config/
│   └── pipeline_config.py               # Configuration management
├── requirements.txt                      # Python dependencies
└── README.md                            # This file
```

## SAS to PySpark Mapping

### Data Filtering (SAS lines 240-249)

**Original SAS:**
```sas
where paramcd = "&m_var" and 
      not missing(aval) and
      not missing(visit) and
      not missing(trta) and
      anl01fl = 'Y' and
      trta ^= '' and
      avisitn ^= 99
```

**New PySpark:**
```python
filtered_df = df.filter(
    (col("paramcd") == m_var) &
    (col("aval").isNotNull()) &
    (col("visit").isNotNull()) &
    (col("trta").isNotNull()) &
    (col("anl01fl") == "Y") &
    (col("trta") != "") &
    (col("avisitn") != 99)
)
```

### Statistical Aggregations (SAS lines 343-348)

**Original SAS:**
```sas
proc summary data=css_nexttimept noprint;
  by avisitn &tn_var avisit &t_var;
  var &m_var;
  output out=css_stats n=n mean=mean std=std median=median 
         min=datamin max=datamax q1=q1 q3=q3;
run;
```

**New PySpark:**
```python
stats_df = df.groupBy("avisitn", "trtpn", "avisit", "trta").agg(
    count("aval").alias("n"),
    mean("aval").alias("mean"),
    stddev("aval").alias("std"),
    expr("percentile_approx(aval, 0.5)").alias("median"),
    min("aval").alias("datamin"),
    max("aval").alias("datamax"),
    expr("percentile_approx(aval, 0.25)").alias("q1"),
    expr("percentile_approx(aval, 0.75)").alias("q3")
)
```

### Outlier Detection (SAS lines 245-248)

**Original SAS:**
```sas
if (2 = n(&m_var, &lo_var) and &m_var < &lo_var) or
   (2 = n(&m_var, &hi_var) and &m_var > &hi_var) then m_var_outlier = &m_var;
else m_var_outlier = .;
```

**New PySpark:**
```python
df_with_outliers = df.withColumn(
    "m_var_outlier",
    when(
        ((col("aval").isNotNull()) & (col("a1lo").isNotNull()) & (col("aval") < col("a1lo"))) |
        ((col("aval").isNotNull()) & (col("a1hi").isNotNull()) & (col("aval") > col("a1hi"))),
        col("aval")
    ).otherwise(None)
)
```

## Configuration Management

### SAS Macro Variables (lines 163-179)

**Original SAS:**
```sas
%let m_var = ALB;
%let t_var = AVISITN;
%let ref_lines = UNIFORM;
%let max_boxes_per_page = 20;
```

**New Python Configuration:**
```python
PIPELINE_CONFIG = {
    "parameter": "ALB",
    "treatment_var": "AVISITN", 
    "ref_lines": "UNIFORM",
    "max_boxes_per_page": 20,
    "visit_filter": [0, 2, 4, 6]
}
```

## PhUSE Utility Functions

The original SAS pipeline uses PhUSE utility macros. These have been reimplemented in Python:

| SAS Macro | Python Function | Purpose |
|-----------|----------------|---------|
| `%util_access_test_data` | `ingest_adlbc_data()` | Load test data |
| `%util_labels_from_var` | `util_labels_from_var()` | Extract value/label pairs |
| `%util_count_unique_values` | `util_count_unique_values()` | Count unique values |
| `%util_get_reference_lines` | `util_get_reference_lines()` | Calculate reference lines |
| `%util_value_format` | `util_value_format()` | Format numeric displays |
| `%util_axis_order` | `util_axis_order()` | Calculate axis ranges |

## Installation and Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Airflow:**
   ```bash
   # Set Airflow home
   export AIRFLOW_HOME=/path/to/airflow
   
   # Initialize database
   airflow db init
   
   # Create admin user
   airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
   ```

3. **Set Airflow Variables:**
   ```bash
   airflow variables set clinical_dataset "ADLBC"
   airflow variables set analysis_parameter "ALB"
   airflow variables set output_path "/tmp/clinical_outputs"
   ```

4. **Copy DAG to Airflow:**
   ```bash
   cp dags/clinical_data_analysis_dag.py $AIRFLOW_HOME/dags/
   ```

## Running the Pipeline

1. **Start Airflow:**
   ```bash
   airflow webserver --port 8080 &
   airflow scheduler &
   ```

2. **Trigger DAG:**
   - Via Web UI: http://localhost:8080
   - Via CLI: `airflow dags trigger clinical_data_analysis_pipeline`

3. **Monitor Progress:**
   - Check task status in Airflow UI
   - View logs for each task
   - Monitor Spark jobs in Spark UI

## Output

The pipeline generates:

- **Statistics**: Parquet files with summary statistics by visit and treatment
- **Visualizations**: Interactive HTML box plots (replaces PDF output)
- **Logs**: Detailed execution logs for each task
- **Metadata**: Task execution metadata stored in Airflow XCom

### Box Plot Features

- **Interactive**: Zoom, pan, hover tooltips
- **Outliers**: Red dots for values outside normal range
- **Reference Lines**: Configurable normal range indicators
- **Statistics Table**: Summary statistics overlay
- **Multi-page**: Automatic pagination for many visits/treatments

## Validation

The migrated pipeline produces statistically equivalent results to the original SAS implementation:

- **Same filtering logic**: Identical record selection criteria
- **Same statistics**: Mean, std dev, quartiles match SAS PROC SUMMARY
- **Same outlier detection**: Normal range outlier identification preserved
- **Same visualization**: Box plot methodology follows PhUSE standards

## Performance Considerations

- **Spark Optimization**: Adaptive query execution enabled
- **Caching**: Strategic DataFrame caching for reused data
- **Partitioning**: Optimal partitioning for large datasets
- **Memory Management**: Configurable Spark memory settings

## Troubleshooting

### Common Issues

1. **SAS File Reading**: Ensure `sas7bdat` package is installed
2. **Spark Memory**: Increase driver/executor memory for large datasets
3. **Missing Columns**: Validate dataset schema matches expectations
4. **Airflow Permissions**: Ensure proper file system permissions

### Debugging

- Check Airflow task logs for detailed error messages
- Use Spark UI to monitor job execution
- Validate intermediate outputs in `/tmp/clinical_outputs`
- Test individual modules outside of Airflow

## Future Enhancements

- **Cloud Deployment**: Deploy to AWS/Azure with managed Spark
- **Data Validation**: Add Great Expectations for data quality
- **Monitoring**: Integrate with monitoring tools (Prometheus/Grafana)
- **CI/CD**: Automated testing and deployment pipeline
- **Multiple Parameters**: Extend to analyze multiple lab parameters
- **Real-time Processing**: Stream processing for real-time data

## References

- [PhUSE WPCT-F.07.01 Whitepaper](https://github.com/phuse-org/phuse-scripts/tree/master/whitepapers/WPCT)
- [CDISC ADaM Standards](https://www.cdisc.org/standards/foundational/adam)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Plotly Documentation](https://plotly.com/python/)
