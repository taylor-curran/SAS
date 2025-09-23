# SAS to Airflow/PySpark Migration Guide

## Overview

This document describes the migration of a SAS clinical data analysis pipeline to Apache Airflow with Databricks PySpark. The original system processes clinical trial data in SDTM/ADaM format, performs statistical analysis on laboratory measurements (specifically albumin levels), and generates box plot visualizations.

## Architecture Comparison

### Original SAS System
- **Initialization**: `example_call_wpct-f.07.01.sas` - Sets up environment and loads test data
- **Analysis**: `WPCT-F.07.01.sas` - Core analysis script with data filtering, statistical calculations, and visualization
- **Dependencies**: PhUSE utility macros for data access, formatting, and templating
- **Output**: PDF box plots with statistical summaries

### New Airflow/PySpark System
- **Orchestration**: Airflow DAG with 4 sequential tasks
- **Processing**: PySpark DataFrames for distributed data processing
- **Configuration**: YAML-based configuration management
- **Output**: Interactive HTML visualizations with cloud storage

## Task Mapping

### Task 1: Data Ingestion
**Replaces**: `%util_access_test_data` functionality from `example_call_wpct-f.07.01.sas`

**SAS Code**:
```sas
%let ds = ADLBC;
%util_access_test_data(&ds, local=&phuse/data/adam/cdisc-split/) ;

DATA &ds;
    SET &ds;
    ATPTN = 1;
    ATPT = "TimePoint unknown";
RUN;
```

**PySpark Equivalent**:
```python
def load_clinical_data(spark, data_path, dataset_name):
    df = spark.read.format("com.github.saurfang.sas.spark").load(file_path)
    df_with_timepoints = df.withColumn("ATPTN", lit(1)) \
                          .withColumn("ATPT", lit("TimePoint unknown"))
    return df_with_timepoints
```

### Task 2: Data Preprocessing and Filtering
**Replaces**: Data filtering logic from `WPCT-F.07.01.sas` lines 240-249

**SAS Code**:
```sas
data css_anadata;
    set &m_lb..&m_ds (keep=&ana_variables);
    where &p_fl = 'Y' and &a_fl = 'Y';
    
    where paramcd = "&m_var" and 
          not missing(aval) and
          not missing(visit) and
          not missing(trta) and
          anl01fl = 'Y' and
          trta ^= '' and
          avisitn ^= 99;
          
    if (2 = n(&m_var, &lo_var) and &m_var < &lo_var) or
       (2 = n(&m_var, &hi_var) and &m_var > &hi_var) then m_var_outlier = &m_var;
    else m_var_outlier = .;
run;
```

**PySpark Equivalent**:
```python
def filter_clinical_data(df, config):
    filtered_df = df.filter(
        (col("PARAMCD") == parameter) &
        col(measurement_var).isNotNull() &
        col("AVISIT").isNotNull() &
        col("TRTA").isNotNull() &
        (col(analysis_flag) == "Y") &
        (col("TRTA") != "") &
        (col("AVISITN") != 99) &
        (col(population_flag) == "Y")
    )
    
    outlier_condition = (
        (col(measurement_var).isNotNull() & col(low_ref_var).isNotNull() & 
         (col(measurement_var) < col(low_ref_var))) |
        (col(measurement_var).isNotNull() & col(high_ref_var).isNotNull() & 
         (col(measurement_var) > col(high_ref_var)))
    )
    
    df_with_outliers = filtered_df.withColumn(
        "m_var_outlier",
        when(outlier_condition, col(measurement_var)).otherwise(lit(None))
    )
    
    return df_with_outliers
```

### Task 3: Statistical Calculations
**Replaces**: `PROC SUMMARY` operations from `WPCT-F.07.01.sas` lines 343-348

**SAS Code**:
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
def calculate_summary_statistics(df, config):
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
    return stats_df
```

### Task 4: Visualization Generation
**Replaces**: `SGRENDER` functionality from `WPCT-F.07.01.sas` lines 398-423

**SAS Code**:
```sas
proc sgrender data=css_plot template=PhUSEboxplot ;
    dynamic
            _MARKERS    = "&t_var"
            _XVAR       = 'avisitn'
            _BLOCKLABEL = 'avisit'
            _YVAR       = "&m_var"
            _YOUTLIERS  = 'm_var_outlier'
            _REFLINES   = "%sysfunc(translate( &nxt_reflines, %str(,), %str( ) ))"
            _YLABEL     = "&&paramcd_lab&pdx"
            _N          = 'n'
            _MEAN       = 'mean'
            _STD        = 'std';
run;
```

**PySpark/Plotly Equivalent**:
```python
def create_parameter_boxplot(raw_data, stats_data, config, parameter, timepoint):
    fig = make_subplots(rows=2, cols=1, row_heights=[0.8, 0.2])
    
    for i, treatment in enumerate(treatments):
        fig.add_trace(
            go.Box(
                x=treatment_data['AVISITN'],
                y=treatment_data[measurement_var],
                name=treatment,
                boxpoints='outliers'
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=treatment_stats['AVISITN'],
                y=treatment_stats['mean'],
                mode='markers',
                marker=dict(symbol='diamond', size=8)
            ),
            row=1, col=1
        )
    
    return fig
```

## Configuration Mapping

### SAS Macro Variables → YAML Configuration

| SAS Macro Variable | YAML Path | Description |
|-------------------|-----------|-------------|
| `%let param = 'ALB'` | `analysis.parameter` | Parameter to analyze |
| `%let cond = %str(and AVISITN in (0 2 4 6))` | `analysis.visits` | Visit numbers to include |
| `%let ref_lines = UNIFORM` | `analysis.reference_lines` | Reference lines configuration |
| `%let max_boxes_per_page = 20` | `analysis.max_boxes_per_page` | Pagination limit |
| `%let t_var = trtp_short` | `variables.treatment_var` | Treatment variable |
| `%let tn_var = trtpn` | `variables.treatment_num_var` | Treatment number variable |
| `%let m_var = aval` | `variables.measurement_var` | Measurement variable |
| `%let lo_var = a1lo` | `variables.low_ref_var` | Lower reference limit |
| `%let hi_var = a1hi` | `variables.high_ref_var` | Upper reference limit |
| `%let p_fl = saffl` | `variables.population_flag` | Population flag |
| `%let a_fl = anl01fl` | `variables.analysis_flag` | Analysis flag |

## PhUSE Utility Macros → Python Functions

| SAS Macro | Python Function | Purpose |
|-----------|----------------|---------|
| `%util_access_test_data` | `load_clinical_data()` | Data loading and access |
| `%util_labels_from_var` | `util_labels_from_var()` | Extract unique values and labels |
| `%util_count_unique_values` | `util_count_unique_values()` | Count unique values |
| `%util_get_reference_lines` | `util_get_reference_lines()` | Get reference line values |
| `%util_value_format` | `util_value_format()` | Determine formatting precision |
| `%util_boxplot_block_ranges` | `util_boxplot_block_ranges()` | Create pagination ranges |
| `%util_get_var_min_max` | `util_get_var_min_max()` | Get min/max for axis scaling |
| `%util_axis_order` | `util_axis_order()` | Calculate axis increments |
| `%util_delete_dsets` | `util_delete_dsets()` | Cleanup temporary datasets |

## Data Flow

### Original SAS Flow
1. Initialize environment and paths
2. Load ADLBC dataset with PhUSE utilities
3. Add dummy timepoint variables
4. Filter data by parameter, visits, and flags
5. Calculate summary statistics with PROC SUMMARY
6. Generate box plots with SGRENDER template
7. Output PDF files to local directory

### New Airflow/PySpark Flow
1. **Data Ingestion Task**: Load ADLBC from cloud storage, add timepoint variables
2. **Data Preprocessing Task**: Apply filters and outlier detection
3. **Statistical Calculations Task**: Compute summary statistics with PySpark aggregations
4. **Visualization Generation Task**: Create interactive box plots with Plotly
5. Save results to cloud storage (S3/ADLS)

## Key Differences

### Technology Stack
- **SAS → PySpark**: Distributed processing instead of single-machine SAS
- **PDF → HTML**: Interactive visualizations instead of static PDFs
- **Local Files → Cloud Storage**: Scalable storage solution
- **Manual Execution → Airflow**: Automated orchestration and scheduling

### Statistical Accuracy
- All statistical calculations remain identical
- Outlier detection logic preserved exactly
- Reference line handling maintains same business rules
- Box plot specifications follow same PhUSE standards

### Scalability Improvements
- **Horizontal Scaling**: PySpark can process larger datasets across multiple nodes
- **Cloud Native**: Leverages cloud storage and compute resources
- **Monitoring**: Airflow provides execution monitoring and alerting
- **Reproducibility**: Version-controlled configuration and code

## Setup and Deployment

### Prerequisites
- Apache Airflow 2.5+
- Databricks workspace with Spark 3.3+
- Python 3.8+ with required packages
- Access to SDTM/ADaM data sources

### Installation Steps
1. Deploy Airflow DAG to your Airflow environment
2. Upload PySpark scripts to Databricks File System (DBFS)
3. Configure Databricks connection in Airflow
4. Update configuration file with your data paths
5. Test with sample data before production deployment

### Configuration
Update `config/pipeline_config.yaml` with your specific:
- Data source paths
- Output storage locations
- Spark cluster configurations
- Analysis parameters

## Testing and Validation

### Statistical Validation
- Compare summary statistics between SAS and PySpark outputs
- Verify outlier detection produces identical results
- Validate box plot data points match exactly

### Performance Testing
- Benchmark processing times with representative data volumes
- Test scalability with larger datasets
- Monitor resource utilization

### Integration Testing
- End-to-end DAG execution
- Error handling and retry logic
- Data quality checks at each stage

## Maintenance and Support

### Monitoring
- Airflow provides built-in execution monitoring
- Set up alerts for task failures
- Monitor data quality metrics

### Updates
- Configuration changes through YAML files
- Code updates through version control
- Databricks cluster management for scaling

### Troubleshooting
- Check Airflow logs for orchestration issues
- Review Spark logs for data processing errors
- Validate input data format and quality

## Benefits of Migration

1. **Scalability**: Handle larger datasets with distributed processing
2. **Cost Efficiency**: Pay-per-use cloud resources vs. dedicated SAS licenses
3. **Automation**: Scheduled execution with monitoring and alerting
4. **Interactivity**: Modern web-based visualizations
5. **Integration**: Easy integration with other data pipeline tools
6. **Maintainability**: Version-controlled, modular codebase
7. **Flexibility**: Easy to extend and modify for new requirements

## Future Enhancements

- Add support for additional clinical parameters
- Implement real-time data processing capabilities
- Integrate with clinical data management systems
- Add advanced statistical analysis features
- Implement automated report generation
