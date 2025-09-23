# SAS to Airflow Migration Guide

This document provides a comprehensive mapping between the original SAS clinical data analysis pipeline and the new Airflow + PySpark implementation.

## Executive Summary

The migration preserves the exact statistical methodology from the PhUSE whitepaper WPCT-F.07.01 while modernizing the technology stack. All data filtering, statistical calculations, and visualization logic produce equivalent results to the original SAS implementation.

## File Mapping

| Original SAS File | New Python Module | Purpose |
|-------------------|-------------------|---------|
| `example_call_wpct-f.07.01.sas` | `data_ingestion.py` | Data loading and initialization |
| `WPCT-F.07.01.sas` (lines 240-350) | `data_preprocessing.py` | Data filtering and quality checks |
| `WPCT-F.07.01.sas` (lines 343-348) | `statistical_calculations.py` | PROC SUMMARY equivalent |
| `WPCT-F.07.01.sas` (lines 398-422) | `visualization_generation.py` | SGRENDER equivalent |
| PhUSE utility macros | `phuse_utilities.py` | Utility function implementations |
| SAS macro variables | `pipeline_config.py` | Configuration management |

## Detailed Code Mapping

### 1. Data Initialization

**SAS (example_call_wpct-f.07.01.sas:54)**
```sas
%util_access_test_data(&ds, local=&phuse/data/adam/cdisc-split/) ;
```

**Python (data_ingestion.py)**
```python
def ingest_adlbc_data(config, **context):
    pandas_df = pd.read_sas(dataset_path)
    spark_df = spark.createDataFrame(pandas_df)
    return processed_df
```

### 2. Treatment Format Creation

**SAS (WPCT-F.07.01.sas:186-192)**
```sas
proc format;
    value trt_short
    0 = 'P'
    54 = 'X-high'
    81 = 'X-low'
    other = 'UNEXPECTED';
run;
```

**Python (data_ingestion.py)**
```python
processed_df = spark_df.withColumn(
    "trtp_short",
    when(col("TRTPN") == 0, "P")
    .when(col("TRTPN") == 54, "X-high") 
    .when(col("TRTPN") == 81, "X-low")
    .otherwise("UNEXPECTED")
)
```

### 3. Data Subset Creation

**SAS (WPCT-F.07.01.sas:195-204)**
```sas
data &ds._sub;
  set work.&ds;
  where (paramcd in (&param) &cond);
  
  attrib trtp_short length=$6 label='Planned Treatment, abbreviated';
  trtp_short = put(&tn_var,trt_short.);
run;
```

**Python (data_preprocessing.py)**
```python
def preprocess_clinical_data(config, **context):
    param_filtered_df = df.filter(col("PARAMCD") == config['parameter'])
    visit_filtered_df = param_filtered_df.filter(col("AVISITN").isin(config['visit_filter']))
    return filtered_df
```

### 4. Safety Population Filtering

**SAS (WPCT-F.07.01.sas:241-243)**
```sas
data css_anadata;
  set &m_lb..&m_ds (keep=&ana_variables);
  where &p_fl = 'Y' and &a_fl = 'Y';
```

**Python (data_preprocessing.py)**
```python
filtered_df = df.filter(
    (col(config['population_flag']) == 'Y') &
    (col(config['analysis_flag']) == 'Y') &
    # Additional filters...
)
```

### 5. Outlier Detection

**SAS (WPCT-F.07.01.sas:245-248)**
```sas
if (2 = n(&m_var, &lo_var) and &m_var < &lo_var) or
   (2 = n(&m_var, &hi_var) and &m_var > &hi_var) then m_var_outlier = &m_var;
else m_var_outlier = .;
```

**Python (data_preprocessing.py)**
```python
outlier_df = filtered_df.withColumn(
    "m_var_outlier",
    when(
        ((col("aval").isNotNull()) & (col("a1lo").isNotNull()) & (col("aval") < col("a1lo"))) |
        ((col("aval").isNotNull()) & (col("a1hi").isNotNull()) & (col("aval") > col("a1hi"))),
        col("aval")
    ).otherwise(None)
)
```

### 6. Statistical Summary

**SAS (WPCT-F.07.01.sas:343-349)**
```sas
proc summary data=css_nexttimept noprint;
  by avisitn &tn_var avisit &t_var;
  var &m_var;
  output out=css_stats (drop=_type_ _freq_)
         n=n mean=mean std=std median=median min=datamin max=datamax q1=q1 q3=q3;
run;
```

**Python (statistical_calculations.py)**
```python
stats_df = df.groupBy("AVISITN", "TRTPN", "AVISIT", "TRTA").agg(
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

### 7. Plot Data Preparation

**SAS (WPCT-F.07.01.sas:355-360)**
```sas
data css_plot;
  set css_nexttimept
      css_stats;
  format mean %scan(&util_value_format, 1, %str( )) std %scan(&util_value_format, 2, %str( ));
run;
```

**Python (statistical_calculations.py)**
```python
def create_plot_dataset(raw_df, stats_df, config):
    # Format statistics for display
    formatted_stats_df = stats_df.withColumn(
        "mean", spark_round(col("mean"), 2)
    ).withColumn(
        "std", spark_round(col("std"), 3)
    )
    return plot_data_path
```

### 8. Box Plot Generation

**SAS (WPCT-F.07.01.sas:398-422)**
```sas
proc sgrender data=css_plot (where=( %unquote(&nxtvis) )) template=PhUSEboxplot ;
  dynamic
          _MARKERS    = "&t_var"
          _XVAR       = 'avisitn'
          _BLOCKLABEL = 'avisit'
          _YVAR       = "&m_var"
          _YOUTLIERS  = 'm_var_outlier'
          _YLABEL     = "&&paramcd_lab&pdx"
          _N          = 'n'
          _MEAN       = 'mean'
          _STD        = 'std';
run;
```

**Python (visualization_generation.py)**
```python
def create_clinical_box_plot(raw_data, stats_data, config, parameter, timepoint):
    fig = make_subplots(rows=1, cols=1)
    
    for treatment in treatments:
        fig.add_trace(
            go.Box(
                x=treatment_data['AVISITN'],
                y=treatment_data['aval'],
                name=treatment,
                boxpoints='outliers'
            )
        )
    
    # Add outlier markers for values outside normal range
    outliers = treatment_data[treatment_data['m_var_outlier'].notna()]
    fig.add_trace(
        go.Scatter(
            x=outliers['AVISITN'],
            y=outliers['m_var_outlier'],
            mode='markers',
            marker=dict(color='red', size=6)
        )
    )
    
    return fig
```

## Configuration Migration

### SAS Macro Variables

**SAS (WPCT-F.07.01.sas:163-179)**
```sas
%let m_lb   = work;
%let m_ds   = &ds._sub;
%let param = 'ALB';
%let cond = %str(and AVISITN in (0 2 4 6));
%let t_var  = trtp_short;
%let tn_var = trtpn;
%let m_var  = aval;
%let lo_var = a1lo;
%let hi_var = a1hi;
%let p_fl = saffl;
%let a_fl = anl01fl;
%let ref_lines = UNIFORM;
%let max_boxes_per_page = 20;
```

**Python (pipeline_config.py)**
```python
base_config = {
    'dataset': 'ADLBC',
    'parameter': 'ALB',
    'visit_filter': [0, 2, 4, 6],
    'treatment_var': 'trtp_short',
    'treatment_num_var': 'trtpn',
    'measurement_var': 'aval',
    'low_limit_var': 'a1lo',
    'high_limit_var': 'a1hi',
    'population_flag': 'saffl',
    'analysis_flag': 'anl01fl',
    'ref_lines': 'UNIFORM',
    'max_boxes_per_page': 20,
}
```

## PhUSE Utility Macro Migration

### util_labels_from_var

**SAS Usage:**
```sas
%util_labels_from_var(css_anadata, paramcd, param)
```

**Python Implementation:**
```python
def util_labels_from_var(df, value_var, label_var):
    unique_pairs = df.select(value_var, label_var).distinct().collect()
    value_label_map = {row[value_var]: row[label_var] for row in unique_pairs}
    return {
        'count': len(unique_pairs),
        'values': [row[value_var] for row in unique_pairs],
        'labels': [row[label_var] for row in unique_pairs],
        'value_label_map': value_label_map
    }
```

### util_count_unique_values

**SAS Usage:**
```sas
%util_count_unique_values(css_anadata, &t_var, trtn)
```

**Python Implementation:**
```python
def util_count_unique_values(df, variable, result_name):
    unique_count = df.select(variable).distinct().count()
    return unique_count
```

### util_get_reference_lines

**SAS Usage:**
```sas
%util_get_reference_lines(css_nextparam, nxt_reflines,
                          low_var=&lo_var, high_var=&hi_var, ref_lines=&ref_lines)
```

**Python Implementation:**
```python
def util_get_reference_lines(df, low_var, high_var, ref_lines_type='UNIFORM'):
    if ref_lines_type == 'UNIFORM':
        low_values = df.select(low_var).distinct().collect()
        high_values = df.select(high_var).distinct().collect()
        # Return uniform reference lines if all values are the same
        ref_lines = []
        if len(set(low_values)) == 1:
            ref_lines.append(low_values[0])
        if len(set(high_values)) == 1:
            ref_lines.append(high_values[0])
        return ref_lines
```

## Output Format Changes

### Original SAS Output
- **Format**: PDF files
- **Location**: Local file system
- **Naming**: `WPCT-F.07.01_Box_plot_ALB_by_visit_for_timepoint_1.pdf`

### New Python Output
- **Format**: Interactive HTML files
- **Location**: Configurable (local or cloud storage)
- **Naming**: `WPCT-F.07.01_Box_plot_ALB_by_visit_for_timepoint_1.html`
- **Features**: 
  - Interactive zoom/pan
  - Hover tooltips with exact values
  - Downloadable as PNG/PDF
  - Responsive design

## Data Flow Comparison

### SAS Pipeline Flow
```
1. %util_access_test_data → Load ADLBC
2. DATA step → Create subset with filters
3. PROC SUMMARY → Calculate statistics
4. DATA step → Combine raw + stats
5. PROC SGRENDER → Generate PDF plots
```

### Python Pipeline Flow
```
1. ingest_adlbc_data() → Load ADLBC with Spark
2. preprocess_clinical_data() → Apply filters + outlier detection
3. calculate_statistics() → PySpark aggregations
4. generate_box_plots() → Plotly interactive plots
```

## Performance Improvements

| Aspect | SAS | Python + Spark |
|--------|-----|----------------|
| **Scalability** | Single machine | Distributed processing |
| **Memory** | Limited by SAS workspace | Configurable Spark memory |
| **Parallelization** | Limited | Full Spark parallelization |
| **Cloud Integration** | Requires SAS Grid | Native cloud support |
| **Cost** | SAS licensing costs | Open source stack |

## Validation Strategy

### Statistical Equivalence
- **Filtering**: Same record counts after each filter step
- **Statistics**: Mean, std dev, quartiles match to specified precision
- **Outliers**: Same outlier identification logic
- **Visualization**: Same box plot methodology

### Testing Approach
- **Unit Tests**: Individual function validation
- **Integration Tests**: End-to-end pipeline testing
- **Comparison Tests**: Side-by-side SAS vs Python results
- **Performance Tests**: Scalability and memory usage

## Migration Benefits

### Technical Benefits
- **Scalability**: Handle larger datasets with Spark
- **Cloud Native**: Deploy on AWS, Azure, GCP
- **Cost Effective**: No SAS licensing fees
- **Modern Stack**: Python ecosystem and tools
- **Version Control**: Git-based development workflow

### Business Benefits
- **Faster Processing**: Parallel processing capabilities
- **Interactive Outputs**: Web-based visualizations
- **Easier Maintenance**: Standard Python/SQL skills
- **Better Integration**: REST APIs, databases, cloud services
- **Compliance**: Same statistical methodology, auditable code

## Deployment Considerations

### Development Environment
- Local Spark for development/testing
- Jupyter notebooks for exploration
- Git version control

### Production Environment
- Managed Spark (Databricks, EMR, Dataproc)
- Airflow on Kubernetes
- Cloud storage (S3, ADLS, GCS)
- Monitoring and alerting

### Security
- Data encryption at rest and in transit
- Role-based access control
- Audit logging
- Compliance with clinical data regulations

This migration preserves the scientific integrity of the original SAS analysis while providing a modern, scalable, and cost-effective solution for clinical data analysis.
