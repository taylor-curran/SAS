# SAS to PySpark Operation Mapping

This document provides a detailed mapping of SAS operations to their PySpark equivalents as implemented in the migrated clinical data analysis pipeline.

## Data Filtering Operations

| SAS Operation | PySpark Equivalent | Notes |
|---------------|-------------------|-------|
| `WHERE paramcd in (&param)` | `df.filter(F.col("paramcd").isin(parameters))` | Filter by parameter codes |
| `WHERE avisitn in (0 2 4 6)` | `df.filter(F.col("avisitn").isin([0, 2, 4, 6]))` | Filter by visit numbers |
| `WHERE &p_fl = 'Y' and &a_fl = 'Y'` | `df.filter((F.col(population_flag) == "Y") & (F.col(analysis_flag) == "Y"))` | Filter by population and analysis flags |

## Data Transformation Operations

| SAS Operation | PySpark Equivalent | Notes |
|---------------|-------------------|-------|
| `proc format; value trt_short...` | `abbreviate_treatment_udf = F.udf(abbreviate_treatment, StringType())` | Custom formatting using UDFs |
| `DATA &ds; SET &ds; ATPTN = 1; ATPT = "TimePoint unknown"; RUN;` | `df = df.withColumn("atptn", F.lit(1)).withColumn("atpt", F.lit("TimePoint unknown"))` | Adding new columns |
| `if (2 = n(&m_var, &lo_var) and &m_var < &lo_var) or (2 = n(&m_var, &hi_var) and &m_var > &hi_var) then m_var_outlier = &m_var; else m_var_outlier = .;` | `df.withColumn("m_var_outlier", F.when((F.col(measure_var).isNotNull() & F.col(low_var).isNotNull() & (F.col(measure_var) < F.col(low_var))) \| (F.col(measure_var).isNotNull() & F.col(high_var).isNotNull() & (F.col(measure_var) > F.col(high_var))), F.col(measure_var)).otherwise(None))` | Outlier detection |

## Statistical Operations

| SAS Operation | PySpark Equivalent | Notes |
|---------------|-------------------|-------|
| `proc summary data=css_nexttimept noprint; by avisitn &tn_var avisit &t_var; var &m_var; output out=css_stats (drop=_type_ _freq_) n=n mean=mean std=std median=median min=datamin max=datamax q1=q1 q3=q3; run;` | `df.groupBy("avisitn", treatment_num_var, "avisit", treatment_var).agg(F.count(measure_var).alias("n"), F.mean(measure_var).alias("mean"), F.stddev(measure_var).alias("std"), F.expr(f"percentile_approx({measure_var}, 0.5)").alias("median"), F.min(measure_var).alias("datamin"), F.max(measure_var).alias("datamax"), F.expr(f"percentile_approx({measure_var}, 0.25)").alias("q1"), F.expr(f"percentile_approx({measure_var}, 0.75)").alias("q3"))` | Summary statistics calculation |

## Visualization Operations

| SAS Operation | PySpark/Plotly Equivalent | Notes |
|---------------|--------------------------|-------|
| `proc sgrender data=css_plot (where=( %unquote(&nxtvis) )) template=PhUSEboxplot;` | `fig = go.Figure()` with `fig.add_trace(go.Box(...))` | Box plot generation |
| `_MARKERS = "&t_var"` | `marker=dict(color=f"hsl({(i * 360) // len(treatments)}, 70%, 50%)")` | Treatment markers |
| `_REFLINES = "%sysfunc(translate( &nxt_reflines, %str(,), %str( ) ))"` | `fig.add_shape(type="line", ...)` | Reference lines |
| `_YLABEL = "&&paramcd_lab&pdx"` | `yaxis_title=metadata['parameter_label']` | Axis labels |

## PhUSE Utility Macros

| SAS Macro | Python Function | Notes |
|-----------|----------------|-------|
| `%util_labels_from_var` | `labels_from_var()` | Extract parameter codes and labels |
| `%util_count_unique_values` | `count_unique_values()` | Count distinct values |
| `%util_get_reference_lines` | `get_reference_lines()` | Calculate reference lines |
| `%util_get_var_min_max` | `get_var_min_max()` | Determine axis ranges |
| `%util_value_format` | `value_format()` | Format statistical values |
| `%util_boxplot_block_ranges` | `boxplot_block_ranges()` | Paginate visits for plots |
| `%util_axis_order` | `axis_order()` | Calculate axis tick intervals |

## Configuration Management

| SAS Macro Variables | YAML Configuration | Notes |
|---------------------|-------------------|-------|
| `%let ds = ADLBC;` | `dataset: "ADLBC"` | Dataset name |
| `%let param = 'ALB';` | `parameters: ["ALB"]` | Parameters to analyze |
| `%let cond = %str(and AVISITN in (0 2 4 6));` | `visits: [0, 2, 4, 6]` | Visits to include |
| `%let t_var = trtp_short;` | `treatment_var: "trtp_short"` | Treatment variable |
| `%let ref_lines = UNIFORM;` | `ref_lines: "UNIFORM"` | Reference lines type |
