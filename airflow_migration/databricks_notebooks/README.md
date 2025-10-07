# Databricks Notebooks

This directory contains PySpark notebooks for Databricks that migrate SAS clinical trial data analysis scripts from the PhUSE Scripts repository.

## Overview

These notebooks provide an interactive, parameterized alternative to the Airflow DAG pipeline. While the Airflow DAG orchestrates automated batch processing, these Databricks notebooks enable:

- **Interactive analysis** with real-time results
- **User-configurable parameters** via Databricks widgets
- **Iterative development** and testing
- **Ad-hoc analysis** capabilities

Both implementations use the same core statistical logic and PhUSE utility functions to ensure consistency.

## Available Notebooks

### WPCT-F.07.01: Box Plot Analysis

**File:** `WPCT_F_07_01_box_plot_analysis.py`

**Purpose:** Migrates the SAS WPCT-F.07.01 script for box plot analysis of clinical measurements by visit and timepoint.

**Key Features:**
- Converts SAS PROC SUMMARY to PySpark `groupBy().agg()`
- Implements outlier detection based on normal ranges
- Processes multiple parameters and timepoints
- Outputs Delta tables for downstream visualization

**Original SAS Script:** `phuse-scripts/whitepapers/WPCT/WPCT-F.07.01.sas`

## Architecture

### Databricks Notebooks vs. Airflow DAG

| Feature | Databricks Notebooks | Airflow DAG |
|---------|---------------------|-------------|
| **Execution** | Interactive, on-demand | Scheduled, automated |
| **Parameters** | Databricks widgets | YAML configuration |
| **Use Case** | Exploration, testing | Production pipeline |
| **Output** | Delta tables | Parquet files + visualizations |
| **Deployment** | Databricks workspace | Airflow cluster |

### Common Components

Both implementations share:
- **PhUSE Utilities** (`utils/phuse_utils.py`)
- **Statistical Logic** (PROC SUMMARY → groupBy().agg())
- **Data Transformations** (filtering, outlier detection)
- **Output Format** (Delta/Parquet tables)

## Usage

### Prerequisites

1. **Databricks Workspace** with cluster running Spark 3.x+
2. **Input Data** in Delta/Parquet format with ADaM/SDTM schema
3. **PhUSE Utilities** available in workspace (upload `utils/` directory)

### Running a Notebook

1. **Upload to Databricks:**
   ```bash
   databricks workspace import WPCT_F_07_01_box_plot_analysis.py \
     /Users/your-email/notebooks/WPCT_F_07_01_box_plot_analysis \
     --language PYTHON
   ```

2. **Configure Widgets:**
   - Open notebook in Databricks
   - Adjust widget values for your analysis:
     - Dataset name (e.g., "ADLBC")
     - Parameters (e.g., "ALB,ALT,AST")
     - Visit numbers (e.g., "0,2,4,6,8")
     - Variable names

3. **Load Input Data:**
   ```python
   input_df = spark.read.format("delta").load("/path/to/clinical/data")
   ```

4. **Execute Functions:**
   ```python
   config = get_configuration()
   filtered_df = filter_and_prepare_data(input_df, config)
   prepared_df = create_treatment_abbreviations(filtered_df, config)
   process_all_parameters_and_timepoints(prepared_df, config, "/path/to/output")
   ```

5. **View Results:**
   ```python
   results_df = spark.read.format("delta").load("/path/to/output/param_alb/timepoint_0/statistics")
   display(results_df)
   ```

### Expected Input Schema

All notebooks expect DataFrames with standard ADaM/SDTM columns:

| Column | Description | Type |
|--------|-------------|------|
| `paramcd` | Parameter code | String |
| `param` | Parameter label | String |
| `aval` | Analysis value | Double |
| `avisitn` | Visit number | Integer |
| `avisit` | Visit label | String |
| `atptn` | Analysis timepoint number | Integer |
| `atpt` | Analysis timepoint label | String |
| `trtpn` | Treatment number | Integer |
| `trtp` | Treatment label | String |
| `a1lo` | Lower limit of normal range | Double |
| `a1hi` | Upper limit of normal range | Double |
| `saffl` | Safety population flag | String |
| `anl01fl` | Analysis flag | String |
| `usubjid` | Subject ID | String |
| `studyid` | Study ID | String |

## Output Structure

Notebooks create hierarchical Delta table structure:

```
{output_path}/
  ├── param_alb/
  │   ├── timepoint_0/
  │   │   ├── data/          # Filtered clinical data
  │   │   └── statistics/    # Summary statistics
  │   ├── timepoint_2/
  │   │   ├── data/
  │   │   └── statistics/
  │   └── ...
  ├── param_alt/
  │   └── ...
  └── ...
```

### Statistics Output Schema

The `statistics/` Delta tables contain:

| Column | Description | SAS Equivalent |
|--------|-------------|----------------|
| `avisitn` | Visit number | AVISITN |
| `avisit` | Visit label | AVISIT |
| `trtpn` | Treatment number | TRTPN |
| `trtp_short` | Treatment abbreviation | formatted TRTP |
| `atptn` | Timepoint number | ATPTN |
| `atpt` | Timepoint label | ATPT |
| `paramcd` | Parameter code | PARAMCD |
| `param` | Parameter label | PARAM |
| `n` | Count | N |
| `mean` | Mean | MEAN |
| `std` | Standard deviation | STD |
| `median` | Median | MEDIAN |
| `datamin` | Minimum | MIN |
| `datamax` | Maximum | MAX |
| `q1` | First quartile | Q1 |
| `q3` | Third quartile | Q3 |

## Development

### Adding New Notebooks

To migrate additional SAS scripts:

1. **Identify SAS Script:**
   - Review `phuse-scripts/whitepapers/` for candidate scripts
   - Focus on scripts with clear statistical logic

2. **Create Notebook Structure:**
   ```python
   # Databricks notebook source
   # MAGIC %md
   # MAGIC # Title
   
   # COMMAND ----------
   # Setup widgets
   
   # COMMAND ----------
   # Import libraries
   
   # COMMAND ----------
   # Define functions
   ```

3. **Convert SAS Patterns:**
   - DATA steps → `DataFrame.filter()` / `DataFrame.withColumn()`
   - PROC SUMMARY → `DataFrame.groupBy().agg()`
   - PROC SQL → `DataFrame` operations or Spark SQL
   - PROC GLM → scipy.stats / statsmodels (via UDFs if needed)
   - Macro loops → Python for loops

4. **Reuse Utilities:**
   - Check `utils/phuse_utils.py` for existing functions
   - Add new utilities if needed (keep SAS equivalence)

5. **Document Thoroughly:**
   - Link to original SAS script
   - Explain conversion patterns
   - Provide usage examples
   - Include conversion notes

### Testing

Since clinical trial data cannot be included in the repository:

1. **Syntax Validation:**
   ```bash
   python -m py_compile WPCT_F_07_01_box_plot_analysis.py
   ```

2. **Import Check:**
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.getOrCreate()
   # Test functions with mock data
   ```

3. **Integration Testing:**
   - Test with sample ADaM/SDTM datasets
   - Compare outputs to SAS results
   - Verify Delta table structure

## References

- **PhUSE Scripts Repository:** https://github.com/phuse-org/phuse-scripts
- **PhUSE Central Tendency White Paper:** https://www.phuse.eu/css
- **Migration Framework:** `../README.md`
- **Airflow DAG:** `../dags/clinical_analysis_pipeline.py`
- **PhUSE Utilities:** `../utils/phuse_utils.py`

## Support

For questions or issues:
- Review the conversion notes in each notebook
- Check the main migration framework README
- Consult the original SAS scripts
- Contact: @taylor-curran

## License

These notebooks are part of the PhUSE Scripts migration project and follow the same licensing as the original PhUSE Scripts repository.
