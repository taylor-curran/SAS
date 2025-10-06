# Adverse Events Processing Scripts for Databricks

This directory contains PySpark scripts for processing adverse events (AE) data in Databricks, converted from the original SAS scripts in the `taylor-curran/phuse-scripts` repository.

## Overview

These scripts provide a complete pipeline for adverse events analysis in clinical trials:

1. **ae_setup.py** - Merge AE, Demographics (DM), and Exposure (EX) domains
2. **ae_aggregate.py** - Aggregate adverse events by term, severity, and treatment arm
3. **ae_oncology_aggregate.py** - Oncology-specific AE processing with toxicity grades
4. **ae_meddra.py** - MedDRA hierarchy matching and statistical analysis

## Prerequisites

### Data Requirements

All scripts expect CDISC SDTM-compliant data stored in Delta Lake tables:

- **AE (Adverse Events)**: Required columns: USUBJID, AEBODSYS, AEDECOD, AESTDTC
- **DM (Demographics)**: Required columns: USUBJID, ARM or ACTARM
- **EX (Exposure)**: Required columns: USUBJID, EXSTDTC, EXENDTC
- **MedDRA Hierarchy**: Required columns: SOC_NAME, HLGT_NAME, HLT_NAME, PT_NAME, PRIMARY_SOC_FG

### Databricks Environment

- Databricks Runtime 13.3 LTS or higher
- Python 3.10+
- Required libraries (install via cluster configuration):
  ```
  scipy>=1.10.0
  numpy>=1.24.0
  pandas>=2.0.0
  ```

## Script Descriptions

### 1. ae_setup.py - Domain Merging and Setup

Merges adverse events with demographics and exposure data to create a baseline dataset for analysis.

**Key Features:**
- Validates required columns across domains
- Converts date fields (AESTDTC, RFSTDTC, EXSTDTC) to proper date types
- Filters AEs to safety population (events during or within lag period after treatment)
- Assigns arm numbers for analysis
- Handles treatment start/end dates with configurable study lag

**Databricks Widgets:**
- `catalog` (default: "main") - Delta Lake catalog name
- `schema` (default: "sdtm") - Delta Lake schema name
- `output_catalog` (default: "main") - Output catalog
- `output_schema` (default: "results") - Output schema
- `output_table` (default: "ae_base") - Output table name
- `study_lag` (default: "30") - Days after treatment end to include AEs
- `source_type` (default: "delta") - Data source type ("delta" or "xpt")
- `ae_path`, `dm_path`, `ex_path` - Paths for XPT files (if source_type="xpt")

**Output:**
- Delta table: `{output_catalog}.{output_schema}.{output_table}`
- Columns: USUBJID, ARM, ARM_NUM, AEBODSYS, AEDECOD, dates, AESER, AESEV, AETOXGR (if available)

**Usage:**
```python
# Run as Databricks notebook
%run ./ae_setup
```

### 2. ae_aggregate.py - AE Aggregation and Counting

Aggregates adverse events by preferred term, severity, and treatment arm with statistical summaries.

**Key Features:**
- Analysis A: All AEs by preferred term (filtered by incidence threshold)
- Analysis B: Serious AEs only (AESER='Y')
- Analysis C: AEs by severity level (AESEV)
- Analysis D: Serious AEs by severity level
- Calculates subject counts and percentages per treatment arm
- Supports filtering by minimum incidence percentage

**Databricks Widgets:**
- `input_catalog` (default: "main")
- `input_schema` (default: "results")
- `input_table` (default: "ae_base")
- `output_catalog` (default: "main")
- `output_schema` (default: "results")
- `output_prefix` (default: "ae_analysis_")
- `filter_pct_threshold` (default: "2") - Minimum % for Analysis A

**Outputs:**
- `{output_prefix}all_aes` - All AEs above threshold
- `{output_prefix}serious_aes` - Serious AEs only
- `{output_prefix}aes_by_severity` - AEs by severity
- `{output_prefix}serious_aes_by_severity` - Serious AEs by severity

**Usage:**
```python
# Run as Databricks notebook
%run ./ae_aggregate
```

### 3. ae_oncology_aggregate.py - Oncology-Specific Processing

Processes adverse events with toxicity grades and performs statistical comparisons between treatment arms.

**Key Features:**
- Aggregates by toxicity grade (AETOXGR: 1-5)
- Keeps highest toxicity grade per subject per AE term
- Creates Grade 3/4 and Grade 3/4/5 combination groups
- Statistical comparisons:
  - Risk difference with confidence intervals
  - Relative risk with confidence intervals
  - Odds ratio with confidence intervals
  - Fisher's exact test p-values
- Configurable continuity correction (0, 0.5, 1, or reciprocal method)

**Databricks Widgets:**
- `input_catalog` (default: "main")
- `input_schema` (default: "results")
- `input_table` (default: "ae_base")
- `output_catalog` (default: "main")
- `output_schema` (default: "results")
- `output_table` (default: "ae_oncology_analysis")
- `continuity_correction` (default: "0.5") - Options: "0", "0.5", "1", "arm"
- `exp_arm` (default: "1") - Experimental arm number
- `ctl_arm` (default: "2") - Control arm number

**Outputs:**
- `{output_table}` - Aggregated counts by toxicity grade
- `{output_table}_stats` - Statistical comparisons between arms

**Usage:**
```python
# Run as Databricks notebook
%run ./ae_oncology_aggregate
```

### 4. ae_meddra.py - MedDRA Hierarchy and Statistical Analysis

Matches adverse event terms to MedDRA dictionary hierarchy and performs statistical analysis at each level.

**Key Features:**
- Loads MedDRA hierarchy from Delta table
- Matches AE terms to MedDRA preferred terms (PT)
- Traverses hierarchy: PT → HLT → HLGT → SOC
- Aggregates counts at each MedDRA level
- Statistical comparisons between treatment arms:
  - Risk difference
  - Fisher's exact test p-values
- Optional designated medical event (DME) flagging

**Databricks Widgets:**
- `input_catalog` (default: "main")
- `input_schema` (default: "results")
- `input_table` (default: "ae_base")
- `meddra_catalog` (default: "main")
- `meddra_schema` (default: "sdtm")
- `meddra_table` (default: "mdhier_14_0")
- `output_catalog` (default: "main")
- `output_schema` (default: "results")
- `output_prefix` (default: "ae_meddra_")
- `dme_table` (optional) - DME list table name
- `exp_arm` (default: "1")
- `ctl_arm` (default: "2")

**Outputs:**
- `{output_prefix}soc_counts` - SOC level aggregation
- `{output_prefix}soc_stats` - SOC level statistics
- `{output_prefix}hlgt_counts` - HLGT level aggregation
- `{output_prefix}hlgt_stats` - HLGT level statistics
- `{output_prefix}hlt_counts` - HLT level aggregation
- `{output_prefix}hlt_stats` - HLT level statistics
- `{output_prefix}pt_counts` - PT level aggregation
- `{output_prefix}pt_stats` - PT level statistics
- `{output_prefix}full_hierarchy` - Full dataset with MedDRA hierarchy

**Usage:**
```python
# Run as Databricks notebook
%run ./ae_meddra
```

## Utilities

### DatabricksAdamLoader

Class for loading SDTM domains from Delta Lake or XPT files.

**Methods:**
- `load_domain(domain, source_path, source_type)` - Generic domain loader
- `load_ae()` - Load Adverse Events domain
- `load_dm()` - Load Demographics domain
- `load_ex()` - Load Exposure domain
- `load_meddra_hierarchy()` - Load MedDRA hierarchy

### Statistical Functions

Functions for statistical calculations:
- `calculate_fisher_exact()` - Fisher's exact test
- `calculate_risk_difference()` - Risk difference with CI
- `calculate_relative_risk()` - Relative risk with CI
- `calculate_odds_ratio()` - Odds ratio with CI
- `apply_continuity_correction()` - Apply continuity correction

## Configuration

The `config/ae_analysis_config.yaml` file provides default configuration values:

```yaml
data_sources:
  catalog: "main"
  schema: "sdtm"
  
analysis:
  study_lag: 30
  continuity_correction: "0.5"
  filter_pct_threshold: 2.0
  
output:
  catalog: "main"
  schema: "results"
```

## Workflow Example

Typical workflow for adverse events analysis:

```python
# Step 1: Merge domains and create base dataset
%run ./ae_setup

# Step 2: Aggregate by term and severity
%run ./ae_aggregate

# Step 3: Oncology-specific analysis (if applicable)
%run ./ae_oncology_aggregate

# Step 4: MedDRA hierarchy analysis
%run ./ae_meddra
```

## Data Validation

All scripts include validation checks:
- Required column verification
- Date format validation
- Treatment arm consistency
- MedDRA hierarchy completeness

Errors are raised with descriptive messages if validation fails.

## Performance Considerations

- Scripts use Spark broadcast joins for small lookup tables (MedDRA hierarchy)
- Delta Lake tables provide optimized storage and query performance
- Databricks runtime optimizations are automatically applied
- Consider partitioning large tables by ARM_NUM for faster aggregation

## Troubleshooting

### Common Issues

1. **Missing Columns**: Verify SDTM data includes all required columns
2. **Date Conversion Errors**: Ensure date fields use ISO 8601 format (YYYY-MM-DD)
3. **MedDRA Matching Failures**: Check that PT_NAME in MedDRA matches AEDECOD in AE domain
4. **Import Errors**: Verify scipy, numpy, and pandas are installed on cluster

### Getting Help

For issues or questions:
- Review the original SAS scripts in `taylor-curran/phuse-scripts/tested/SAS/macros/`
- Check Delta Lake table schemas match expected structure
- Verify Databricks cluster configuration includes required libraries

## Migration from SAS

These scripts are direct conversions from SAS macros:
- `ae_setup.sas` → `ae_setup.py`
- `ae_aggregate.sas` → `ae_aggregate.py`
- `ae_oncology_aggregate.sas` → `ae_oncology_aggregate.py`
- `ae_meddra.sas` → `ae_meddra.py`

Key translation patterns:
- SAS DATA steps → PySpark DataFrame transformations
- SAS PROC SQL → PySpark SQL operations
- SAS PROC FREQ → PySpark groupBy/agg operations
- SAS hash objects → PySpark joins
- SAS macro variables → Databricks widgets

## License

This code is part of the PHUSE Scripts repository migration project.

## Contact

For questions or contributions, please contact the repository maintainers.
