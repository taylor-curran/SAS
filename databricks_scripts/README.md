# Databricks Adverse Events Analysis Scripts

This directory contains PySpark implementations of SAS adverse events analysis scripts, converted for use in Databricks environments.

## Overview

Four core SAS macros have been converted to PySpark:

1. **ae_setup.py** - Domain merging and base dataset creation
2. **ae_aggregate.py** - Standard adverse events aggregation (Analysis A/B/C/D)
3. **ae_oncology_aggregate.py** - Oncology-specific toxicity grade analysis
4. **ae_meddra.py** - MedDRA hierarchy traversal and analysis

## Directory Structure

```
databricks_scripts/
├── loaders/
│   ├── __init__.py
│   └── databricks_adam_loader.py    # Flexible data loader class
├── adverse_events/
│   ├── __init__.py
│   ├── ae_setup.py                  # Domain merging
│   ├── ae_aggregate.py              # Standard AE aggregation
│   ├── ae_oncology_aggregate.py     # Toxicity grade analysis
│   └── ae_meddra.py                 # MedDRA hierarchy analysis
├── utils/
│   ├── __init__.py
│   └── ae_utils.py                  # Shared utility functions
├── README.md
└── requirements.txt
```

## Prerequisites

### Databricks Runtime
- Databricks Runtime 11.3 LTS or higher
- Python 3.9+
- PySpark 3.3.0+

### Required Libraries
```
pyspark>=3.3.0
scipy>=1.9.0  # For Fisher's exact test in oncology analysis
```

Install in Databricks:
```python
%pip install scipy>=1.9.0
```

### Data Requirements

#### SDTM Domains
The scripts require the following SDTM domains:
- **AE** (Adverse Events) - Required columns: USUBJID, AEBODSYS, AEDECOD, AESER (optional), AESEV (optional), AETOXGR (optional)
- **DM** (Demographics) - Required columns: USUBJID, ACTARM or ARM, ARMCD (optional)
- **EX** (Exposure) - Required columns: USUBJID

#### MedDRA Hierarchy (for ae_meddra.py)
A Delta Lake table containing the MedDRA hierarchy with columns:
- PT_CODE, PT_NAME (Preferred Term)
- HLT_CODE, HLT_NAME (High Level Term)
- HLGT_CODE, HLGT_NAME (High Level Group Term)
- SOC_CODE, SOC_NAME (System Organ Class)
- AEBODSYS, AEDECOD (for matching with AE data)

## DatabricksAdamLoader Class

The `DatabricksAdamLoader` class provides flexible data access supporting:

### Data Sources
1. **Delta Lake tables** - Using catalog.schema.table format
2. **XPT files** - SAS transport format from DBFS
3. **SAS7BDAT files** - Native SAS format from DBFS

### Example Usage

```python
from loaders.databricks_adam_loader import DatabricksAdamLoader

# Load from Delta Lake
loader = DatabricksAdamLoader(
    spark=spark,
    source_type="delta",
    catalog="clinical_dev",
    schema="sdtm"
)

ae_df = loader.load_domain("ae")
dm_df = loader.load_domain("dm")

# Load from XPT files
loader = DatabricksAdamLoader(
    spark=spark,
    source_type="xpt",
    file_path="/dbfs/mnt/clinical/data"
)

ae_df = loader.load_domain("ae")

# Write results to Delta Lake
output_loader = DatabricksAdamLoader(
    spark=spark,
    source_type="delta",
    catalog="clinical_dev",
    schema="results"
)

output_loader.write_delta_table(
    df=result_df,
    table_name="ae_analysis_a",
    mode="overwrite"
)
```

## Script Usage

### 1. ae_setup.py

Merges AE, DM, and EX domains to create base analysis datasets.

**Databricks Widgets:**
- `source_type` - "delta", "xpt", or "sas7bdat"
- `catalog` - Source catalog name (for Delta)
- `schema` - Source schema name (for Delta)
- `file_path` - Base path for files (for XPT/SAS7BDAT)
- `output_catalog` - Output catalog name
- `output_schema` - Output schema name

**Output Tables:**
- `{output_catalog}.{output_schema}.ae_base` - Full AE dataset with ARM_NUM
- `{output_catalog}.{output_schema}.ae_base_by_subj_term` - One record per subject per term
- `{output_catalog}.{output_schema}.arm_mapping` - Arm number to name mapping

**Example:**
```python
%run ./adverse_events/ae_setup

# Results are automatically written to Delta tables
# View the arm mapping
display(arm_mapping)
```

### 2. ae_aggregate.py

Performs standard adverse events aggregation (Analysis A/B/C/D).

**Databricks Widgets:**
- All widgets from ae_setup.py, plus:
- `serious_only` - "Yes" or "No" (filters to AESER='Y')
- `percent_threshold` - Minimum percentage for Analysis A (default: "2.0")

**Analysis Types:**
- **Analysis A** - AEs by preferred term (non-serious, with threshold)
- **Analysis B** - Serious AEs by preferred term
- **Analysis C** - AEs by severity level (non-serious)
- **Analysis D** - Serious AEs by severity level

**Output Tables:**
- `{output_catalog}.{output_schema}.ae_analysis_a` or `ae_analysis_b`
- `{output_catalog}.{output_schema}.ae_analysis_c` or `ae_analysis_d`

**Example:**
```python
%run ./adverse_events/ae_aggregate

# View Analysis A results (non-serious AEs by preferred term)
display(pt_result)

# View Analysis C results (AEs by severity)
display(sev_result)
```

### 3. ae_oncology_aggregate.py

Oncology-specific analysis focusing on toxicity grades with statistical comparisons.

**Databricks Widgets:**
- All widgets from ae_setup.py, plus:
- `experimental_arm` - Experimental arm number (default: "1")
- `control_arm` - Control arm number (default: "2")
- `continuity_correction` - Value for zero cell correction (default: "0.5")

**Statistical Measures:**
- Subject counts and percentages per arm
- Risk difference
- Relative risk
- Fisher's exact test p-value

**Output Tables:**
- `{output_catalog}.{output_schema}.ae_oncology_analysis`

**Example:**
```python
%run ./adverse_events/ae_oncology_aggregate

# View results with statistical comparisons
display(result)
```

### 4. ae_meddra.py

Analyzes adverse events across MedDRA hierarchy levels.

**Databricks Widgets:**
- All widgets from ae_setup.py, plus:
- `meddra_catalog` - MedDRA hierarchy catalog (default: "clinical_dev")
- `meddra_schema` - MedDRA hierarchy schema (default: "reference")
- `meddra_table` - MedDRA hierarchy table name (default: "mdhier")

**Hierarchy Levels:**
1. SOC (System Organ Class)
2. HLGT (High Level Group Term)
3. HLT (High Level Term)
4. PT (Preferred Term)

**Output Tables:**
- `{output_catalog}.{output_schema}.ae_meddra_soc`
- `{output_catalog}.{output_schema}.ae_meddra_hlgt`
- `{output_catalog}.{output_schema}.ae_meddra_hlt`
- `{output_catalog}.{output_schema}.ae_meddra_pt`

**Example:**
```python
%run ./adverse_events/ae_meddra

# View SOC level results
display(soc_result)

# View PT level results
display(pt_result)
```

## Typical Workflow

### Complete Analysis Pipeline

```python
# 1. Set up base datasets
%run ./adverse_events/ae_setup

# 2. Run standard AE aggregation
%run ./adverse_events/ae_aggregate

# 3. Run oncology-specific analysis (if applicable)
%run ./adverse_events/ae_oncology_aggregate

# 4. Run MedDRA hierarchy analysis
%run ./adverse_events/ae_meddra
```

### Custom Widget Values

```python
# Set custom widget values before running
dbutils.widgets.text("catalog", "my_catalog")
dbutils.widgets.text("schema", "my_schema")
dbutils.widgets.text("output_catalog", "my_output_catalog")
dbutils.widgets.text("output_schema", "my_output_schema")

%run ./adverse_events/ae_setup
```

## Key Differences from SAS Implementation

1. **Data Loading**: Supports both Delta Lake tables and file formats (XPT, SAS7BDAT)
2. **Configuration**: Uses Databricks widgets instead of SAS macro parameters
3. **Output**: Writes to Delta Lake tables instead of SAS datasets
4. **Statistical Functions**: Uses scipy for Fisher's exact test
5. **Column Names**: Enforces CDISC uppercase naming convention
6. **Error Handling**: Python exceptions instead of SAS macro error handling

## Troubleshooting

### Common Issues

**"Neither ACTARM nor ARM found in DM dataset"**
- Ensure your DM domain contains either ACTARM or ARM column

**"catalog and schema are required for Delta source_type"**
- Provide both catalog and schema parameters when using source_type="delta"

**MedDRA hierarchy not found**
- Verify the MedDRA table exists at the specified catalog.schema.table location
- Check that the table has the required columns (SOC_NAME, HLGT_NAME, HLT_NAME, PT_NAME)

**"AETOXGR column not found"**
- The oncology analysis requires AETOXGR in the AE domain
- This column is optional for other analyses

### Performance Optimization

1. **Partitioning**: Consider partitioning large Delta tables by ARM_NUM or AEBODSYS
2. **Caching**: Cache intermediate DataFrames for iterative analysis
3. **Broadcast Joins**: The loader uses standard joins; consider broadcast for small reference tables

## Testing

These scripts are designed for Databricks environments and cannot be tested locally without a Databricks cluster. Testing should include:

1. **Data Validation**: Verify input domains have required columns
2. **Output Verification**: Compare counts with SAS output using test data
3. **Edge Cases**: Test with missing values, single-arm studies, zero counts
4. **Performance**: Monitor execution time for large datasets

## Support

For issues or questions:
- Review this README
- Check Databricks cluster configuration
- Verify data source accessibility
- Ensure required libraries are installed

## License

Inherits license from the parent SAS repository.

## Contributors

Converted from SAS to PySpark by Devin AI
Original SAS code: PhUSE CDISC Standard Analyses Working Group
