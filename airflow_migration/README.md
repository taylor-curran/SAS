# Clinical Data Analysis Pipeline - SAS to Airflow Migration

This project contains the migrated clinical data analysis pipeline from SAS to Apache Airflow with Databricks PySpark. The pipeline processes clinical trial data in SDTM/ADaM format, performs statistical analysis on laboratory measurements, and generates box plot visualizations.

## Project Structure

```
airflow_migration/
├── config/
│   └── pipeline_config.yaml          # Configuration parameters
├── dags/
│   └── clinical_data_analysis_dag.py  # Airflow DAG definition
├── scripts/
│   ├── data_ingestion.py             # Task 1: Data loading and preparation
│   ├── data_preprocessing.py         # Task 2: Data filtering and outlier detection
│   ├── statistical_analysis.py      # Task 3: Summary statistics calculation
│   ├── visualization.py             # Task 4: Box plot generation
│   └── install_plotly.sh            # Databricks init script
├── utils/
│   └── phuse_utilities.py           # PhUSE utility functions
├── docs/
│   └── migration_guide.md           # Detailed migration documentation
└── README.md                        # This file
```

## Quick Start

### Prerequisites
- Apache Airflow 2.5+
- Databricks workspace
- Python 3.8+
- Access to clinical data in SDTM/ADaM format

### Installation

1. **Deploy to Airflow**:
   ```bash
   # Copy DAG to your Airflow DAGs folder
   cp dags/clinical_data_analysis_dag.py $AIRFLOW_HOME/dags/
   ```

2. **Upload to Databricks**:
   ```bash
   # Upload scripts and config to DBFS
   databricks fs cp -r scripts/ dbfs:/scripts/
   databricks fs cp -r config/ dbfs:/config/
   databricks fs cp -r utils/ dbfs:/utils/
   ```

3. **Configure Airflow Connection**:
   - Create Databricks connection in Airflow UI
   - Set connection ID as `databricks_default`

4. **Update Configuration**:
   - Edit `config/pipeline_config.yaml` with your data paths
   - Adjust analysis parameters as needed

### Running the Pipeline

1. Enable the DAG in Airflow UI
2. Trigger manual run or wait for scheduled execution
3. Monitor progress through Airflow task logs
4. View generated visualizations in configured output location

## Pipeline Overview

The pipeline consists of 4 sequential tasks:

1. **Data Ingestion**: Loads ADLBC dataset and adds timepoint variables
2. **Data Preprocessing**: Applies clinical data filters and outlier detection
3. **Statistical Calculations**: Computes summary statistics (n, mean, std, median, quartiles)
4. **Visualization Generation**: Creates interactive box plots with statistical overlays

## Configuration

Key configuration parameters in `config/pipeline_config.yaml`:

```yaml
analysis:
  parameter: 'ALB'              # Clinical parameter to analyze
  visits: [0, 2, 4, 6]         # Visit numbers to include
  reference_lines: 'UNIFORM'    # Reference line configuration
  max_boxes_per_page: 20       # Pagination limit

variables:
  treatment_var: 'trtp_short'   # Treatment variable
  measurement_var: 'aval'       # Measurement variable
  population_flag: 'saffl'      # Safety population flag
  analysis_flag: 'anl01fl'      # Analysis flag

data:
  dataset_name: 'ADLBC'         # Input dataset name
  input_path: '/data/adam/'     # Data source path
  output_path: '/results/'      # Output location
```

## Original SAS Mapping

This migration preserves the exact statistical logic from the original SAS scripts:

- **`example_call_wpct-f.07.01.sas`** → Data Ingestion Task
- **`WPCT-F.07.01.sas` lines 240-249** → Data Preprocessing Task  
- **`WPCT-F.07.01.sas` lines 343-348** → Statistical Calculations Task
- **`WPCT-F.07.01.sas` lines 398-423** → Visualization Generation Task

## Key Features

- **Exact Statistical Equivalence**: All calculations match original SAS output
- **Scalable Processing**: Distributed PySpark for large datasets
- **Interactive Visualizations**: HTML box plots with Plotly
- **Cloud Native**: Designed for cloud storage and compute
- **Automated Orchestration**: Airflow scheduling and monitoring
- **Configurable**: YAML-based parameter management

## Testing

### Statistical Validation
```python
# Compare outputs between SAS and PySpark
python scripts/validate_statistics.py --sas-output sas_results.csv --spark-output spark_results.csv
```

### DAG Syntax Check
```python
# Validate Airflow DAG syntax
python -m py_compile dags/clinical_data_analysis_dag.py
```

### Sample Data Test
```bash
# Run with sample data
airflow dags test clinical_data_analysis_pipeline 2024-01-01
```

## Troubleshooting

### Common Issues

1. **Data Loading Errors**:
   - Verify data path configuration
   - Check SAS dataset format compatibility
   - Ensure proper Databricks permissions

2. **Statistical Calculation Failures**:
   - Validate input data quality
   - Check for missing required variables
   - Review filter criteria

3. **Visualization Generation Issues**:
   - Ensure Plotly installation on cluster
   - Check output path permissions
   - Verify statistics data availability

### Logs and Monitoring

- **Airflow Logs**: Available in Airflow UI task instance logs
- **Spark Logs**: Accessible through Databricks cluster logs
- **Application Logs**: Python logging output in task execution logs

## Performance Optimization

### Spark Configuration
```yaml
spark:
  executor_memory: '4g'
  driver_memory: '2g'
  max_result_size: '1g'
```

### Data Partitioning
- Partition by study and parameter for large datasets
- Use Delta Lake format for optimized storage
- Enable adaptive query execution

## Support and Documentation

- **Migration Guide**: See `docs/migration_guide.md` for detailed mapping
- **PhUSE Utilities**: Reference `utils/phuse_utilities.py` for function documentation
- **Configuration Reference**: All parameters documented in `config/pipeline_config.yaml`

## Contributing

1. Follow existing code structure and naming conventions
2. Add unit tests for new functionality
3. Update documentation for configuration changes
4. Validate statistical accuracy against SAS baseline

## License

This project maintains the MIT license from the original PhUSE scripts.
