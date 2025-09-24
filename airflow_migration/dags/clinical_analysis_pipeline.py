"""
Clinical Data Analysis Pipeline DAG with OpenLineage and OpenMetadata Integration

This Airflow DAG orchestrates the clinical data analysis pipeline with comprehensive
data lineage tracking and metadata management for pharmaceutical compliance.

The pipeline consists of four main tasks:
1. Data Ingestion: Load and process SDTM/ADaM datasets
2. Data Preprocessing: Clean and prepare data for analysis
3. Statistical Calculations: Perform statistical analysis
4. Visualization: Generate plots and reports

Configuration is managed through YAML files, replacing SAS macro variables.
OpenLineage captures data lineage automatically, while OpenMetadata serves
as the central metadata catalog for regulatory compliance.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

try:
    from airflow_provider_openmetadata.lineage.callback import success_callback, failure_callback
except ImportError:
    success_callback = None
    failure_callback = None

default_args = {
    'owner': 'clinical_data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_callback,
    'on_success_callback': success_callback,
}

dag = DAG(
    'clinical_data_analysis_pipeline',
    default_args=default_args,
    description='Clinical Data Analysis Pipeline with OpenLineage and OpenMetadata',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['clinical', 'analysis', 'pyspark', 'openlineage', 'pharmaceutical'],
)


data_ingestion_task = BashOperator(
    task_id='data_ingestion',
    bash_command='python {{ var.value.scripts_dir }}/data_ingestion.py --config {{ var.value.config_path }}',
    outlets={
        "tables": [
            "clinical_data.adam.adlbc",
            "clinical_data.sdtm.lb"
        ]
    },
    dag=dag,
)

data_preprocessing_task = BashOperator(
    task_id='data_preprocessing',
    bash_command='python {{ var.value.scripts_dir }}/data_preprocessing.py --config {{ var.value.config_path }}',
    inlets={
        "tables": [
            "clinical_data.adam.adlbc"
        ]
    },
    outlets={
        "tables": [
            "clinical_data.processed.lab_data_filtered",
            "clinical_data.processed.outliers_detected"
        ]
    },
    dag=dag,
)

statistical_analysis_task = BashOperator(
    task_id='statistical_analysis',
    bash_command='python {{ var.value.scripts_dir }}/statistical_analysis.py --config {{ var.value.config_path }}',
    inlets={
        "tables": [
            "clinical_data.processed.lab_data_filtered"
        ]
    },
    outlets={
        "tables": [
            "clinical_data.analysis.summary_statistics",
            "clinical_data.analysis.outlier_analysis"
        ]
    },
    dag=dag,
)

visualization_task = BashOperator(
    task_id='visualization',
    bash_command='python {{ var.value.scripts_dir }}/visualization.py --config {{ var.value.config_path }}',
    inlets={
        "tables": [
            "clinical_data.analysis.summary_statistics"
        ]
    },
    outlets={
        "tables": [
            "clinical_data.reports.box_plots",
            "clinical_data.reports.statistical_summaries"
        ]
    },
    dag=dag,
)

data_ingestion_task >> data_preprocessing_task >> statistical_analysis_task >> visualization_task


cleanup_task = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='''
    if [ -d "{{ var.value.temp_dir }}" ]; then
        find {{ var.value.temp_dir }} -type f -name "*.tmp" -delete
    fi
    ''',
    dag=dag,
)

visualization_task >> cleanup_task


dag.doc_md = """

This DAG implements a modernized clinical data analysis pipeline, migrated from SAS to Airflow and PySpark.


1. **Data Ingestion**: Loads clinical trial data (SDTM/ADaM) from source location
2. **Data Preprocessing**: Filters and prepares data for analysis
3. **Statistical Analysis**: Calculates summary statistics and identifies outliers
4. **Visualization**: Generates interactive box plots using Plotly


The pipeline is configured using a YAML configuration file, which replaces SAS macro variables.
Key configuration parameters include:

- Data sources (dataset, parameters, visits)
- Variable mappings
- Plotting options
- File paths


This DAG replaces the sequential execution of SAS scripts:
- `example_call_wpct-f.07.01.sas`: Initialization script
- `WPCT-F.07.01.sas`: Core analysis script

The statistical logic remains identical, just translated to PySpark syntax.
"""
