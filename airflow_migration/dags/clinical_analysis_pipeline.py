"""
Clinical Data Analysis Pipeline DAG

This Airflow DAG orchestrates the clinical data analysis pipeline, replacing the
sequential SAS script execution with a 4-task workflow: data ingestion, preprocessing,
statistical calculations, and visualization.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clinical_data_analysis_pipeline',
    default_args=default_args,
    description='Clinical Data Analysis Pipeline',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['clinical', 'analysis', 'pyspark'],
)


data_ingestion_task = BashOperator(
    task_id='data_ingestion',
    bash_command='python {{ var.value.scripts_dir }}/data_ingestion.py --config {{ var.value.config_path }}',
    dag=dag,
)

data_preprocessing_task = BashOperator(
    task_id='data_preprocessing',
    bash_command='python {{ var.value.scripts_dir }}/data_preprocessing.py --config {{ var.value.config_path }}',
    dag=dag,
)

statistical_analysis_task = BashOperator(
    task_id='statistical_analysis',
    bash_command='python {{ var.value.scripts_dir }}/statistical_analysis.py --config {{ var.value.config_path }}',
    dag=dag,
)

visualization_task = BashOperator(
    task_id='visualization',
    bash_command='python {{ var.value.scripts_dir }}/visualization.py --config {{ var.value.config_path }}',
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
