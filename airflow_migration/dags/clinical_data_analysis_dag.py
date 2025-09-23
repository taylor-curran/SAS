"""
Airflow DAG for Clinical Data Analysis Pipeline
Migrated from SAS WPCT-F.07.01 whitepaper implementation

This DAG replaces the SAS pipeline with PySpark processing for:
- Data ingestion from ADLBC dataset
- Data preprocessing and filtering
- Statistical calculations (means, quartiles, outliers)
- Box plot visualization generation
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'modules'))

from data_ingestion import ingest_adlbc_data
from data_preprocessing import preprocess_clinical_data
from statistical_calculations import calculate_statistics
from visualization_generation import generate_box_plots

default_args = {
    'owner': 'clinical-analytics',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clinical_data_analysis_pipeline',
    default_args=default_args,
    description='Clinical trial data analysis pipeline migrated from SAS',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['clinical', 'analytics', 'pyspark', 'migration'],
)

config = {
    'dataset': Variable.get('clinical_dataset', default_var='ADLBC'),
    'parameter': Variable.get('analysis_parameter', default_var='ALB'),
    'treatment_var': Variable.get('treatment_variable', default_var='trtp_short'),
    'treatment_num_var': Variable.get('treatment_num_variable', default_var='trtpn'),
    'measurement_var': Variable.get('measurement_variable', default_var='aval'),
    'low_limit_var': Variable.get('low_limit_variable', default_var='a1lo'),
    'high_limit_var': Variable.get('high_limit_variable', default_var='a1hi'),
    'population_flag': Variable.get('population_flag', default_var='saffl'),
    'analysis_flag': Variable.get('analysis_flag', default_var='anl01fl'),
    'ref_lines': Variable.get('reference_lines', default_var='UNIFORM'),
    'max_boxes_per_page': int(Variable.get('max_boxes_per_page', default_var='20')),
    'visit_filter': [0, 2, 4, 6],  # Equivalent to SAS: and AVISITN in (0 2 4 6)
    'output_path': Variable.get('output_path', default_var='/tmp/clinical_outputs'),
}

task_data_ingestion = PythonOperator(
    task_id='data_ingestion',
    python_callable=ingest_adlbc_data,
    op_kwargs={'config': config},
    dag=dag,
)

task_data_preprocessing = PythonOperator(
    task_id='data_preprocessing',
    python_callable=preprocess_clinical_data,
    op_kwargs={'config': config},
    dag=dag,
)

task_statistical_calculations = PythonOperator(
    task_id='statistical_calculations',
    python_callable=calculate_statistics,
    op_kwargs={'config': config},
    dag=dag,
)

task_visualization_generation = PythonOperator(
    task_id='visualization_generation',
    python_callable=generate_box_plots,
    op_kwargs={'config': config},
    dag=dag,
)

task_data_ingestion >> task_data_preprocessing >> task_statistical_calculations >> task_visualization_generation
