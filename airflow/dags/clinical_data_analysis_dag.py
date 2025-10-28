from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import sys
import os

sys.path.append('/home/ubuntu/repos/SAS')

from pyspark.data_ingestion import ingest_clinical_data
from pyspark.data_preprocessing import preprocess_clinical_data
from pyspark.statistical_analysis import calculate_statistics
from visualization.box_plots import generate_box_plots

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
    'clinical_data_analysis',
    default_args=default_args,
    description='Clinical trial data analysis pipeline - migrated from SAS',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['clinical', 'analytics', 'phuse'],
)

def data_ingestion_task(**context):
    """
    Task 1: Data ingestion - replaces %util_access_test_data functionality
    Reads ADLBC dataset and prepares for analysis
    """
    config = {
        'dataset': Variable.get('clinical_dataset', default_var='ADLBC'),
        'data_path': Variable.get('clinical_data_path', default_var='/home/ubuntu/repos/SAS/data/adam/'),
        'output_path': Variable.get('clinical_output_path', default_var='/tmp/clinical_analysis/')
    }
    
    result = ingest_clinical_data(config)
    return result

def data_preprocessing_task(**context):
    """
    Task 2: Data preprocessing and filtering
    Converts SAS WHERE clauses to PySpark DataFrame filters
    """
    config = {
        'measurement_variable': Variable.get('measurement_variable', default_var='ALB'),
        'visit_condition': Variable.get('visit_condition', default_var='and AVISITN in (0 2 4 6)'),
        'population_flag': Variable.get('population_flag', default_var='SAFFL'),
        'analysis_flag': Variable.get('analysis_flag', default_var='ANL01FL'),
        'input_path': '/tmp/clinical_analysis/raw_data',
        'output_path': '/tmp/clinical_analysis/preprocessed_data'
    }
    
    result = preprocess_clinical_data(config)
    return result

def statistical_analysis_task(**context):
    """
    Task 3: Statistical calculations
    Replaces PROC SUMMARY operations with PySpark groupBy().agg()
    """
    config = {
        'treatment_var': Variable.get('treatment_variable', default_var='trtp_short'),
        'treatment_num_var': Variable.get('treatment_num_variable', default_var='trtpn'),
        'measurement_var': Variable.get('measurement_variable', default_var='aval'),
        'visit_var': Variable.get('visit_variable', default_var='avisitn'),
        'input_path': '/tmp/clinical_analysis/preprocessed_data',
        'output_path': '/tmp/clinical_analysis/statistics'
    }
    
    result = calculate_statistics(config)
    return result

def visualization_task(**context):
    """
    Task 4: Visualization generation
    Replaces SAS SGRENDER with Python box plots
    """
    config = {
        'reference_lines': Variable.get('reference_lines', default_var='UNIFORM'),
        'max_boxes_per_page': int(Variable.get('max_boxes_per_page', default_var='20')),
        'output_format': Variable.get('output_format', default_var='html'),
        'input_path': '/tmp/clinical_analysis/statistics',
        'output_path': Variable.get('clinical_output_path', default_var='/tmp/clinical_analysis/') + 'visualizations/'
    }
    
    result = generate_box_plots(config)
    return result

task1 = PythonOperator(
    task_id='data_ingestion',
    python_callable=data_ingestion_task,
    dag=dag,
)

task2 = PythonOperator(
    task_id='data_preprocessing',
    python_callable=data_preprocessing_task,
    dag=dag,
)

task3 = PythonOperator(
    task_id='statistical_analysis',
    python_callable=statistical_analysis_task,
    dag=dag,
)

task4 = PythonOperator(
    task_id='visualization',
    python_callable=visualization_task,
    dag=dag,
)

task1 >> task2 >> task3 >> task4
