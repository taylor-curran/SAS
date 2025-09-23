from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
import yaml
import os

config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'pipeline_config.yaml')
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

default_args = {
    'owner': 'clinical-data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'clinical_data_analysis_pipeline',
    default_args=default_args,
    description='Clinical trial data analysis pipeline migrated from SAS',
    schedule_interval='@daily',
    catchup=False,
    tags=['clinical', 'pharma', 'statistics', 'migration'],
)

data_ingestion_task = DatabricksSubmitRunOperator(
    task_id='data_ingestion',
    databricks_conn_id='databricks_default',
    new_cluster={
        'spark_version': '11.3.x-scala2.12',
        'node_type_id': 'i3.xlarge',
        'num_workers': 2,
        'spark_conf': {
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        }
    },
    spark_python_task={
        'python_file': 'dbfs:/scripts/data_ingestion.py',
        'parameters': [
            '--config-path', 'dbfs:/config/pipeline_config.yaml',
            '--dataset-name', config['data']['dataset_name'],
            '--input-path', config['data']['input_path'],
        ]
    },
    dag=dag,
)

data_preprocessing_task = DatabricksSubmitRunOperator(
    task_id='data_preprocessing',
    databricks_conn_id='databricks_default',
    new_cluster={
        'spark_version': '11.3.x-scala2.12',
        'node_type_id': 'i3.xlarge',
        'num_workers': 2,
    },
    spark_python_task={
        'python_file': 'dbfs:/scripts/data_preprocessing.py',
        'parameters': [
            '--config-path', 'dbfs:/config/pipeline_config.yaml',
            '--input-table', 'clinical_raw_data',
            '--output-table', 'clinical_filtered_data',
        ]
    },
    dag=dag,
)

statistical_calculations_task = DatabricksSubmitRunOperator(
    task_id='statistical_calculations',
    databricks_conn_id='databricks_default',
    new_cluster={
        'spark_version': '11.3.x-scala2.12',
        'node_type_id': 'i3.xlarge',
        'num_workers': 2,
    },
    spark_python_task={
        'python_file': 'dbfs:/scripts/statistical_analysis.py',
        'parameters': [
            '--config-path', 'dbfs:/config/pipeline_config.yaml',
            '--input-table', 'clinical_filtered_data',
            '--output-table', 'clinical_statistics',
        ]
    },
    dag=dag,
)

visualization_generation_task = DatabricksSubmitRunOperator(
    task_id='visualization_generation',
    databricks_conn_id='databricks_default',
    new_cluster={
        'spark_version': '11.3.x-scala2.12',
        'node_type_id': 'i3.xlarge',
        'num_workers': 1,
        'init_scripts': [
            {'dbfs': {'destination': 'dbfs:/scripts/install_plotly.sh'}}
        ]
    },
    spark_python_task={
        'python_file': 'dbfs:/scripts/visualization.py',
        'parameters': [
            '--config-path', 'dbfs:/config/pipeline_config.yaml',
            '--input-table', 'clinical_statistics',
            '--output-path', config['data']['output_path'],
        ]
    },
    dag=dag,
)

data_ingestion_task >> data_preprocessing_task >> statistical_calculations_task >> visualization_generation_task
