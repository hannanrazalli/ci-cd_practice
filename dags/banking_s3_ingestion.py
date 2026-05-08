from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Adding 'include' directory to sys.path for Docker compatibility
sys.path.append(os.path.join(os.getenv('AIRFLOW_HOME', '/usr/local/airflow'), 'include'))

try:
    from s3_ingestor import run_s3_ingestion
except ImportError:
    # Fallback for different directory structures
    from include.s3_ingestor import run_s3_ingestion

default_args = {
    'owner': 'hannan',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='banking_s3_ingestion_pipeline',
    default_args=default_args,
    description='Ingest banking transaction data from Python to S3',
    start_date=datetime(2026, 5, 1),
    schedule='@daily',
    catchup=True,
    tags=['finance', 's3', 'ingestion']
) as dag:

    ingest_to_s3_task = PythonOperator(
        task_id='generate_and_ingest_to_s3',
        python_callable=run_s3_ingestion,
        op_kwargs={
            'execution_date': '{{ ds }}' 
        }
    )

    ingest_to_s3_task