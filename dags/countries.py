from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

from scripts.main import run_extraction, run_upload_raw_to_s3, run_load_data_from_s3, run_extract_country_data, run_upload_cleaned_to_s3

# Define the DAG
default_args = {
    'owner': 'Chidera',
    'email': 'chideraozigbo@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=10)
}

countries_dag = DAG(
    'countries_dag',
    default_args=default_args,
    schedule_interval='0/10 * * * *',
    start_date=datetime(2024, 6, 21),
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=run_extraction,
    provides_context=True,
    dag=countries_dag
)

raw_upload_task = PythonOperator(
    task_id='upload_raw_to_s3_task',
    python_callable=run_upload_raw_to_s3,
    provides_context=True,
    dag=countries_dag
)

raw_load_task = PythonOperator(
    task_id='load_data_from_s3_task',
    python_callable=run_load_data_from_s3,
    provides_context=True,
    dag=countries_dag
)

extract_country_data_task = PythonOperator(
    task_id='extract_country_data_task',
    python_callable=run_extract_country_data,
    provides_context=True,
    dag=countries_dag
)

cleaned_upload_task = PythonOperator(
    task_id='upload_cleaned_to_s3_task',
    python_callable=run_upload_cleaned_to_s3,
    provides_context=True,
    dag=countries_dag
)


extract_task >> raw_upload_task >> raw_load_task >> extract_country_data_task >> cleaned_upload_task