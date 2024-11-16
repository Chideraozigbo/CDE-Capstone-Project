import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

sys.path.append(
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts")
)


from scripts.main import (
    load_s3_to_snowflake,
    run_extract_country_data,
    run_extraction,
    run_load_data_from_s3,
    run_upload_cleaned_to_s3,
    run_upload_raw_to_s3,
    truncate_snowflake_table,
)

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/countries_dbt"
DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"


profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default1",
        profile_args={
            "database": "country_database",
            "schema": "raw_country_schema",
        },
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


# Define the DAG
default_args = {
    "owner": "Chidera",
    "email": "chideraozigbo@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=3),
}

countries_dag = DAG(
    "countries_dag",
    default_args=default_args,
    schedule_interval="0 0 1 * *",
    start_date=datetime(2024, 6, 21),
    catchup=False,
)

extract_task = PythonOperator(
    task_id="extract_data_task",
    python_callable=run_extraction,
    dag=countries_dag,
)

raw_upload_task = PythonOperator(
    task_id="upload_raw_to_s3_task",
    python_callable=run_upload_raw_to_s3,
    dag=countries_dag,
)

raw_load_task = PythonOperator(
    task_id="load_data_from_s3_task",
    python_callable=run_load_data_from_s3,
    dag=countries_dag,
)

extract_country_data_task = PythonOperator(
    task_id="extract_country_data_task",
    python_callable=run_extract_country_data,
    dag=countries_dag,
)

cleaned_upload_task = PythonOperator(
    task_id="upload_cleaned_to_s3_task",
    python_callable=run_upload_cleaned_to_s3,
    dag=countries_dag,
)

truncate_table_task = PythonOperator(
    task_id="trucate_table_task",
    python_callable=truncate_snowflake_table,
    dag=countries_dag,
)

load_snowflake_table_task = PythonOperator(
    task_id="load_s3_to_snowflake_task",
    python_callable=load_s3_to_snowflake,
    dag=countries_dag,
)


dbt_tg = DbtTaskGroup(
    group_id="transform_data",
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args={"install_deps": True},
    default_args=default_args,
    dag=countries_dag,
)

(
    extract_task
    >> raw_upload_task
    >> raw_load_task
    >> extract_country_data_task
    >> cleaned_upload_task
    >> truncate_table_task
    >> load_snowflake_table_task
    >> dbt_tg
)
