from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from .download import extract_country_data, get_json_data_from_s3
from .extract import extract_data, log
from .upload_to_s3 import upload_to_s3

# Constants
url = "https://restcountries.com/v3.1/all"
bucket_name = "cde-countries"


def run_extraction(**kwargs):
    log("Starting the extraction phase")
    json_file, json_path = extract_data(url)
    if json_file and json_path:
        log(f"Extraction completed successfully: {json_file}")
        kwargs["ti"].xcom_push(key="json_file", value=json_file)
        kwargs["ti"].xcom_push(key="json_path", value=json_path)
    else:
        raise Exception("Extraction failed")


def run_upload_raw_to_s3(**kwargs):
    json_file = kwargs["ti"].xcom_pull(
        task_ids="extract_data_task", key="json_file"
    )
    json_path = kwargs["ti"].xcom_pull(
        task_ids="extract_data_task", key="json_path"
    )
    object_s3_path = f"raw/{json_file}"
    upload_success = upload_to_s3(json_path, bucket_name, object_s3_path)
    if upload_success:
        log("Raw data file uploaded successfully")
        kwargs["ti"].xcom_push(key="object_s3_path", value=object_s3_path)
    else:
        raise Exception("Failed to upload raw data to S3")


def run_load_data_from_s3(**kwargs):
    object_s3_path = kwargs["ti"].xcom_pull(
        task_ids="upload_raw_to_s3_task", key="object_s3_path"
    )
    json_data = get_json_data_from_s3(bucket_name, object_s3_path)
    if json_data:
        log("Data loaded into DataFrame successfully")
        kwargs["ti"].xcom_push(key="json_data", value=json_data)
    else:
        raise Exception("Failed to load data from S3 into DataFrame")


def run_extract_country_data(**kwargs):
    json_data = kwargs["ti"].xcom_pull(
        task_ids="load_data_from_s3_task", key="json_data"
    )
    cleaned_file, cleaned_file_path = extract_country_data(json_data)
    if cleaned_file and cleaned_file_path:
        log(f"Country data saved to {cleaned_file_path}")
        kwargs["ti"].xcom_push(key="cleaned_file", value=cleaned_file)
        kwargs["ti"].xcom_push(
            key="cleaned_file_path", value=cleaned_file_path
        )
    else:
        raise Exception("Failed to extract and save country data")


def run_upload_cleaned_to_s3(**kwargs):
    cleaned_file = kwargs["ti"].xcom_pull(
        task_ids="extract_country_data_task", key="cleaned_file"
    )
    cleaned_file_path = kwargs["ti"].xcom_pull(
        task_ids="extract_country_data_task", key="cleaned_file_path"
    )

    if cleaned_file_path is None:
        log("Error: Cleaned file path is None. Unable to upload to S3.")
        raise Exception("Cleaned file path is None. Unable to upload to S3.")

    cleaned_object_s3_path = f"cleaned/{cleaned_file}"
    cleaned_upload_success = upload_to_s3(
        cleaned_file_path, bucket_name, cleaned_object_s3_path
    )
    if cleaned_upload_success:
        s3_full_path = f"s3://{bucket_name}/{cleaned_object_s3_path}"
        kwargs["ti"].xcom_push(key="cleaned_s3_path", value=s3_full_path)
        log("Cleaned data file uploaded successfully")
    else:
        raise Exception("Failed to upload cleaned data to S3")


def truncate_snowflake_table(**kwargs):
    try:
        log("Starting Snowflake table truncate operation...")
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default1")
        log(
            f"Successfully connected to Snowflake"
            f"using connection ID: {snowflake_hook}"
        )

        truncate_query = "TRUNCATE TABLE country_data"
        log(f"Executing truncate query: {truncate_query}")

        snowflake_hook.run(truncate_query)
        log("Successfully truncated Snowflake table")

    except Exception as e:
        log(f"Failed to truncate Snowflake table: {str(e)}")
        raise Exception(f"Failed to truncate Snowflake table: {str(e)}")


def load_s3_to_snowflake(**kwargs):
    try:
        log("Starting S3 to Snowflake data load operation...")

        s3_path = kwargs["ti"].xcom_pull(
            task_ids="upload_cleaned_to_s3_task", key="cleaned_s3_path"
        )
        log(f"Retrieved S3 path from XCom: {s3_path}")

        if not s3_path:
            log("S3 path is empty or None. Cannot proceed with data load.")
            raise Exception(
                "S3 path is empty or None. Cannot proceed with data load."
            )
        # AWS Credentials Stored on Airflow
        aws_access_key = Variable.get("aws_access_key")
        aws_secret_key = Variable.get("aws_secret_key")

        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default1")
        log(
            f"Successfully connected to Snowflake"
            f"using connection ID: {snowflake_hook}"
        )

        copy_query = f"""
            COPY INTO country_database.raw_country_schema.country_data
            FROM '{s3_path}'
            FILE_FORMAT = (TYPE = 'PARQUET')
            CREDENTIALS = (
                AWS_KEY_ID = '{aws_access_key}'
                AWS_SECRET_KEY = '{aws_secret_key}'
            )
            MATCH_BY_COLUMN_NAME = case_insensitive
        """

        log(f"Executing COPY query: {copy_query}")

        result = snowflake_hook.run(copy_query)
        log(f"Data load completed successfully. Query result: {result}")

        if isinstance(result, list) and len(result) > 0:
            rows_loaded = result[0].get("rows_loaded", 0)
            log(f"Number of rows loaded into Snowflake: {rows_loaded}")

    except Exception as e:
        log(f"Failed to load data from S3 to Snowflake: {str(e)}")
        raise Exception(f"Failed to load data from S3 to Snowflake: {str(e)}")



