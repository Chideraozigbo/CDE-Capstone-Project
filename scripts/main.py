from extract import extract_data, log
from upload_to_s3 import upload_to_s3
from download_and_process import get_json_data_from_s3, extract_country_data

# Constants
url = 'https://restcountries.com/v3.1/all'
bucket_name = 'cde-countries'


def run_extraction(**kwargs):
    log("Starting the extraction phase")
    json_file, json_path = extract_data(url)
    if json_file and json_path:
        log(f"Extraction completed successfully: {json_file}")
        kwargs['ti'].xcom_push(key='json_file', value=json_file)
        kwargs['ti'].xcom_push(key='json_path', value=json_path)
    else:
        raise Exception("Extraction failed")


def run_upload_raw_to_s3(**kwargs):
    json_file = kwargs['ti'].xcom_pull(
        task_ids='extract_data_task', key='json_file')
    json_path = kwargs['ti'].xcom_pull(
        task_ids='extract_data_task', key='json_path')
    object_s3_path = f'raw/{json_file}'
    upload_success = upload_to_s3(json_path, bucket_name, object_s3_path)
    if upload_success:
        log("Raw data file uploaded successfully")
        kwargs['ti'].xcom_push(key='object_s3_path', value=object_s3_path)
    else:
        raise Exception("Failed to upload raw data to S3")


def run_load_data_from_s3(**kwargs):
    object_s3_path = kwargs['ti'].xcom_pull(
        task_ids='upload_raw_to_s3_task', key='object_s3_path')
    json_data = get_json_data_from_s3(bucket_name, object_s3_path)
    if json_data:
        log("Data loaded into DataFrame successfully")
        kwargs['ti'].xcom_push(key='json_data', value=json_data)
    else:
        raise Exception("Failed to load data from S3 into DataFrame")


def run_extract_country_data(**kwargs):
    json_data = kwargs['ti'].xcom_pull(
        task_ids='load_data_from_s3_task', key='json_data')
    cleaned_file, cleaned_file_path = extract_country_data(json_data)
    if cleaned_file and cleaned_file_path:
        log(f"Country data saved to {cleaned_file_path}")
        kwargs['ti'].xcom_push(key='cleaned_file', value=cleaned_file)
        kwargs['ti'].xcom_push(
            key='cleaned_file_path', value=cleaned_file_path)
    else:
        raise Exception("Failed to extract and save country data")


def run_upload_cleaned_to_s3(**kwargs):
    cleaned_file = kwargs['ti'].xcom_pull(
        task_ids='extract_country_data', key='cleaned_file')
    cleaned_file_path = kwargs['ti'].xcom_pull(
        task_ids='extract_country_data', key='cleaned_file_path')
    cleaned_object_s3_path = f'cleaned/{cleaned_file}'
    cleaned_upload_success = upload_to_s3(
        cleaned_file_path, bucket_name, cleaned_object_s3_path)
    if cleaned_upload_success:
        log("Cleaned data file uploaded successfully")
    else:
        raise Exception("Failed to upload cleaned data to S3")
