from scripts.extract import extract_data, process_data, log
from scripts.upload_to_s3 import upload_to_s3
import os
import sys
from datetime import datetime

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# Constants
url = 'https://restcountries.com/v3.1/all'
bucket_name = 'cde-countries'


def run_extraction(**kwargs):
    # Call the extraction function with provided arguments
    raw_data = extract_data(url)
    if not raw_data:
        log("Failed to extract data")
        return None
    
    output_file = process_data(raw_data)
    if not output_file:
        log("Failed to process data")
        return None
    kwargs['ti'].xcom_push(key='extracted_and_process_data', value=output_file)
    log((f"File path saved to XCom: {output_file}"))
    return output_file

def run_upload_to_s3(**kwargs):
    # Pull the output file path from the extraction task
    file_path = kwargs['ti'].xcom_pull(task_ids='extract_and_process_task', key='file_path')
    if not file_path:
        raise Exception("No file path received from extraction task")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    object_name = f'raw/countries_{timestamp}.parquet'

    success = upload_to_s3(file_path=file_path, bucket_name=bucket_name, object_name=object_name)
    log(f"File {file_path} successfully uploaded to S3.")
    if not success:
        raise Exception("Failed to upload file to S3.")
    
    log("Process completed successfully")

    return success



# def main():
    
#     raw_data = extract_data(url)
#     if not raw_data:
#         log("Failed to extract data")
#         return None
    
#     output_file = process_data(raw_data)
#     if not output_file:
#         log("Failed to process data")
#         return None
#     timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#     object_name = f'raw/countries_{timestamp}.parquet'
#     if upload_to_s3(file_path=output_file, bucket_name=bucket_name, object_name=object_name):
#         log(f"File {output_file} successfully uploaded to S3.")
#     else:
#         log("Failed to upload file to S3.")
    
#     log("Process completed successfully")

#     return output_file

    
# if __name__ == "__main__":
#     main()

