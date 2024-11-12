from extract import extract_data, log
from upload_to_s3 import upload_to_s3
from datetime import datetime
from download_and_process import get_json_data_from_s3, extract_country_data




# Constants
url = 'https://restcountries.com/v3.1/all'
bucket_name = 'cde-countries'


# def run_extraction(**kwargs):
#     # Call the extraction function with provided arguments
#     raw_data = extract_data(url)
#     if not raw_data:
#         log("Failed to extract data")
#         return None
    
#     output_file = process_data(raw_data)
#     if not output_file:
#         log("Failed to process data")
#         return None
#     kwargs['ti'].xcom_push(key='extracted_and_process_data', value=output_file)
#     log((f"File path saved to XCom: {output_file}"))
#     return output_file

# def run_upload_to_s3(**kwargs):
#     # Pull the output file path from the extraction task
#     file_path = kwargs['ti'].xcom_pull(task_ids='extract_and_process_task', key='file_path')
#     if not file_path:
#         raise Exception("No file path received from extraction task")
#     timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#     object_name = f'raw/countries_{timestamp}.parquet'

#     success = upload_to_s3(file_path=file_path, bucket_name=bucket_name, object_name=object_name)
#     log(f"File {file_path} successfully uploaded to S3.")
#     if not success:
#         raise Exception("Failed to upload file to S3.")
    
#     log("Process completed successfully")

#     return success



def main():
    log("Starting the ETL process")

    # Step 1: Extract Data
    log("Starting the Extraction Phase")
    json_file, json_path = extract_data(url)
    
    if json_file and json_path:
        log(f"Extraction completed successfully: {json_file}")
        
        # Step 2: Load Raw Data to S3
        log("Starting the Loading Phase for Raw Data")
        object_s3_path = f'raw/{json_file}'
        upload_success = upload_to_s3(json_path, bucket_name, object_s3_path)
        
        if upload_success:
            log("Raw data file uploaded successfully")
            
            # Step 3: Get data from S3 and load into DataFrame
            log(f"Retrieving JSON data {object_s3_path} from S3 and loading into DataFrame")
            json_data = get_json_data_from_s3(bucket_name, object_s3_path)
            
            if json_data is not None:
                log("Data loaded into DataFrame successfully")
                
                # Step 4: Extract and save country data
                log("Extracting and saving country data")
                cleaned_file, cleaned_file_path = extract_country_data(json_data)
                
                if cleaned_file and cleaned_file_path:
                    log(f"Country data saved to {cleaned_file_path}")
                    
                    # Step 5: Load Cleaned Data to S3
                    log("Starting the Loading Phase for Cleaned Data")
                    cleaned_object_s3_path = f'cleaned/{cleaned_file}'
                    cleaned_upload_success = upload_to_s3(cleaned_file_path, bucket_name, cleaned_object_s3_path)
                    
                    if cleaned_upload_success:
                        log("Cleaned data file uploaded successfully")
                    else:
                        log("Failed to upload cleaned data file to S3")
                else:
                    log("Failed to extract and save country data")
            else:
                log("Failed to load data into DataFrame")
        else:
            log("ETL process failed at the Load Phase for Raw Data")
    else:
        log("ETL process failed at the Extraction Phase")

if __name__ == "__main__":
    main()


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

