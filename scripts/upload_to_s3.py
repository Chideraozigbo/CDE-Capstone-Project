import boto3
from botocore.exceptions import ClientError
import configparser
from scripts.extract import log
import os


# constants
base_dir = '/Users/user/Documents/CDE-Capstone-Project/'
config_dir = os.path.join(base_dir, 'credentials/config.ini')
config = configparser.ConfigParser()
config.read(config_dir)

# load AWS credentials from a configuration file
ACCESS_KEY_ID = config['AWS']['ACCESS_KEY_ID' ]
SECRET_ACCESS_KEY = config['AWS']['SECRET_ACCESS_KEY' ]


def upload_to_s3(file_path, bucket_name, object_name=None):
    if not os.path.exists(file_path):
        log(f'Error: File {file_path} does not exist.')
        return False
    
    log(f'Starting the Load Phase for {file_path}')
    
    if object_name is None:
        object_name = os.path.basename(file_path)
    
    # Create an S3 client
    try:
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY
        )
        s3_client = session.client('s3')
        
        s3_client.upload_file(file_path, bucket_name, object_name)
        log(f'Success: File {file_path} uploaded to {bucket_name} as {object_name}')
        return True

    except ClientError as e:
        log(f'Error uploading file to S3: {e}', level='ERROR')
        return False