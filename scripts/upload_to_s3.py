import boto3
from botocore.exceptions import ClientError
import configparser
from extract import log
import os


# constants
base_dir = os.getcwd() + '/'
config_dir = os.path.join(base_dir, 'credentials/config.ini')
config = configparser.ConfigParser()
config.read(config_dir)

# load AWS credentials from a configuration file
ACCESS_KEY_ID = config['AWS']['ACCESS_KEY_ID' ]
SECRET_ACCESS_KEY = config['AWS']['SECRET_ACCESS_KEY' ]


def upload_to_s3(json_path, bucket_name, object_name=None):
    if not os.path.exists(json_path):
        log(f'Error: File {json_path} does not exist.')
        return False
    
    log(f'Starting the Load Phase for {json_path}')
    
    if object_name is None:
        object_name = os.path.basename(json_path)
    
    # Create an S3 client
    try:
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY_ID,
            aws_secret_access_key=SECRET_ACCESS_KEY
        )
        s3_client = session.client('s3')
        
        s3_client.upload_file(json_path, bucket_name, object_name)
        log(f'Success: File {json_path} uploaded to {bucket_name} as {object_name}')
        return True

    except ClientError as e:
        log(f'Error uploading file to S3: {e}', level='ERROR')
        return False