# Shared DAG utilities
import json
import os
import logging

import boto3 # type: ignore
from dotenv import load_dotenv # type: ignore  

# Use separate 

load_dotenv()

def get_logger(name):
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(name)

# MinIO: Configuration BUCKET:
SMART_CITY_BUCKET = "casablanca"
# s3a://
MINIO_WEATHER_RAW_PATH = "weather/raw/"
MINIO_WEATHER_PROCESSED_PATH = "weather/processed/"
LOCAL_WEATHER_RAW_PATH = os.getenv("WEATHER_LOCAL_PATH", "/opt/airflow/data/casablanca/weather/raw/")
LOCAL_WEATHER_PROCESSED_PATH = os.getenv("WEATHER_LOCAL_PATH", "/opt/airflow/data/casablanca/weather/processed/")

MINIO_TRAFFIC_RAW_PATH = "traffic/raw/"
MINIO_TRAFFIC_PROCESSED_PATH = "traffic/processed/"
LOCAL_TRAFFIC_RAW_PATH = os.getenv("TRAFFIC_LOCAL_PATH", "/opt/airflow/data/casablanca/traffic/raw/")
LOCAL_TRAFFIC_PROCESSED_PATH = os.getenv("TRAFFIC_LOCAL_PATH", "/opt/airflow/data/casablanca/traffic/processed/")

# DATABASE: Configuration For Weather and Traffic data:
DB_USER = "ibrahim"
DB_PASSWORD = "ibrahim"
DB_URL ="jdbc:postgresql://postgres:5432/smartcity"
DB_DRIVER = "org.postgresql.Driver"

# DATABASE: TABLES
DB_WEATHER_TABLE = "weather_data"
DB_TRAFFIC_TABLE = "traffic_data"
DB_AGGREGATED_TABLE = "aggregated_data"

def upload_from_local_to_minio(file_path: str, bucket_name: str, object_name: str) -> None:
    '''Uploads a file to MinIO using boto3.
    Args:
        file_path: Local path to the file to be uploaded.
        bucket_name: Name of the target bucket in MinIO.
        object_name: Desired object name in MinIO (including any prefix).
    
    Raises:
        Exception: If the upload fails.    
    '''
    logger = get_logger("Upload From Local To MinIO")

    s3_client = boto3.client(
        's3',
        endpoint_url= os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id= os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key= os.getenv("MINIO_SECRET_KEY"),
        region_name='us-east-1',
    )
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        logger.info("File Uploaded Sucessfully To MINIO")
    except Exception as e:
        logger.error(f"Error uploading file to MinIO: {e}", exc_info=True)


def append_json_line_localy(file_path: str, data: dict) -> None:
    '''
    Append a dictionary as a JSON line to a local file.
    
    Args:
        file_path: Local path to the file where the JSON line will be appended.
        data: Dictionary to be appended as a JSON line.
    '''
    os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Ensure the directory exists or create it if not
    with open(file_path, "a") as f:
        f.write(json.dumps(data) + "\n")   
        

def upload_to_minio_directly(data: dict, bucket_name: str, object_name: str) -> None:
    """
    Write JSON data directly to MinIO without local file.
    Appends to existing content if object exists.
    
    Args:
        data: Dictionary to serialize and upload.
        bucket_name: Target MinIO bucket.
        object_name: Destination path in bucket.
        logger: Logger instance.
    """
    logger = get_logger("UPLOAD DATA TO MINIO")

    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        region_name='us-east-1',
    )
    try:
        # Important transform before save: 
        # dict  →  JSON string  →  bytes  →  stored in MinIO
        json_bytes = json.dumps(data).encode('utf-8')
        
        s3_client.put_object(
            Bucket = bucket_name,               # Args Must be started with Maj Alphapetic
            Key = object_name,
            Body = json_bytes
        )
            
    except Exception as e:
        logger.error(f"Error uploading file to MinIO: {e}", exc_info=True)
        return


def append_json_line_minio(data , bucket_name: str, path) -> None:
    '''
    Function That get The JSON File From MinIO and Append to it The New data recieving from the API.
    Args:
        minio_json_path: 
    '''
    logger = get_logger("Append JSON Line in MinIO ")
    try:
        
        # Get the Data from Minio ... --> Append NewLine --> Upload it Back

        # ( json.dumps(data)+ "\n" )  :  dict → string: '{"temp": 15.19, ...}' and then add newline
        # .encode("utf-8")            :  string → bytes: b'{"temp": 15.19, ...}\n'
        newLine = (json.dumps(data)+"\n").encode("utf-8")



        logger.info(f"✅ Uploaded directly to MinIO: {path}")
    except Exception as e:
        logger.error(f"Failed to upload to MinIO: {e}", exc_info=True)
