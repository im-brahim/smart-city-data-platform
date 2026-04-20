# Shared DAG utilities
import json
import os
import logging

import boto3 # type: ignore
from dotenv import load_dotenv # type: ignore  

load_dotenv()

def get_logger(name):
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(name)

# MinIO: Configuration & Paths:
MINIO_ENDPOINT = "http://minio:9000"
SMART_CITY_BUCKET = "casablanca"
MINIO_WEATHER_RAW_PATH = "weather/raw/"
MINIO_TRAFFIC_RAW_PATH = "traffic/raw/"


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
        endpoint_url= MINIO_ENDPOINT,
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
        endpoint_url= MINIO_ENDPOINT,
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
