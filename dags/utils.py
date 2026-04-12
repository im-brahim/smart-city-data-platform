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

TRAFFIC_LOCAL_PATH = os.getenv("TRAFFIC_LOCAL_PATH", "/opt/airflow/data/traffic/casablanca.json")
WEATHER_LOCAL_PATH = os.getenv("WEATHER_LOCAL_PATH", "/opt/airflow/data/weather/casablanca.json")


def upload_to_minio(file_path: str, bucket_name: str, object_name: str, logger) -> None:
    '''Uploads a file to MinIO using boto3.
    Args:
        file_path: Local path to the file to be uploaded.
        bucket_name: Name of the target bucket in MinIO.
        object_name: Desired object name in MinIO (including any prefix).
    
    Raises:
        Exception: If the upload fails.    
    '''
    s3_client = boto3.client(
        's3',
        endpoint_url= os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id= os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key= os.getenv("MINIO_SECRET_KEY"),
        region_name='us-east-1',
    )
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
    except Exception as e:
        logger.error(f"Error uploading file to MinIO: {e}", exc_info=True)

def upload_to_minio_directly(data: json, bucket_name: str, object_name: str, logger) -> None:
    '''Uploads a file to MinIO using boto3.
    Args:
        file_path: Local path to the file to be uploaded.
        bucket_name: Name of the target bucket in MinIO.
        object_name: Desired object name in MinIO (including any prefix).
    
    Raises:
        Exception: If the upload fails.    
    '''
    s3_client = boto3.client(
        's3',
        endpoint_url= os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id= os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key= os.getenv("MINIO_SECRET_KEY"),
        region_name='us-east-1',
    )
    try:
        s3_client.upload_file(data, bucket_name, object_name)
    except Exception as e:
        logger.error(f"Error uploading file to MinIO: {e}", exc_info=True)

def append_json_line(file_path: str, data: dict) -> None:
    '''
    Append a dictionary as a JSON line to a local file.
    
    Args:
        file_path: Local path to the file where the JSON line will be appended.
        data: Dictionary to be appended as a JSON line.
    '''
    os.makedirs(os.path.dirname(file_path), exist_ok=True)  # Ensure the directory exists
    with open(file_path, "a") as f:
        f.write(json.dumps(data) + "\n")    