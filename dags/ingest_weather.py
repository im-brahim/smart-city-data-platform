import os
from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
import requests
from dotenv import load_dotenv # type: ignore

from utils import (
    upload_to_minio_directly, get_logger,
    SMART_CITY_BUCKET, MINIO_WEATHER_RAW_PATH
)

load_dotenv()
logger = get_logger("Ingest Weather Data")


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def fetch_and_save():
    
    """
    Save a single JSON record directly to MinIO as a new object.
    Each call creates a new file — no appending, no rewriting.
    
    Args:
        data: Dictionary to serialize and save.
        bucket_name: Target MinIO bucket.
        object_name: Unique destination path (include timestamp in name).
        logger: Logger instance.
    """
    
    url = os.getenv("WEATHER_API")

    try:
        res = requests.get(url)
        res.raise_for_status()  # Raise an exception for HTTP errors
        data = res.json()
        
        try:
            # Upload to Minio in sepat
            timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
            path = MINIO_WEATHER_RAW_PATH
            object_name = f"{path}{timestamp}.json"
            
            upload_to_minio_directly(data, SMART_CITY_BUCKET, object_name)      

            logger.info(f"Data Uploaded To MinIO Succesfully: {object_name}")
        except Exception as e:
            logger.error(f"---------Not upload to Minio: {e}", exc_info=True)
            return
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Not get Data From API: {e}", exc_info=True)
        
    
with DAG(
    dag_id="ingest_weather_casablanca",
    default_args=default_args,
    start_date=datetime(2025, 4, 15),
    schedule_interval='@hourly',         # every minute: '*/1 * * * *',       # every hour: '@hourly', 
    catchup=False,
    tags=["ingestion", "API_weather", "smart_city"]
) as dag:

    task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_and_save
    )
