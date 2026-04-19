# PEP8 style guide recommends organizing imports in the following order:
# 1. Standard library — alphabetical
import os
from datetime import datetime, timedelta
import json

# 2. Third party — alphabetical
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from dotenv import load_dotenv # type: ignore
import requests

# 3. Local imports
from utils import get_logger, upload_to_minio_directly, MINIO_TRAFFIC_RAW_PATH, SMART_CITY_BUCKET

# Load environment variables and initialize logger
load_dotenv()
logger = get_logger("Ingest Traffic Data")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Function to fetch traffic data and save it locally and to MinIO
def fetch_and_save():
    
    # Make the API request to fetch traffic data
    url = os.getenv("TRAFFIC_API")
    try:

        res = requests.get(url)
        res.raise_for_status() 
        data = res.json()

        # Add timestamp to the data

        now = datetime.utcnow()
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S") # --> for the filename 
        ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S")
        
        data['ingested_at'] = ingested_at

        key = f"{MINIO_TRAFFIC_RAW_PATH}{timestamp}.json"
        
        try:
            # Upload to MinIO
            upload_to_minio_directly(data, SMART_CITY_BUCKET, key)
            logger.info("Sucessfully Traffic Response Saved To Minio")
            
        except Exception as e: # Catch any exception during upload 
            logger.error(f"----TRAFFIC Respons Not Uploaded to Minio: {e}", exc_info=True)
            return
    # Handle any request exceptions (e.g., network issues, invalid responses)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error: {e}", exc_info=True) 

# Define the DAG and its schedule
with DAG(
    dag_id="ingest_traffic_casablanca",
    default_args=default_args,
    start_date=datetime(2025, 4, 15),
    schedule_interval='@hourly',         # every minute: '*/1 * * * *',       # every hour: '@hourly', 
    catchup=False,
    tags=["ingestion", "API_traffic", "smart_city"]
) as dag:

    # Define the task to fetch and save traffic data
    task = PythonOperator(
        task_id="fetch_traffic_data",
        python_callable=fetch_and_save
    )
