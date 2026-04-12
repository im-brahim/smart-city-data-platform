# PEP8 style guide recommends organizing imports in the following order:
# 1. Standard library — alphabetical
import os
from datetime import datetime, timedelta

# 2. Third party — alphabetical
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from dotenv import load_dotenv # type: ignore
import requests

# 3. Local imports
from utils import upload_to_minio, append_json_line, get_logger, TRAFFIC_LOCAL_PATH

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
    url = os.getenv("TRAFFIC_API")
    try:
        # Make the API request to fetch traffic data
        res = requests.get(url)
        '''
        Raise an exception for HTTP errors (e.g., 4xx or 5xx responses)
        This ensures that we only proceed if the request was successful
        If the response status code indicates an error, an HTTPError will be raised
        and we can catch it in the except block below to log the error details.
        ''' 
        res.raise_for_status() 
        # Parse the response as JSON
        data = res.json()


        # Add timestamp to the data
        data["timestamp"] = datetime.utcnow().isoformat()

        # Append as a new line (JSONL style)
        local_file_path = TRAFFIC_LOCAL_PATH
        append_json_line(local_file_path, data)
        

        # s3_json_path = os.getenv("MINIO_TRAFFIC_JSON_PATH")
        try:
            # Upload to MinIO
            upload_to_minio(local_file_path, "traffic", "casablanca.json", logger)
        except Exception as e: # Catch any exception during upload 
            logger.error(f"---------Not upload to Minio: {e}", exc_info=True)
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
