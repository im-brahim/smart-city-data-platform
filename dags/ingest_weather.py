import os
from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
import requests
from dotenv import load_dotenv # type: ignore

from utils import (
    upload_to_minio, upload_to_minio_directly, append_json_line, get_logger, WEATHER_LOCAL_PATH
)

load_dotenv()
logger = get_logger("Ingest Weather Data")


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def fetch_and_save():
    url = os.getenv("WEATHER_API")
    try:
        res = requests.get(url)
        res.raise_for_status()  # Raise an exception for HTTP errors
        data = res.json()

        # Add timestamp
        # data["timestamp"] = datetime.utcnow().isoformat() # tiemstamp is existe in api repons

        # Append as a new line (JSONL style)
        file_path = WEATHER_LOCAL_PATH
        append_json_line(file_path, data)

        try:
            # Upload to MinIO
            # upload_to_minio(file_path, "weather", "casablanca.json", logger)
            upload_to_minio_directly(data, "weather", "casablanca.json", logger)
        except Exception as e:
            logger.error(f"---------Not upload to Minio: {e}", exc_info=True)

    except requests.exceptions.RequestException as e:
        logger.error(f"Error: {e}", exc_info=True)

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
