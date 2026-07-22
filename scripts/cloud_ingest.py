import os
import requests
import boto3
from datetime import datetime 
from dotenv import load_dotenv
import json 

# Load credentials from .env file
load_dotenv()

# Configuration
ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "https://s3.us-east-005.backblazeb2.com")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "smart-city")

TRAFFIC_API = os.getenv("TRAFFIC_API")
WEATHER_API = os.getenv("WEATHER_API")


def get_s3_client():
    """Initialize and return a boto3 S3 client for Backblaze B2."""
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-east-005"
    )


def ingest_api_data(api_url: str, category: str):
    """
    Fetches data from an API and uploads raw JSON to Backblaze B2 under bronze/<category>/
    """
    if not api_url:
        print(f"⚠️ Warning: API URL for '{category}' is not set in environment. Skipping...")
        return

    print(f"📡 Fetching data from {category.upper()} API...")

    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        data = response.json()

        # Generate timestamps
        now = datetime.utcnow()
        file_timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
        ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S")

        # Inject ingestion metadata into the raw payload
        if isinstance(data, dict):
            data["ingested_at"] = ingested_at

        # Target S3 key in Bronze layer: bronze/traffic/2026-07-22T17-00-00.json
        s3_key = f"bronze/{category}/{file_timestamp}.json"

        # Upload directly to Backblaze B2
        s3_client = get_s3_client()
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        print(f"✅ Successfully uploaded to s3://{BUCKET_NAME}/{s3_key}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data from {category.upper()} API: {e}")
    except Exception as e:
        print(f"❌ Error uploading {category.upper()} data to Backblaze B2: {e}")


if __name__ == "__main__":
    print("🚀 Starting Smart City Cloud Ingestion Pipeline...")
    ingest_api_data(TRAFFIC_API, "traffic")
    ingest_api_data(WEATHER_API, "weather")
    print("🏁 Ingestion Pipeline Complete.")