import os
import json
import requests
import boto3
from datetime import datetime
from botocore.config import Config
from dotenv import load_dotenv

# Load local environment variables (for local execution)
load_dotenv()


def clean_env(key: str, default: str = "") -> str:
    """Sanitizes environment variables by removing quotes, whitespace, and newlines."""
    val = os.getenv(key, default)
    if not val:
        return ""
    return val.strip().strip('"').strip("'").replace("\r", "").replace("\n", "")


# Configuration variables
ENDPOINT_URL = clean_env("S3_ENDPOINT_URL", "https://s3.us-east-005.backblazeb2.com")
AWS_ACCESS_KEY_ID = clean_env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = clean_env("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = clean_env("S3_BUCKET_NAME", "smart-city")

# Ensure valid HTTP/HTTPS protocol prefix
if ENDPOINT_URL and not (ENDPOINT_URL.startswith("http://") or ENDPOINT_URL.startswith("https://")):
    ENDPOINT_URL = f"https://{ENDPOINT_URL}"

TRAFFIC_API = clean_env("TRAFFIC_API")
WEATHER_API = clean_env("WEATHER_API")


def get_s3_client():
    """Initializes a boto3 S3 client configured for Backblaze B2 compatibility."""
    boto_config = Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    )

    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        config=boto_config,
    )


def ingest_api_data(api_url: str, category: str):
    """Fetches data from an API endpoint and uploads the raw JSON payload to S3 storage."""
    if not api_url:
        print(f"⚠️ Warning: API URL for '{category}' is not set in environment. Skipping...")
        return

    print(f"📡 Fetching data from {category.upper()} API...")

    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        data = response.json()

        now = datetime.utcnow()
        file_timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
        ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S")

        # Inject ingestion metadata timestamp if payload is a dictionary
        if isinstance(data, dict):
            data["ingested_at"] = ingested_at

        s3_key = f"bronze/{category}/{file_timestamp}.json"

        s3_client = get_s3_client()
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data).encode("utf-8"),
            ContentType="application/json",
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