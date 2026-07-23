import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from b2sdk.v2 import InMemoryAccountInfo, B2Api

load_dotenv()


def clean_env(key: str, default: str = "") -> str:
    val = os.getenv(key, default)
    if not val:
        return ""
    return val.strip().strip('"').strip("'").replace("\r", "").replace("\n", "")


B2_KEY_ID = clean_env("AWS_ACCESS_KEY_ID")
B2_APPLICATION_KEY = clean_env("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = clean_env("S3_BUCKET_NAME", "smart-city")

TRAFFIC_API = clean_env("TRAFFIC_API")
WEATHER_API = clean_env("WEATHER_API")


def get_b2_bucket():
    """Authorizes with B2's native API and returns the target bucket."""
    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account("production", B2_KEY_ID, B2_APPLICATION_KEY)
    return b2_api.get_bucket_by_name(BUCKET_NAME)


def ingest_api_data(api_url: str, category: str, bucket):
    if not api_url:
        print(f"⚠️ Warning: API URL for '{category}' is not set. Skipping...")
        return

    print(f"📡 Fetching data from {category.upper()} API...")

    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        data = response.json()

        now = datetime.utcnow()
        file_timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
        ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S")

        if isinstance(data, dict):
            data["ingested_at"] = ingested_at

        b2_key = f"bronze/{category}/{file_timestamp}.json"

        bucket.upload_bytes(
            json.dumps(data).encode("utf-8"),
            b2_key,
            content_type="application/json",
        )
        print(f"✅ Successfully uploaded to b2://{BUCKET_NAME}/{b2_key}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data from {category.upper()} API: {e}")
    except Exception as e:
        print(f"❌ Error uploading {category.upper()} data to Backblaze B2: {e}")


if __name__ == "__main__":
    print("🚀 Starting Smart City Cloud Ingestion Pipeline...")
    print(f"📦 Target Bucket: {BUCKET_NAME}")
    bucket = get_b2_bucket()
    ingest_api_data(TRAFFIC_API, "traffic", bucket)
    ingest_api_data(WEATHER_API, "weather", bucket)
    print("🏁 Ingestion Pipeline Complete.")