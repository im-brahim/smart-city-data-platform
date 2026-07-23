import os
import json
import requests
import boto3
from datetime import datetime
from botocore.config import Config
from dotenv import load_dotenv

# Load local environment variables from .env file
load_dotenv()


def clean_env(key: str, default: str = "") -> str:
    """Sanitizes environment variables by removing quotes, whitespace, and newlines."""
    val = os.getenv(key, default)
    if not val:
        return ""
    return val.strip().strip('"').strip("'").replace("\r", "").replace("\n", "")


# Configuration
ENDPOINT_URL = clean_env("S3_ENDPOINT_URL", "https://s3.us-east-005.backblazeb2.com")
AWS_ACCESS_KEY_ID = clean_env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = clean_env("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = clean_env("S3_BUCKET_NAME", "smart-city")

TRAFFIC_API = clean_env("TRAFFIC_API")
WEATHER_API = clean_env("WEATHER_API")

# Ensure valid HTTP/HTTPS protocol prefix
if ENDPOINT_URL and not (ENDPOINT_URL.startswith("http://") or ENDPOINT_URL.startswith("https://")):
    ENDPOINT_URL = f"https://{ENDPOINT_URL}"

# Extract region name automatically (e.g. 'us-east-005')
region = "us-east-005"
if "s3." in ENDPOINT_URL and ".backblazeb2.com" in ENDPOINT_URL:
    try:
        region = ENDPOINT_URL.split("s3.")[1].split(".backblazeb2.com")[0]
    except Exception:
        pass


def get_s3_client():
    """Initializes a boto3 S3 client explicitly configured for Backblaze B2."""
    boto_config = Config(
        region_name=region,
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        request_checksum_calculation="when_required",   ###
        response_checksum_validation="when_required"   ###
    )
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        config=boto_config
    )


def ingest_api_data(api_url: str, category: str):
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

        if isinstance(data, dict):
            data["ingested_at"] = ingested_at

        s3_key = f"bronze/{category}/{file_timestamp}.json"

        s3_client = get_s3_client()
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(data).encode("utf-8"),
            ContentType="application/json"
        )
        print(f"✅ Successfully uploaded to s3://{BUCKET_NAME}/{s3_key}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch data from {category.upper()} API: {e}")
    except Exception as e:
        print(f"❌ Error uploading {category.upper()} data to Backblaze B2: {e}")


if __name__ == "__main__":
    print("🚀 Starting Local Smart City Cloud Ingestion Test...")
    print(f"🔧 Endpoint: {ENDPOINT_URL} (Region: {region})")
    print(f"📦 Target Bucket: {BUCKET_NAME}")
    ingest_api_data(TRAFFIC_API, "traffic")
    ingest_api_data(WEATHER_API, "weather")
    print("🏁 Local Pipeline Execution Complete.")





# import os
# import json
# import requests
# from datetime import datetime
# from dotenv import load_dotenv
# from b2sdk.v2 import InMemoryAccountInfo, B2Api

# load_dotenv()


# def clean_env(key: str, default: str = "") -> str:
#     val = os.getenv(key, default)
#     if not val:
#         return ""
#     return val.strip().strip('"').strip("'").replace("\r", "").replace("\n", "")


# B2_KEY_ID = clean_env("AWS_ACCESS_KEY_ID")
# B2_APPLICATION_KEY = clean_env("AWS_SECRET_ACCESS_KEY")
# BUCKET_NAME = clean_env("S3_BUCKET_NAME", "smart-city")

# TRAFFIC_API = clean_env("TRAFFIC_API")
# WEATHER_API = clean_env("WEATHER_API")


# def get_b2_bucket():
#     """Authorizes with B2's native API and returns the target bucket."""
#     info = InMemoryAccountInfo()
#     b2_api = B2Api(info)
#     b2_api.authorize_account("production", B2_KEY_ID, B2_APPLICATION_KEY)
#     return b2_api.get_bucket_by_name(BUCKET_NAME)


# def ingest_api_data(api_url: str, category: str, bucket):
#     if not api_url:
#         print(f"⚠️ Warning: API URL for '{category}' is not set. Skipping...")
#         return

#     print(f"📡 Fetching data from {category.upper()} API...")

#     try:
#         response = requests.get(api_url, timeout=15)
#         response.raise_for_status()
#         data = response.json()

#         now = datetime.utcnow()
#         file_timestamp = now.strftime("%Y-%m-%dT%H-%M-%S")
#         ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S")

#         if isinstance(data, dict):
#             data["ingested_at"] = ingested_at

#         b2_key = f"bronze/{category}/{file_timestamp}.json"

#         bucket.upload_bytes(
#             json.dumps(data).encode("utf-8"),
#             b2_key,
#             content_type="application/json",
#         )
#         print(f"✅ Successfully uploaded to b2://{BUCKET_NAME}/{b2_key}")

#     except requests.exceptions.RequestException as e:
#         print(f"❌ Failed to fetch data from {category.upper()} API: {e}")
#     except Exception as e:
#         print(f"❌ Error uploading {category.upper()} data to Backblaze B2: {e}")


# if __name__ == "__main__":
#     print("🚀 Starting Smart City Cloud Ingestion Pipeline...")
#     print(f"📦 Target Bucket: {BUCKET_NAME}")
#     bucket = get_b2_bucket()
#     ingest_api_data(TRAFFIC_API, "traffic", bucket)
#     ingest_api_data(WEATHER_API, "weather", bucket)
#     print("🏁 Ingestion Pipeline Complete.")