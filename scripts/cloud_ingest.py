import os
import json
import requests
import boto3  # type: ignore
from datetime import datetime
from botocore.config import Config  # type: ignore
from dotenv import load_dotenv  # type: ignore

# Load local environment variables from .env file
load_dotenv()


def clean_env(key: str, default: str = "") -> str:
    """Sanitizes environment variables by removing quotes, whitespace, and newlines."""
    val = os.getenv(key, default)

    if not val:
        return ""

    return val.strip().strip('"').strip("'").replace("\r", "").replace("\n", "")

# ============================================================
# Configuration
# ============================================================

#TODO: i need to change the secret in the VM to B2* instead aws* for the credentials
# before i upload this updated ingest script

ENDPOINT_URL = clean_env(
    "B2_ENDPOINT_URL",
    "https://s3.us-east-005.backblazeb2.com"
)

AWS_ACCESS_KEY_ID = clean_env("B2_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = clean_env("B2_SECRET_ACCESS_KEY")
BUCKET_NAME = clean_env("BUCKET_NAME", "smart-city")

TOMTOM_API_KEY = clean_env("TOMTOM_API_KEY")
WEATHER_API = clean_env("WEATHER_API")

# Ensure valid HTTP/HTTPS protocol prefix
if ENDPOINT_URL and not (
    ENDPOINT_URL.startswith("http://")
    or ENDPOINT_URL.startswith("https://")
):
    ENDPOINT_URL = f"https://{ENDPOINT_URL}"

# Extract region name automatically (e.g. 'us-east-005')
region = "us-east-005"

if "s3." in ENDPOINT_URL and ".backblazeb2.com" in ENDPOINT_URL:
    try:
        region = ENDPOINT_URL.split("s3.")[1].split(".backblazeb2.com")[0]
    except Exception:
        pass

# ============================================================
# Backblaze B2 / S3 Client
# ============================================================

def get_s3_client():
    """Initializes a boto3 S3 client explicitly configured for Backblaze B2."""

    boto_config = Config(
        region_name=region,
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required"
    )

    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        config=boto_config
    )

# ============================================================
# TomTom Traffic Ingestion
# ============================================================

def ingest_traffic_segment(segment_id: str, latitude: float, longitude: float, expected_openlr: str):

    if not TOMTOM_API_KEY:
        print("❌ TOMTOM_API_KEY is not configured.")
        return

    print("=" * 70)
    print(f"📡 Fetching traffic segment: {segment_id}")
    print(f"📍 Point: {latitude}, {longitude}")

    traffic_url = (
        "https://api.tomtom.com/traffic/services/4/"
        "flowSegmentData/absolute/10/json"
    )

    params = {
        "point": f"{latitude},{longitude}",
        "openLr": "true",
        "key": TOMTOM_API_KEY
    }

    try:

        response = requests.get(
            traffic_url,
            params=params,
            timeout=15
        )

        response.raise_for_status()
        data = response.json()

        # ----------------------------------------------------
        # Verify TomTom returned the expected segment
        # ----------------------------------------------------

        tomtom_openlr = (
            data.get("flowSegmentData", {}).get("openlr")
        )

        print(f"🔑 Expected OpenLR : {expected_openlr}")
        print(f"🔑 TomTom OpenLR   : {tomtom_openlr}")

        if tomtom_openlr != expected_openlr:
            print("❌ OPENLR MISMATCH — segment rejected.")
            return

        print("✅ OpenLR MATCH — correct segment.")

        # ----------------------------------------------------
        # Metadata
        # ----------------------------------------------------

        now = datetime.utcnow()
        ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S")

        # Add OUR ingestion metadata
        if isinstance(data, dict):
            data["ingested_at"] = ingested_at
            data["segment_id"] = segment_id
            data["query_latitude"] = latitude
            data["query_longitude"] = longitude
        
        # ----------------------------------------------------
        # Bronze path
        # ----------------------------------------------------

        s3_key = (f"bronze/traffic/{segment_id}/{ingested_at}.json")

        s3_client = get_s3_client()

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(
                data,
                ensure_ascii=False
            ).encode("utf-8"),
            ContentType="application/json"
        )

        print(f"✅ Segment {segment_id} successfully uploaded")
        print(f"📦 s3://{BUCKET_NAME}/{s3_key}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch traffic segment {segment_id}: {e}")

    except Exception as e:
        print(f"❌ Error uploading traffic segment {segment_id}: {e}")

# ============================================================
# Weather Ingestion
# ============================================================

def ingest_weather_data(api_url: str):

    if not api_url:
        print("⚠️ Warning: Weather API URL is not set in environment. Skipping...")
        return

    print("=" * 70)
    print("🌤️ Fetching data from WEATHER API...")

    try:

        response = requests.get(
            api_url,
            timeout=15
        )

        response.raise_for_status()
        data = response.json()

        now = datetime.utcnow()
        ingested_at = now.strftime("%Y-%m-%dT%H:%M:%S")

        if isinstance(data, dict):
            data["ingested_at"] = ingested_at

        s3_key = (f"bronze/weather/{ingested_at}.json")

        s3_client = get_s3_client()

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(
                data,
                ensure_ascii=False
            ).encode("utf-8"),
            ContentType="application/json"
        )

        print(
            f"✅ Weather successfully uploaded to s3://{BUCKET_NAME}/{s3_key}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to fetch WEATHER API: {e}")

    except Exception as e:
        print(f"❌ Error uploading WEATHER data: {e}")


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":

    # --------------------------------------------------------
    # Traffic
    # --------------------------------------------------------
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    SEGMENTS_FILE = os.path.join(BASE_DIR, "segments.json")

    with open (SEGMENTS_FILE, "r") as f:
        segments = json.load(f)

    for segment in segments:
        ingest_traffic_segment(
            segment["segment_id"],
            segment["latitude"],
            segment["longitude"],
            segment["openlr"]
        )

    # --------------------------------------------------------
    # Weather
    # --------------------------------------------------------
    ingest_weather_data(WEATHER_API)

    print("=" * 70)
    print("🏁 Local Pipeline Execution Complete.")