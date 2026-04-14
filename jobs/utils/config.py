# config.py — safe to commit to GitHub
import os
from dotenv import load_dotenv  # type: ignore

load_dotenv()

# SPARK: 
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://master:7077")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "Smart City ETL Pipeline")

# MinIO: Connection parameters for Spark
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
PATH_STYLE_ACCESS = "true"
S3A_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem"

# MinIO: Credientiel
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") 

# MinIO: Configuration BUCKET:
MINIO_JSON_BUCKET = "casablanca"
MINIO_PROCESSED_BUCKET = "casablanca_processed"

# MinIO: Configuration PATH For Weather: 
MINIO_WEATHER_JSON_PATH = "s3a://casablanca/weather/raw/casablanca.json"
MINIO_WEATHER_PROCESSED_PATH = "s3a://casablanca/weather/processed/"

# MinIO: Configuration PATH For Traffic:
MINIO_TRAFFIC_JSON_PATH = "s3a://casablanca/traffic/raw/casablanca.json"
MINIO_TRAFFIC_PROCESSED_PATH = "s3a://casablanca/traffic/processed/"

# DATABASE: CONFIGURATION:
DB_URL = os.getenv("DB_URL", "jdbc:postgresql://postgres:5432/airflow")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")  
DB_DRIVER = "org.postgresql.Driver"
# DATABASE: TABLES
DB_WEATHER_TABLE = "weather_data"
DB_TRAFFIC_TABLE = "traffic_data"
DB_AGGREGATED_TABLE = "aggregated_data"

# Local (containers using mount volume) paths for Saving traffic and weather data
WEATHER_LOCAL_PATH = "/opt/airflow/data/weather/casablanca.json"
TRAFFIC_LOCAL_PATH = "/opt/airflow/data/traffic/casablanca.json"