import os

from dotenv import load_dotenv  # type: ignore

load_dotenv()

# SPARK: 
SPARK_MASTER = "spark://master:7077"
SPARK_APP_NAME = "Smart City ETL Pipeline"

# MinIO: Connection parameters for Spark
MINIO_ENDPOINT = "http://minio:9000"
PATH_STYLE_ACCESS = "true"
S3A_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem"

# MinIO: Credientiel
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY") 

# MinIO: Configuration BUCKET:
SMART_CITY_BUCKET = "casablanca"

# MinIO: Configuration PATH For Weather: 
MINIO_WEATHER_RAW_PATH = "weather/raw/"
MINIO_WEATHER_PROCESSED_PATH = "weather/processed/"

# MinIO: Configuration PATH For Traffic:
MINIO_TRAFFIC_RAW_PATH = "traffic/raw/"
MINIO_TRAFFIC_PROCESSED_PATH = "traffic/processed/"

# DATABASE: CONFIGURATION:
DB_URL = "jdbc:postgresql://postgres:5432/smartcity"
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")  
DB_DRIVER = "org.postgresql.Driver"

# DATABASE: TABLES
DB_WEATHER_TABLE = "weather"
DB_TRAFFIC_TABLE = "traffic"
DB_AGGREGATED_TABLE = "aggregated_data"

# Local (containers using mount volume) paths for Saving traffic and weather data
# WEATHER_LOCAL_PATH = "/opt/airflow/data/weather/casablanca.json"
# TRAFFIC_LOCAL_PATH = "/opt/airflow/data/traffic/casablanca.json"
