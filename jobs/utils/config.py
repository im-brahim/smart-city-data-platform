# config.py — safe to commit to GitHub
import os
from dotenv import load_dotenv  # type: ignore

load_dotenv()

# Spark
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://master:7077")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "Crypto ETL Pipeline")

# MinIO
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")  # No default — must be in .env

# Database
DB_URL = os.getenv("DB_URL", "jdbc:postgresql://postgres:5432/airflow")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASSWORD = os.getenv("DB_PASSWORD")  # No default — must be in .env
DB_DRIVER = "org.postgresql.Driver"     # Not a secret, hardcode is fine