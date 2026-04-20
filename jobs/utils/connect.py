import os
import logging

from pyspark.sql import SparkSession # type: ignore
from dotenv import load_dotenv  # type: ignore

from utils.config import(
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    PATH_STYLE_ACCESS,
    S3A_IMPL
)

load_dotenv()

def get_logger(name):
    """
    Intialization of logging 

    Args:
        the name of the task u want to appeared 
    Return:
        intialization logger 
    """
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(name)

def create_spark_session(app_name , use_minio = True): 
    """Create a Spark session with appropriate configurations"""
    # spark_master = os.getenv("SPARK_MASTER")
    builder = SparkSession.builder.appName(app_name).master("spark://master:7077")
    if use_minio:
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key",  MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", PATH_STYLE_ACCESS) \
            .config("spark.hadoop.fs.s3a.impl", S3A_IMPL) \
    
    return builder.getOrCreate()