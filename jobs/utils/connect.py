import os
import logging

from pyspark.sql import SparkSession # type: ignore
from dotenv import load_dotenv  # type: ignore

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
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT")) \
            .config("spark.hadoop.fs.s3a.access.key",  os.getenv("MINIO_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
            .config("spark.hadoop.fs.s3a.path.style.access", os.getenv("PATH_STYLE_ACCESS")) \
            .config("spark.hadoop.fs.s3a.impl", os.getenv("S3A_IMPL")) \
    
    return builder.getOrCreate()