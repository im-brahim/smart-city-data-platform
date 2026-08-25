import logging

from pyspark.sql import SparkSession # type: ignore

from utils.config import(
    B2_ENDPOINT_URL,
    B2_ACCESS_KEY_ID,
    B2_SECRET_ACCESS_KEY,
)


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

def create_spark_session(app_name): 
    """Create a Spark session with appropriate configurations"""
    builder = SparkSession.builder.appName(app_name) \
        .master("spark://master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", B2_ENDPOINT_URL) \
        .config("spark.hadoop.fs.s3a.access.key",  B2_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", B2_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep") \
        
    return builder.getOrCreate()