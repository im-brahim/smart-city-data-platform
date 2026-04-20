"""
Process raw weather and traffic data from MinIO.
Applies Bronze → Silver → Gold transformation:
  Bronze: raw JSON from MinIO
  Silver: flattened, validated, enriched DataFrame  
  Gold:   saved to PostgreSQL
"""

from dotenv import load_dotenv
from pyspark.sql.functions import col, round as spark_round
from pyspark.errors import AnalysisException

from utils.connect import create_spark_session,  get_logger
from utils.data_io import (
    read_json_from_minio,
    save_parquet_to_minio
)
from utils.config import (
    SMART_CITY_BUCKET,
    MINIO_TRAFFIC_RAW_PATH,
    MINIO_WEATHER_RAW_PATH,
    MINIO_WEATHER_PROCESSED_PATH,
    MINIO_TRAFFIC_PROCESSED_PATH
)

from validate_data import run_validation

# WEATHER PROCESSING
def flatten_weather(df):
    """
    Flatten nested weather JSON into a clean DataFrame.
    
    Args:
        df: Raw Spark DataFrame from MinIO weather JSON.
    
    Returns:
        Flattened DataFrame with selected columns.
    """
    return df.select(
        col("ingested_at").cast("timestamp").alias("ingested_at"),
        col("dt").cast("timestamp").alias("timestamp"),
        col("main.temp").alias("temp"),
        col("main.humidity").alias("humidity"),
        col("weather").getItem(0).getField("main").alias("weather_condition"),
        col("clouds.all").alias("cloud_coverage"),
        col("name").alias("city"),
        col("coord.lat").alias("latitude"),
        col("coord.lon").alias("longitude")
    )

# TRAFFIC PROCESSING
def flatten_traffic(df):
    """
    Flatten nested traffic JSON into a clean DataFrame.
    
    Args:
        df: Raw Spark DataFrame from MinIO traffic JSON.
    
    Returns:
        Flattened DataFrame with selected columns
        including derived congestion_ratio.
    """
    return df.select(
        col("ingested_at").cast("timestamp").alias("ingested_at"),
        col("flowSegmentData.currentSpeed").alias("currentSpeed"),
        col("flowSegmentData.freeFlowSpeed").alias("freeFlowSpeed"),
        col("flowSegmentData.currentTravelTime").alias("currentTravelTime"),
        col("flowSegmentData.freeFlowTravelTime").alias("freeFlowTravelTime"),
        col("flowSegmentData.confidence").alias("confidence"),
        col("flowSegmentData.coordinates.coordinate").getItem(0).getField("latitude").alias("latitude"),
        col("flowSegmentData.coordinates.coordinate").getItem(0).getField("longitude").alias("longitude"),
        spark_round(col("flowSegmentData.currentSpeed") / col("flowSegmentData.freeFlowSpeed"), 2).alias("congestion_ratio")

    )
    

def main():

    load_dotenv()
    logger = get_logger("Process Weather & Traffic")
    spark = create_spark_session("SmartCity ETL", use_minio=True)
    
    # ---------- WEATHER -----------------------
    try:
        s3_weather_path =  f"s3a://{SMART_CITY_BUCKET}/{MINIO_WEATHER_RAW_PATH}"
        raw_weather = read_json_from_minio(spark, s3_weather_path)
        weather_df = flatten_weather(raw_weather)
        logger.info(f"✅ Weather flattened : {weather_df.count()} rows")
    except AnalysisException as e:
        logger.error("Failed To Read Weather Data From MinIO", exc_info=True)
        spark.stop()
        return

    # Validate weather
    if not run_validation(weather_df, "weather"):
        logger.warning(" ⚠️ Weather data failed validation — skipping save")
    else:

        save_parquet_to_minio(weather_df, f"s3a://{SMART_CITY_BUCKET}/{MINIO_WEATHER_PROCESSED_PATH}") # os.getenv("MINIO_WEATHER_PROCESSED_PATH"))
        logger.info("✅ Weather saved to MinIO Silver layer")


    # ---------- Traffic -----------------------
    try:
        s3_traffic_path = f"s3a://{SMART_CITY_BUCKET}/{MINIO_TRAFFIC_RAW_PATH}"
        raw_traffic = read_json_from_minio(spark, s3_traffic_path)
        traffic_df = flatten_traffic(raw_traffic)
        logger.info(f"✅ Traffic flattened: {traffic_df.count()} rows")
    except AnalysisException as e:
        logger.error(f"Failed To Read Traffic Data From MinIO, {e}")
        spark.stop()
        return
    

    if not run_validation(traffic_df, 'traffic'):
        logger.warning("⚠️ Traffic data failed validation — skipping save")
    else:

        save_parquet_to_minio(traffic_df, f"s3a://{SMART_CITY_BUCKET}/{MINIO_TRAFFIC_PROCESSED_PATH}")  # os.getenv("MINIO_TRAFFIC_PROCESSED_PATH"))
        logger.info("✅ Traffic saved to MinIO Silver layer")


    spark.stop()

if __name__ == "__main__":
    main()
