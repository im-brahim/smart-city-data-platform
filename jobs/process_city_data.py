"""
Process raw weather and traffic data from MinIO.
Applies Bronze → Silver → Gold transformation:
  Bronze: JSON files from BackBlaze B2
  Silver: flattened, validated, enriched DataFrame  ---> Load it as parquet to B2
  Gold:   Parquet Saved to PostgreSQL
"""
from dotenv import load_dotenv      #type: ignore
from pyspark.sql.functions import col, round as spark_round, hour, dayofweek    #type: ignore

from utils.schemas import WEATHER_SCHEMA, TRAFFIC_SCHEMA
from utils.connect import create_spark_session,  get_logger
from utils.data_io import get_track, update_track, get_client, save_parquet_to_Cloud
from validate_data import run_validation
from utils.config import (
    B2_BUCKET_NAME, BRONZE_TRAFFIC_PREFIX, BRONZE_WEATHER_PREFIX,
      SILVER_TRAFFIC_PREFIX, SILVER_WEATHER_PREFIX
)
# from validate_data import run_validation

# WEATHER PROCESSING
def flatten_weather(df):
    """
    Flatten nested weather JSON into a clean, ML-ready DataFrame.

    Args:
        df: Raw Spark DataFrame from Bronze weather JSON.

    Returns:
        Flattened DataFrame with selected columns and derived time features.
    """
    return df.select(
        col("ingested_at").cast("timestamp").alias("ingested_at"),
        col("dt").cast("timestamp").alias("timestamp"),

        # Core weather metrics
        col("main.temp").alias("temp"),
        col("main.humidity").alias("humidity"),
        col("main.pressure").alias("pressure"),
        col("visibility").alias("visibility"),

        # Wind
        col("wind.speed").alias("wind_speed"),
        col("wind.deg").alias("wind_deg"),

        # Sky condition
        col("weather").getItem(0).getField("main").alias("weather_condition"),
        col("weather").getItem(0).getField("description").alias("weather_description"),
        col("clouds.all").alias("cloud_coverage"),

        # Location (fixed single city — cheap to keep as-is, no redundancy concern)
        col("name").alias("city"),
        col("coord.lat").alias("latitude"),
        col("coord.lon").alias("longitude"),

        # Time features (for periodicity + joining against traffic data)
        hour(col("ingested_at")).alias("hour_of_day"),
        dayofweek(col("ingested_at")).alias("day_of_week"),
    )

# TRAFFIC PROCESSING
def flatten_traffic(df):
    """
    Flatten nested traffic JSON into a clean, ML-ready DataFrame.

    Road geometry (full coordinate list) is NOT stored per-row since
    this pipeline tracks a single fixed road segment — the shape is
    static across all rows. Only one reference point is kept for
    mapping/joins; the full route is preserved separately as a
    static .geojson reference artifact.

    Args:
        df: Raw Spark DataFrame from Bronze traffic JSON.

    Returns:
        Flattened DataFrame with derived congestion and time features.
    """
    return df.select(
        col("ingested_at").cast("timestamp").alias("ingested_at"),

        # Road classification & status
        col("flowSegmentData.frc").alias("road_class"),
        col("flowSegmentData.roadClosure").alias("is_closed"),

        # Core traffic metrics
        col("flowSegmentData.currentSpeed").alias("currentSpeed"),
        col("flowSegmentData.freeFlowSpeed").alias("freeFlowSpeed"),
        col("flowSegmentData.currentTravelTime").alias("currentTravelTime"),
        col("flowSegmentData.freeFlowTravelTime").alias("freeFlowTravelTime"),
        col("flowSegmentData.confidence").alias("confidence"),

        # Derived congestion features
        spark_round(
            col("flowSegmentData.currentSpeed") / col("flowSegmentData.freeFlowSpeed"), 2
        ).alias("congestion_ratio"),
        (col("flowSegmentData.freeFlowSpeed") - col("flowSegmentData.currentSpeed")).alias("speed_delta"),
        (col("flowSegmentData.currentTravelTime") - col("flowSegmentData.freeFlowTravelTime")).alias("delay_seconds"),

        # Time features (for periodicity — hour/weekday patterns)
        hour(col("ingested_at")).alias("hour_of_day"),
        dayofweek(col("ingested_at")).alias("day_of_week"),

        # Single reference point (segment geometry lives in a static .geojson, not here)
        col("flowSegmentData.coordinates.coordinate").getItem(0).getField("latitude").alias("ref_latitude"),
        col("flowSegmentData.coordinates.coordinate").getItem(0).getField("longitude").alias("ref_longitude"),
    )
    

def process_data(spark, s3_client, prefix, logger, source, flattened_function, silver_path, schema):
    logger = get_logger(f"--------------- Process {source} Data-------------")
    load_dotenv()

    base_path = f"s3a://{B2_BUCKET_NAME}/"

    # Get the watermark FIRST, before listing anything
    track = get_track(source)
    list_kwargs = {"Bucket": B2_BUCKET_NAME, "Prefix": prefix}

    if track is not None and track[1]:  # type: ignore
        track_last_processed = track[1]  # type: ignore
        track_dt_str = track_last_processed.strftime("%Y-%m-%dT%H-%M-%S")
        start_after_key = f"{prefix}{track_dt_str}.json"
        list_kwargs["StartAfter"] = start_after_key
  
    try:
        response = s3_client.list_objects_v2(**list_kwargs)
        files = response.get("Contents", [])
        not_processed_path_list = [f["Key"] for f in files]
    except Exception as e:
        logger.error(f"Can't list files for {prefix}: {e}", exc_info=True)
        return

    if not not_processed_path_list:
        logger.info(f"No new files for {source} — skipping.")
        return

    full_path = [base_path + fname for fname in not_processed_path_list]

    try:
        df = spark.read.schema(schema).format("json").load(full_path).cache()
        flattened_df = flattened_function(df).cache()
        count = flattened_df.count()
        
        if not run_validation(flattened_df, source):
            logger.warning(f"⚠️ {source} data failed validation — skipping Silver save and watermark update")
            return None

        latest_ts = flattened_df.agg({"ingested_at": "max"}).collect()[0][0]
        batch_id = latest_ts.strftime("%Y-%m-%dT%H-%M-%S")
        year_month = latest_ts.strftime("%Y-%m")
        silver_path_for_this_batch = f"{silver_path}year_month={year_month}/{batch_id}/"

        save_parquet_to_Cloud(flattened_df, silver_path_for_this_batch)
        logger.info(f"--- {count} of {source} Data Processed, Validated and saved Successfully")

        try:
            update_track(source, latest_ts)
            logger.info(f"Successfully Update the Pipeline Track Table")
            return flattened_df
        except Exception as e:
            logger.warning(f"The Track Table not Update, {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Failed to Process {source}: {e}", exc_info=True)
        return



def main():

    load_dotenv()
    logger = get_logger("Process Weather & Traffic")
    spark = create_spark_session("SmartCity ETL")
    s3_client = get_client()

    silver_traffic_path = f"s3a://{B2_BUCKET_NAME}/{SILVER_TRAFFIC_PREFIX}"
    process_data(spark, s3_client, BRONZE_TRAFFIC_PREFIX, logger, "traffic", flattened_function=flatten_traffic, silver_path=silver_traffic_path, schema=TRAFFIC_SCHEMA)
  
    silver_weather_path = f"s3a://{B2_BUCKET_NAME}/{SILVER_WEATHER_PREFIX}"
    process_data(spark, s3_client, BRONZE_WEATHER_PREFIX, logger, "weather", flattened_function=flatten_weather, silver_path=silver_weather_path, schema=WEATHER_SCHEMA)

    spark.stop()

if __name__ == "__main__":
    main()

