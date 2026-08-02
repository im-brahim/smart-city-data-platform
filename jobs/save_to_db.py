from dotenv import load_dotenv #type:ignore

from utils.data_io import (
    read_parquet_from_Cloud, 
    save_only_new_rows, get_client, get_track, update_track
)
from utils.config import (
    SILVER_TRAFFIC_PREFIX,
    SILVER_WEATHER_PREFIX,
    B2_BUCKET_NAME,
    DB_WEATHER_TABLE,
    DB_TRAFFIC_TABLE,
)
from utils.connect import create_spark_session,  get_logger
load_dotenv()

def read_and_save(spark, logger, source, s3_client, bucket, silver_path, table):

    track = get_track(source)
    list_args = {"Bucket" : bucket, "Prefix" : silver_path}

    if track is not None and track[1]:
        last_batch_id = track[1]
        year_month = last_batch_id.strftime("%Y-%m")
        last_batch_id_str = last_batch_id.strftime("%Y-%m-%dT%H-%M-%S")
        list_args["StartAfter"] = f"{silver_path}year_month={year_month}/{last_batch_id_str}/"

    try:
        response = s3_client.list_objects_v2(**list_args)
        objects = response.get("Contents", [])
    except Exception as e:
        logger.error(f"Can't list Silver files for {silver_path}: {e}", exc_info=True)
        return

    paths = [ f"s3a://{bucket}/{obj['Key']}"
        for obj in objects
        if not obj["Key"].endswith("SUCCESS") and "_temporary" not in obj["Key"]
    ]

    if not paths:
        logger.info(f"No files found for {silver_path} — skipping.")
        return

    try:
        df = read_parquet_from_Cloud(spark, *paths).cache()
        count = df.count()
        logger.info(f"---------- {count} PROCESSED {silver_path} DATA FROM CLOUD SUCESSFULLY READED --------- ")

        try:
            save_only_new_rows(spark, df, table)
            logger.info(f"-------------- {count} rows Saved Sucessfully to Table {table}")
        except Exception as e:
            logger.error(f"Failed to save Data to Database : {e}", exc_info=True)
            return

        try:
            last_ts = df.agg({"ingested_at": "max" }).collect()[0][0]
            update_track(source, last_ts)
            logger.info(f"Successfully updated Silver watermark for {source} -> {last_ts}")
        except Exception as e:
            logger.warning(f"Silver watermark not updated for {source}: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"❌ --- FAILED TO LOAD Processed {silver_path} DATA To DB Table {table} : {e}", exc_info=True)

def main():
    
    logger = get_logger("Save SMART CITY DATA TO DATABASE")
    spark = create_spark_session("Save New Data To Postgres Tables")
    s3_client = get_client()

    # -------------------  Read TRAFFIC DATA from CLOUD ---> SAVE TO DATABASE --------------------------------  
    read_and_save(spark, logger, "traffic_silver", s3_client, B2_BUCKET_NAME, SILVER_TRAFFIC_PREFIX, DB_TRAFFIC_TABLE )
          
    # -------------------  Read WEATHER DATA from CLOUD ---> SAVE TO DATABASE --------------------------------
    read_and_save(spark, logger, "weather_silver", s3_client, B2_BUCKET_NAME, SILVER_WEATHER_PREFIX, DB_WEATHER_TABLE)

    spark.stop()

if __name__ == "__main__":
    main()
