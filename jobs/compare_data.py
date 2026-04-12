from utils.connect import create_spark_session, get_logger
from utils.data_io import read_parquet_from_minio, read_from_db, save_parquet_to_minio
from pyspark.sql.functions import col # type: ignore
from pyspark.sql.utils import AnalysisException # type: ignore
from dotenv import load_dotenv #type:ignore
import os

load_dotenv()

def main():
    logger = get_logger("Compare New Data")
    spark = create_spark_session("CompareNewData")

    # Step 1: Read processed Parquet from MinIO
    df_parquet = read_parquet_from_minio(spark, os.getenv("MINIO_PARQUET_PATH"))
    logger.info(f"📦 Read {df_parquet.count()} rows from Parquet")

    # Step 2: Try to read DB table and get max timestamp
    try:
        df_db = read_from_db(spark)
        max_ts = df_db.agg({"timestamp": "max"}).collect()[0][0]
        logger.info(f"📌 Max timestamp in DB: {max_ts}")

        # Step 3: Filter new rows
        df_new = df_parquet.filter(col("timestamp") > max_ts)
    except AnalysisException as e:
        logger.info(f"⚠️ No existing table. Using all rows. {e}")
        df_new = df_parquet

    # Step 4: Save new rows to processed path
    if df_new.count() > 0:
        path =  os.getenv("MINIO_PROCESSED_PATH")
        # print(f"----------------{path}")
        save_parquet_to_minio(df_new, path)
        logger.info(f"✅ Saved {df_new.count()} new rows to processed/ in MINIO")
    else:
        logger.info("🚫 No new data to save.")

    spark.stop()

if __name__ == "__main__":
    main()