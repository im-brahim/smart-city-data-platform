from utils.connect import create_spark_session, get_logger
from utils.data_io import save_in_db , read_parquet_from_minio
from pyspark.sql.utils import AnalysisException # type: ignore
from dotenv import load_dotenv #type:ignore
import os

load_dotenv()

def main():
    logger = get_logger("Save New to DB")
    spark = create_spark_session("SaveNewToPostgres", True)

    # Step 1: Read new_data from MinIO
    try:
        df = read_parquet_from_minio(spark, os.getenv("MINIO_PROCESSED_PATH"))
        logger.info(f"📥 Read {df.count()} new rows from new_data/")
    except AnalysisException as e:
        logger.error("❌ Failed to read processed ata: " + str(e))
        spark.stop()
        return

    # Step 2: Save to PostgreSQL if data exists
    if df.count() > 0:
        df.printSchema()
        table = os.getenv("DB_TABLE_ENR")
        save_in_db(df, table)
        logger.info(f"✅ {df.count()} New rows saved to PostgreSQL.")
    else:
        logger.info("🚫 No new rows to insert.")

    spark.stop()

if __name__ == "__main__":
    main()
