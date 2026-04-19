from pyspark.sql.utils import AnalysisException # type: ignore
from dotenv import load_dotenv #type:ignore

from utils.data_io import read_parquet_from_minio, save_only_new_rows
from utils.connect import create_spark_session, get_logger
from utils.config import (
    MINIO_TRAFFIC_PROCESSED_PATH,
    MINIO_WEATHER_PROCESSED_PATH,
    SMART_CITY_BUCKET,
    DB_WEATHER_TABLE,
    DB_TRAFFIC_TABLE,
    DB_AGGREGATED_TABLE
)

load_dotenv()

def main():
    
    logger = get_logger("Save SMART CITY DATA TO DATABASE")
    spark = create_spark_session("SaveNewToPostgres", True)

    # -------------------  Read TRAFFIC DATA from MinIO --------------------------------  
    
    # SAVE the Reading DATA to DATABASE TABLE 
    try:
        
        df = read_parquet_from_minio(spark, f"s3a://{SMART_CITY_BUCKET}/{MINIO_TRAFFIC_PROCESSED_PATH}")
        traffic_count = df.count()
        logger.info(f"---------- PROCESSED TRAFFIC DATA FROM MINIO SUCESSFULLY READED --------- ")
        
        save_only_new_rows(spark, df, DB_TRAFFIC_TABLE)
        logger.info(f"--------------The {traffic_count} TRAFFIC Data Saving Sucessfully to Table {DB_TRAFFIC_TABLE}")
    
    except AnalysisException as e:
        logger.error(f"❌----------------FAILED TO LOAD Processed TRAFFIC DATA To DB Table {DB_TRAFFIC_TABLE}, {e}")
        spark.stop()
        return  
    
    
    # -------------------  Read WEATHER DATA from MinIO --------------------------------

    
    try:
        
        df = read_parquet_from_minio(spark, f"s3a://{SMART_CITY_BUCKET}/{MINIO_WEATHER_PROCESSED_PATH}")
        weather_count = df.count()  

        save_only_new_rows(spark, df, DB_WEATHER_TABLE)
        logger.info(f"-------------- THE {weather_count} WEATHER Data Saving Sucessfully to Table {DB_WEATHER_TABLE}")
    
    except AnalysisException as e:
        logger.error(f"❌ ------ FAILED TO LOAD Processed WEATHER DATA To DB Table {DB_WEATHER_TABLE} " + str(e))
        spark.stop()
        return
   

    spark.stop()

if __name__ == "__main__":
    main()
