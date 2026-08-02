import os
import psycopg2     #type: ignore
import boto3    #type: ignore

from dotenv import load_dotenv  # type: ignore
from utils.config import (
    DB_DRIVER,
    DB_URL,
    B2_ACCESS_KEY_ID,
    B2_ENDPOINT_URL,
    B2_SECRET_ACCESS_KEY
)

load_dotenv()


def read_from_db(spark: "SparkSession") -> "DataFrame":     #type: ignore
    """
    Read the data from PostgreSQL TABLE

    Args:
        spark: Active SparkSession instance.

    Returns:
        Spark DataFrame with the full table contents.
    """
    return (
        spark.read.format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", os.getenv("DB_TABLE"))
        .option("user", os.getenv("DB_USER"))
        .option("password", os.getenv("DB_PASSWORD"))
        .option("driver", DB_DRIVER)
        .load()
    )


def save_in_db(data: "DataFrame", DB_TABLE: str) -> None:       #type: ignore
    """
    Append a Spark DataFrame to a PostgreSQL table.

    Args:
        data: Spark DataFrame to save.
        DB_TABLE: Target table name in PostgreSQL.
    """
    (
        data.write.format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", DB_TABLE)
        .option("user", os.getenv("DB_USER"))
        .option("password", os.getenv("DB_PASSWORD"))
        .option("driver", DB_DRIVER)
        .mode("append")
        .save()
    )

def save_parquet_to_Cloud(data, path):
    data.write.format("parquet").mode("overwrite").save(path)

def read_parquet_from_Cloud(spark, *path):
        return spark.read.parquet(*path)

# def read_parquet_from_Cloud(spark, s3_client, bucket, path):
#     # path = f"s3a://{bucket}{silver_path}"
#     response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
#     object = response["Contents"]

#     list_keys = [path["Key"] for path in object]       # Getting the list of keys e.g: silver/traffic/timestamp.json    
#     paths = []
#     for path in list_keys:              # boocle throw the keys to create a list of full paths e.g: s3a://bucket/silver....json 
#         if not path.endswith("SUCCESS"):
#             paths.append(f"s3a://{bucket}/{path}")


#     # return spark.read.format("parquet").load(path)
#     return spark.read.parquet(path)

# Read from database by Spark
# def get_track(spark, source):
#     jdbcDF = spark.read \
#     .format("jdbc") \
#     .option("url", DB_URL) \
#     .option("driver", DB_DRIVER) \
#     .option("query", f"select source, last_processed from {DB_UPDATE_TABLE} where source = '{source}' ") \
#     .option("user", os.getenv("DB_USER")) \
#     .option("password", os.getenv("DB_PASSWORD")) \
#     .load() 
#     return jdbcDF

def get_track(source):
    conn = psycopg2.connect(
        host="postgres",
        dbname="smartcity",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"), 
    )    
    try:
        with conn:
            with conn.cursor() as cur:
                query = """   select source, last_processed from pipeline_track where source = %s   """
                cur.execute(query, (source,))
                result = cur.fetchone()
    finally:
        conn.close()
    return result
            
def update_track(source, ts):
    conn = psycopg2.connect(
        host="postgres",
        dbname="smartcity",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )
    try:
        with conn:
            with conn.cursor() as cur:

                query = """
                        INSERT INTO pipeline_track (source, last_processed)
                        VALUES (%s, %s)
                        ON CONFLICT (source)
                        DO UPDATE SET last_processed = EXCLUDED.last_processed
                        """
                data = (source, ts)
                cur.execute(query,data)       
    finally:
        conn.close()


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT_URL,
        aws_access_key_id=B2_ACCESS_KEY_ID,
        aws_secret_access_key=B2_SECRET_ACCESS_KEY,
    )



def save_only_new_rows(spark, data: "DataFrame", DB_TABLE: str) -> None:        #type: ignore
    """
    Read the data from PostgreSQL Table .

    Args:
        spark: Active SparkSession instance.
        data: the new data that will append.
        DB_TABLE: the table where be data load
    """
    try:
        existing_df = (
            spark.read.format("jdbc")
            .option("url", DB_URL)
            .option("dbtable", f"(SELECT ingested_at FROM {DB_TABLE}) as sub") # Only read IDs to save memory
            .option("user", os.getenv("DB_USER"))
            .option("password", os.getenv("DB_PASSWORD"))
            .option("driver", DB_DRIVER)
            .load()
        )

        # 2. Keep only rows that are NOT in the existing table
        new_data = data.join(existing_df, on="ingested_at", how="left_anti")
    
    except Exception as e:
        print(f"There is an Error : {e}")
        new_data = data

    # 3. Append only the new rows
    save_in_db(new_data, DB_TABLE)
    