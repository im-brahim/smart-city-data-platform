import os

from dotenv import load_dotenv  # type: ignore
from pyspark.errors import AnalysisException

load_dotenv()


def read_json_from_minio(spark: "SparkSession", path: str):
    """
    Read a JSON (or JSONL) dataset from MinIO/S3.

    Args:
        spark: Active SparkSession instance.
        path: S3A path to the JSON file (e.g. s3a://bucket/file.json)

    Returns:
        Spark DataFrame with the JSON contents.
    """
    return spark.read.json(path)


def read_parquet_from_minio(spark: "SparkSession", path: str):
    """
    Read a Parquet dataset from MinIO/S3.

    Args:
        spark: Active SparkSession instance.
        path: S3A path to the parquet directory (e.g. s3a://bucket/folder/)

    Returns:
        Spark DataFrame with the parquet contents.
    """
    return spark.read.parquet(path)


def save_json_to_minio(data: "DataFrame", path: str) -> None:
    """
    Save a Spark DataFrame as JSON to MinIO/S3.

    Args:
        data: Spark DataFrame to persist.
        path: S3A destination path (e.g. s3a://bucket/folder/)
    """
    data.write.mode("overwrite").json(path)


def save_parquet_to_minio(data: "DataFrame", path: str) -> None:
    """
    Save a Spark DataFrame as Parquet to MinIO/S3.

    Args:
        data: Spark DataFrame to persist.
        path: S3A destination path (e.g. s3a://bucket/folder/)
    """
    
    data.write.mode("overwrite").parquet(path)
     



def read_from_db(spark: "SparkSession") -> "DataFrame":
    """
    Read the crypto prices table from PostgreSQL.

    Args:
        spark: Active SparkSession instance.

    Returns:
        Spark DataFrame with the full table contents.
    """
    return (
        spark.read.format("jdbc")
        .option("url", os.getenv("DB_URL"))
        .option("dbtable", os.getenv("DB_TABLE"))
        .option("user", os.getenv("DB_USER"))
        .option("password", os.getenv("DB_PASSWORD"))
        .option("driver", os.getenv("DB_DRIVER"))
        .load()
    )


def save_in_db(data: "DataFrame", DB_TABLE: str) -> None:
    """
    Append a Spark DataFrame to a PostgreSQL table.

    Args:
        data: Spark DataFrame to save.
        DB_TABLE: Target table name in PostgreSQL.
    """
    (
        data.write.format("jdbc")
        .option("url", os.getenv("DB_URL"))
        .option("dbtable", DB_TABLE)
        .option("user", os.getenv("DB_USER"))
        .option("password", os.getenv("DB_PASSWORD"))
        .option("driver", os.getenv("DB_DRIVER"))
        .mode("append")
        .save()
    )