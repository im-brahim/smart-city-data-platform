import boto3    #type: ignore
import logging
import json

# import psycopg2     #type: ignore
from botocore.exceptions import ClientError
from dotenv import load_dotenv  # type: ignore
from utils.config import (
    # DB_DRIVER,
    # DB_URL,
    B2_ACCESS_KEY_ID,
    B2_ENDPOINT_URL,
    B2_SECRET_ACCESS_KEY,
    B2_BUCKET_NAME
)

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

def get_watermarks(s3_client,source):

    try:
        respons = s3_client.get_object(Bucket=B2_BUCKET_NAME, Key=f"watermark/{source}.json")
        content = respons['Body'].read().decode('utf-8')
        watermarks = json.loads(content)
        return watermarks

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return {}
        else:
            raise 


def set_watermark(s3_client,source, segemnt_id, ts):
    watermark = get_watermarks(s3_client,source)
    watermark[segemnt_id] = ts
    
    return s3_client.put_object(
        Bucket='smart-city',
        Key=f"watermark/{source}.json", 
        Body=json.dumps(watermark).encode("utf-8"),
        ContentType="application/json")


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT_URL,
        aws_access_key_id=B2_ACCESS_KEY_ID,
        aws_secret_access_key=B2_SECRET_ACCESS_KEY,
    )


# ------- Postgre -----
# def get_track(source):
#     conn = psycopg2.connect(
#         host="postgres",
#         dbname="smartcity",
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD"), 
#     )    
#     try:
#         with conn:
#             with conn.cursor() as cur:
#                 query = """   select source, last_processed from pipeline_track where source = %s   """
#                 cur.execute(query, (source,))
#                 result = cur.fetchone()
#     finally:
#         conn.close()
#     return result

# ------- Postgre -----        
# def update_track(source, ts):
#     conn = psycopg2.connect(
#         host="postgres",
#         dbname="smartcity",
#         user=os.getenv("DB_USER"),
#         password=os.getenv("DB_PASSWORD"),
#     )
#     try:
#         with conn:
#             with conn.cursor() as cur:

#                 query = """
#                         INSERT INTO pipeline_track (source, last_processed)
#                         VALUES (%s, %s)
#                         ON CONFLICT (source)
#                         DO UPDATE SET last_processed = EXCLUDED.last_processed
#                         """
#                 data = (source, ts)
#                 cur.execute(query,data)       
#     finally:
#         conn.close()

