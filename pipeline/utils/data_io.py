import boto3    #type: ignore
import logging
import json

from botocore.exceptions import ClientError
from dotenv import load_dotenv  # type: ignore
from utils.config import (
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


def get_watermarks(s3_client,source, watermark_key):

    try:
        respons = s3_client.get_object(Bucket=B2_BUCKET_NAME, Key=f"watermark/{source}/{watermark_key}.json")
        content = respons['Body'].read().decode('utf-8')
        watermarks = json.loads(content)
        return watermarks

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return {}
        else:
            raise 


def set_watermark(s3_client,source, watermark_key, ts):
    watermark = get_watermarks(s3_client,source,watermark_key)
    watermark[watermark_key] = ts
    
    return s3_client.put_object(
        Bucket=B2_BUCKET_NAME,
        Key=f"watermark/{source}/{watermark_key}.json", 
        Body=json.dumps(watermark).encode("utf-8"),
        ContentType="application/json")


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=B2_ENDPOINT_URL,
        aws_access_key_id=B2_ACCESS_KEY_ID,
        aws_secret_access_key=B2_SECRET_ACCESS_KEY,
    )


