import io
import pandas as pd
import json
from dotenv import load_dotenv      #type: ignore

from utils.data_io import get_client, get_watermarks, set_watermark, get_logger
from utils.config import (
    B2_BUCKET_NAME, BRONZE, SILVER
)

# ------------------- WEATHER PROCESSING ---------------
# records = the List of the json WEATHER from B2 [{},{}...]    
def flatten_weather(records):
    df = pd.DataFrame({
        "ingested_at":          [r["ingested_at"] for r in records],
        "timestamp":            [r["dt"] for r in records],
        "temp":                 [r["main"]["temp"] for r in records],
        "humidity":             [r["main"]["humidity"] for r in records],
        "pressure":             [r["main"]["pressure"] for r in records],
        "visibility":           [r["visibility"] for r in records],
        "wind_speed":           [r["wind"]["speed"] for r in records],
        "wind_deg":             [r["wind"]["deg"] for r in records],
        "weather_condition":    [r["weather"][0]["main"] for r in records],
        "weather_description":  [r["weather"][0]["description"] for r in records],
        "cloud_coverage":       [r["clouds"]["all"] for r in records],
        "city":                 [r["name"] for r in records],
        "latitude":             [r["coord"]["lat"] for r in records],
        "longitude":            [r["coord"]["lon"] for r in records],
    })

    df["ingested_at"] = pd.to_datetime(df["ingested_at"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    df["hour_of_day"] = df["ingested_at"].dt.hour
    df["day_of_week"] = df["ingested_at"].dt.dayofweek

    return df

# -------------------- TRAFFIC PROCESSING ------------------
# records = the List of the json TRAFFIC from B2 [{},{}...]    
def flatten_traffic(records: list) -> pd.DataFrame:
    df = pd.DataFrame({
        "ingested_at":         [r["ingested_at"] for r in records],
        "road_class":          [r["flowSegmentData"]["frc"] for r in records],
        "road_id":             [r["flowSegmentData"]["openlr"] for r in records],
        "is_closed":           [r["flowSegmentData"]["roadClosure"] for r in records],
        "currentSpeed":        [r["flowSegmentData"]["currentSpeed"] for r in records],
        "freeFlowSpeed":       [r["flowSegmentData"]["freeFlowSpeed"] for r in records],
        "currentTravelTime":   [r["flowSegmentData"]["currentTravelTime"] for r in records],
        "freeFlowTravelTime":  [r["flowSegmentData"]["freeFlowTravelTime"] for r in records],
        "confidence":          [r["flowSegmentData"]["confidence"] for r in records],
        "ref_latitude":        [r["flowSegmentData"]["coordinates"]["coordinate"][0]["latitude"] for r in records],
        "ref_longitude":       [r["flowSegmentData"]["coordinates"]["coordinate"][0]["longitude"] for r in records],
        "segment_id":          [r["segment_id"] for r in records]
    })

    df["ingested_at"] = pd.to_datetime(df["ingested_at"])
    df["congestion_ratio"] = (df["currentSpeed"] / df["freeFlowSpeed"]).round(2)
    df["speed_delta"] = df["freeFlowSpeed"] - df["currentSpeed"]
    df["delay_seconds"] = df["currentTravelTime"] - df["freeFlowTravelTime"]
    df["hour_of_day"] = df["ingested_at"].dt.hour
    df["day_of_week"] = df["ingested_at"].dt.dayofweek

    return df


def process_data(s3_client, source, watermark_key, bronze_prefix, silver_prefix, flatten_data, NbrFile):
    logger = get_logger("Process Data Function ")
    # ----------------- Watermar Logic --------------
    watermarks = get_watermarks(s3_client,source, watermark_key)
    watermark = watermarks.get(watermark_key)
    if watermark:
        start_after_key = f'{bronze_prefix}/{watermark_key}/{watermark}.json'
        logger.info(f" The last Watermark is {watermark} for {source} -> {watermark_key}")
    else:
        logger.info(f" No watermark Yet for {source} -> {watermark_key}")
        start_after_key = ""
    # ---------------------- Separate Sources -------------------
    if source == "traffic":
        response = s3_client.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=f"{bronze_prefix}/{watermark_key}/",
                                          MaxKeys=NbrFile, StartAfter=start_after_key)
    elif source == "weather":
        response = s3_client.list_objects_v2(Bucket=B2_BUCKET_NAME, Prefix=f"{bronze_prefix}/",
                                                  MaxKeys=NbrFile, StartAfter=start_after_key)
    else:
        logger.warn("Unknown data Source")
        return

    # --------------- Construct a List of Keys-paths if there is Some new files -------------
    contents = response.get("Contents", [])
    if not contents:
        logger.info("No new objects to process.")
        return
    
    keys = [obj['Key'] for obj in contents]
    # ------- get the object from the Keys as a List --------
    records = []
    for key in keys:
        resp = s3_client.get_object(Bucket=B2_BUCKET_NAME, Key=key)
        content = resp['Body'].read().decode('utf-8')
        record = json.loads(content)
        records.append(record)

    logger.info(f" Loaded {len(records)} records for {source}")

    try:
        df = flatten_data(records)

        # Use in Memorry for parquet if the Flattened work
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow")

        # extract the timestamp, year, month as string from the last processed record 
        last_record_processed = keys[-1].split('/')[-1].split('.')[0]
        ts = last_record_processed
        year = ts.split('T')[0].split('-')[0]
        month = ts.split('T')[0].split('-')[1]

        # ------ Load the Parquet to Silver -------
        s3_client.put_object(
        Bucket=B2_BUCKET_NAME,
        Key= f"{silver_prefix}/{watermark_key}/{year}-{month}/{ts}.parquet",
        Body=buffer.getvalue(),
        ContentType='application/vnd.apache.parquet'
        )
        # ----- after Successful Load update the Watermark -------
        set_watermark(s3_client,source, watermark_key, ts)
        logger.info(f" Success Upload and Update -- {source}: watermar_key: {watermark_key} Watermark_ts: {ts}")

    except Exception as e:
        logger.error(f" Failed for {watermark_key} --- : {e}", exc_info=True)
        raise

def main():
    load_dotenv()
    logger = get_logger(" Main Funtion ")
    s3_client = get_client()

    # ------------------------ Traffic -------------------------
    bronze_traffic = f"{BRONZE}/traffic"
    silver_traffic = f"{SILVER}/traffic"

    for i in range(1,9):
        segment_id = f"S{i:02d}"
        try:
            process_data(
                s3_client,
                "traffic",
                segment_id,
                bronze_traffic,
                silver_traffic, 
                flatten_traffic,
                NbrFile=50
                )

            logger.info(f" {segment_id} road processed successfully. ")

        except Exception as e:
            logger.error(f"Processing failed for {segment_id}: {e}")
            raise

    # ------------------------ Weather -------------------------
    bronze_weather = f"{BRONZE}/weather"
    silver_weather = f"{SILVER}/weather"
    try:
        process_data(
            s3_client, 
            "weather",
            "weather",
            bronze_weather,
            silver_weather,
            flatten_weather,
            NbrFile = 50
            )

        logger.info(f" Weather processed successfully. ")

    except Exception as e:
        logger.error(f"Process Weather Failed: {e}")
        raise

if __name__ == "__main__":
    main()

