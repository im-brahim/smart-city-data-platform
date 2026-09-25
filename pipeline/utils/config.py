import os
from dotenv import load_dotenv  # type: ignore

load_dotenv()

# B2: Connection parameters & Credentiels for Spark
B2_ENDPOINT_URL = os.getenv("B2_ENDPOINT_URL")
B2_ACCESS_KEY_ID = os.getenv("B2_ACCESS_KEY_ID")
B2_SECRET_ACCESS_KEY = os.getenv("B2_SECRET_ACCESS_KEY") 
B2_BUCKET_NAME = "smart-city"
BRONZE = "bronze"
SILVER = "silver"
WATERMARKS = "watermarks"

# DATABASE: CONFIGURATION:
DB_URL = "jdbc:postgresql://postgres:5432/smartcity"
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")  
DB_DRIVER = "org.postgresql.Driver"
DB_NAME = "smartcity"

# DATABASE: TABLES
DB_WEATHER_TABLE = "weather"
DB_TRAFFIC_TABLE = "traffic"
DB_UPDATE_TABLE = "pipleline_track"

