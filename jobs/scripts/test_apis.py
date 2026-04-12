"""
Quick test to verify API responses and understand data structure.
Run this locally: python jobs/scripts/test_apis.py
"""

import requests , json
import os
from dotenv import load_dotenv # type: ignore

load_dotenv()

def test_weather():
    url = os.getenv("WEATHER_API")
    print("\n--- WEATHER API ---")
    res = requests.get(url)
    res.raise_for_status()
    data = res.json()
    print(json.dumps(data, indent=2))

def test_traffic():
    url = os.getenv("TRAFFIC_API")
    print("\n--- TRAFFIC API ---")
    res = requests.get(url)
    res.raise_for_status()
    data = res.json()
    print(json.dumps(data, indent=2))

if __name__ == "__main__":
    test_weather()
    test_traffic()



