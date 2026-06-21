import requests
import boto3
import json
from datetime import datetime, timezone

STATES = ["va", "az", "tx"]
BUCKET = "water-stress-tracker-rahil"
s3 = boto3.client("s3")

def fetch_historical(state_code):
    url = (f"https://waterservices.usgs.gov/nwis/dv/?format=json"
           f"&stateCd={state_code}&parameterCd=00060"
           f"&startDT=2026-05-20&endDT=2026-06-19")
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def upload(data, state_code):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"historical/{state_code}/{timestamp}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data))
    print(f"Uploaded: {key}")

if __name__ == "__main__":
    for state in STATES:
        data = fetch_historical(state)
        upload(data, state)