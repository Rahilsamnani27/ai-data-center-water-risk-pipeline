import requests
import boto3
import json
from datetime import datetime, timezone

STATES = ["va", "az", "tx"]
BUCKET = "water-stress-tracker-rahil"

s3 = boto3.client("s3")

def fetch_state_data(state_code):
    url = f"https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd={state_code}&parameterCd=00060"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def upload_to_s3(data, state_code):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"raw/{state_code}/{timestamp}.json"
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(data))
    print(f"Uploaded: {key}")

if __name__ == "__main__":
    for state in STATES:
        print(f"Fetching data for {state}...")
        data = fetch_state_data(state)
        upload_to_s3(data, state)
    print("Done — all states uploaded.")