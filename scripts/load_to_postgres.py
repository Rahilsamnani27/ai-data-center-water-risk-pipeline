import boto3
import json
import psycopg2

BUCKET = "water-stress-tracker-rahil"

s3 = boto3.client("s3")
conn = psycopg2.connect(
    host="water_tracker_db",
    port=5432,
    dbname="water_tracker",
    user="postgres",
    password="postgres"
)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS raw_water_readings (
        site_name TEXT,
        latitude FLOAT,
        longitude FLOAT,
        flow_value FLOAT,
        reading_time TIMESTAMP,
        state_code TEXT
    );
""")
conn.commit()

def list_raw_files():
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/")
    return [obj["Key"] for obj in response.get("Contents", [])]

def load_file(key):
    state_code = key.split("/")[1]
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    data = json.loads(obj["Body"].read())

    time_series = data.get("value", {}).get("timeSeries", [])
    count = 0
    for series in time_series:
        site_name = series["sourceInfo"]["siteName"]
        geo = series["sourceInfo"]["geoLocation"]["geogLocation"]
        lat, lon = float(geo["latitude"]), float(geo["longitude"])

        for value_block in series["values"]:
            for v in value_block["value"]:
                flow = float(v["value"]) if v["value"] not in ("", None) else None
                reading_time = v["dateTime"]
                cur.execute("""
                    INSERT INTO raw_water_readings
                    (site_name, latitude, longitude, flow_value, reading_time, state_code)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (site_name, lat, lon, flow, reading_time, state_code))
                count += 1
    conn.commit()
    print(f"Loaded {count} readings from {key}")

if __name__ == "__main__":
    files = list_raw_files()
    print(f"Found {len(files)} files in S3")
    for key in files:
        load_file(key)
    print("Done loading into Postgres.")

cur.close()
conn.close()