import boto3
import json
import psycopg2

BUCKET = "water-stress-tracker-rahil"
s3 = boto3.client("s3")
conn = psycopg2.connect(
    host="localhost", port=5432, dbname="water_tracker",
    user="postgres", password="postgres"
)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS historical_water_readings (
        site_name TEXT,
        latitude FLOAT,
        longitude FLOAT,
        flow_value FLOAT,
        reading_date DATE,
        state_code TEXT
    );
""")
cur.execute("DELETE FROM historical_water_readings;")
conn.commit()

def list_files():
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix="historical/")
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
                if v["value"] in ("", None):
                    continue
                flow = float(v["value"])
                reading_date = v["dateTime"][:10]
                cur.execute("""
                    INSERT INTO historical_water_readings
                    (site_name, latitude, longitude, flow_value, reading_date, state_code)
                    VALUES (%s,%s,%s,%s,%s,%s)
                """, (site_name, lat, lon, flow, reading_date, state_code))
                count += 1
    conn.commit()
    print(f"Loaded {count} readings from {key}")

if __name__ == "__main__":
    files = list_files()
    print(f"Found {len(files)} files")
    for key in files:
        load_file(key)
    print("Done.")

cur.close()
conn.close()