import psycopg2
import pandas as pd
from geopy.distance import geodesic

conn = psycopg2.connect(
    host="localhost", port=5432, dbname="water_tracker",
    user="postgres", password="postgres"
)

historical = pd.read_sql("SELECT * FROM historical_water_readings", conn)
data_centers = pd.read_csv("data/data_centers.csv")

sites = historical[["site_name", "latitude", "longitude"]].drop_duplicates()

def find_nearest_site(dc_row):
    dc_coord = (dc_row["lat"], dc_row["lon"])
    distances = sites.apply(
        lambda s: geodesic(dc_coord, (s["latitude"], s["longitude"])).km, axis=1
    )
    nearest_idx = distances.idxmin()
    return sites.loc[nearest_idx, "site_name"], distances[nearest_idx]

data_centers["nearest_site"], data_centers["distance_km"] = zip(
    *data_centers.apply(find_nearest_site, axis=1)
)

# Use real historical data: avg over 30 days, latest = most recent day
historical_sorted = historical.sort_values("reading_date")
site_stats = historical_sorted.groupby("site_name")["flow_value"].agg(
    avg_flow="mean", min_flow="min", latest_flow="last"
).reset_index()

result = data_centers.merge(site_stats, left_on="nearest_site", right_on="site_name", how="left")

result["stress_score"] = 1 - (result["latest_flow"] / result["avg_flow"])
result["stress_score"] = result["stress_score"].clip(lower=0)

def stress_label(score):
    if pd.isna(score):
        return "no data"
    elif score >= 0.3:
        return "high"
    elif score >= 0.1:
        return "moderate"
    else:
        return "low"

result["stress_level"] = result["stress_score"].apply(stress_label)

print(result[["name", "city", "nearest_site", "distance_km", "latest_flow", "avg_flow", "stress_score", "stress_level"]].to_string())

cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS data_center_water_stress (
        name TEXT, state TEXT, city TEXT, lat FLOAT, lon FLOAT,
        nearest_site TEXT, distance_km FLOAT,
        latest_flow FLOAT, avg_flow FLOAT,
        stress_score FLOAT, stress_level TEXT
    );
""")
cur.execute("DELETE FROM data_center_water_stress;")

for _, row in result.iterrows():
    cur.execute("""
        INSERT INTO data_center_water_stress
        (name, state, city, lat, lon, nearest_site, distance_km, latest_flow, avg_flow, stress_score, stress_level)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (row["name"], row["state"], row["city"], row["lat"], row["lon"],
          row["nearest_site"], row["distance_km"], row["latest_flow"],
          row["avg_flow"], row["stress_score"], row["stress_level"]))

conn.commit()
cur.close()
conn.close()
print("Saved.")