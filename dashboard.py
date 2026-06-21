import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

st.set_page_config(page_title="Water Stress Tracker", layout="wide")

st.title("AI Data Center Water Stress Tracker")
st.caption("Tracking water stress near major US AI data center clusters using USGS data")

conn = psycopg2.connect(
    host="localhost", port=5432, dbname="water_tracker",
    user="postgres", password="postgres"
)

df = pd.read_sql("SELECT * FROM data_center_water_stress", conn)

col1, col2, col3 = st.columns(3)
col1.metric("Data centers tracked", len(df))
col2.metric("High stress", (df["stress_level"] == "high").sum())
col3.metric("No data", (df["stress_level"] == "no data").sum())

st.subheader("Map of data center water stress")
fig = px.scatter_mapbox(
    df, lat="lat", lon="lon", color="stress_level",
    hover_name="name", hover_data=["city", "state", "nearest_site", "distance_km"],
    color_discrete_map={"high": "red", "moderate": "orange", "low": "green", "no data": "gray"},
    zoom=3, height=500
)
fig.update_layout(mapbox_style="open-street-map")
st.plotly_chart(fig, use_container_width=True)

st.subheader("Full results")
st.dataframe(df[["name", "city", "state", "nearest_site", "distance_km",
                  "latest_flow", "avg_flow", "stress_score", "stress_level"]])