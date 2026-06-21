SELECT
    site_name,
    latitude,
    longitude,
    flow_value,
    reading_date,
    state_code
FROM historical_water_readings
WHERE flow_value IS NOT NULL