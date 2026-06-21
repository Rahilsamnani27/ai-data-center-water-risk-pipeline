WITH site_stats AS (
    SELECT
        site_name,
        AVG(flow_value) AS avg_flow,
        MAX(reading_date) AS latest_date
    FROM {{ ref('stg_water_readings') }}
    GROUP BY site_name
),

latest_reading AS (
    SELECT
        s.site_name,
        AVG(s.flow_value) AS latest_flow
    FROM {{ ref('stg_water_readings') }} s
    JOIN site_stats st
        ON s.site_name = st.site_name
        AND s.reading_date = st.latest_date
    GROUP BY s.site_name
)

SELECT
    ss.site_name,
    ss.avg_flow,
    lr.latest_flow,
    CASE
        WHEN ss.avg_flow = 0 OR ss.avg_flow IS NULL THEN NULL
        ELSE LEAST(GREATEST(1 - (lr.latest_flow / ss.avg_flow), 0), 1)
    END AS stress_score
FROM site_stats ss
JOIN latest_reading lr ON ss.site_name = lr.site_name