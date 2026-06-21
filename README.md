# AI Data Center Water Stress Tracker

A data engineering pipeline that ingests real-time and historical water flow data from the USGS Water Services API, joins it against known AI data center locations, computes a water-stress score per site, and surfaces it through a dashboard — orchestrated end-to-end by Apache Airflow and tested with dbt.

![Architecture Diagram](architecture_diagram.svg)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
  - [Ingestion](#ingestion)
  - [Storage](#storage)
  - [Transformation (dbt)](#transformation-dbt)
  - [Data Quality](#data-quality)
  - [Orchestration (Airflow)](#orchestration-airflow)
- [Dashboard](#dashboard)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Running the Pipeline](#running-the-pipeline)
- [Results](#results)
- [Data Sources](#data-sources)

---

## Overview

AI data centers consume enormous volumes of water for cooling. While carbon footprint tracking for AI infrastructure is becoming standard, water stress is rarely monitored at the same resolution. This pipeline tracks real water flow near major US AI data center clusters and flags which ones sit near water sources currently running below their 30-day average — surfacing a sustainability signal that most public dashboards don't track today.

---

## Architecture

```
Data Source              Ingestion            Storage              Transform            Orchestration         Output
┌──────────────┐    ┌────────────────┐   ┌────────────────┐   ┌────────────────┐   ┌────────────────┐   ┌──────────────┐
│ USGS Water   │    │                │   │                │   │                │   │                │   │              │
│ Services API │───>│  Python +      │──>│  Amazon S3     │──>│  Postgres      │──>│  dbt models    │   │  Streamlit   │
│ (live +      │    │  Boto3         │   │  (raw JSON)    │   │  (Docker)      │   │  + tests       │──>│  Dashboard   │
│ 30-day       │    │                │   │                │   │                │   │                │   │              │
│ historical)  │    └────────────────┘   └────────────────┘   └────────────────┘   └────────────────┘   └──────────────┘
└──────────────┘                                                       ▲                    │
                                                                        │                    │
                    ┌──────────────┐                                   │                    ▼
                    │ data_centers │───────────────────────────────────┘           ┌────────────────┐
                    │   .csv       │      (joined by nearest site)                  │ Apache Airflow │
                    └──────────────┘                                                │  (daily DAG)   │
                                                                                     └────────────────┘
```

**Orchestration** is handled by Apache Airflow running in Docker, which sequences all four pipeline stages on a daily schedule: ingestion → load → transform → test.

---

## Tech Stack

| Component           | Technology                            |
|----------------------|----------------------------------------|
| **Ingestion**        | Python, Requests, Boto3                |
| **Cloud Storage**    | Amazon S3                              |
| **Database**         | PostgreSQL (Docker)                    |
| **Transformation**   | dbt (dbt-postgres)                     |
| **Data Quality**     | dbt tests, dbt_utils                   |
| **Orchestration**    | Apache Airflow (Docker, custom image)  |
| **Dashboard**        | Streamlit, Plotly                      |
| **Infrastructure**   | Docker, Docker Compose                 |
| **Cloud/Auth**       | AWS IAM, AWS CLI                       |
| **Languages**        | Python 3, SQL                          |

---

## Project Structure

```
ai-data-center-water-risk-pipeline/
│
├── lambdas/
│   └── fetch_water_data.py          # Pulls live USGS data, uploads raw JSON to S3
│
├── scripts/
│   ├── fetch_historical_water.py    # Pulls 30-day historical USGS data
│   ├── load_to_postgres.py          # Loads raw S3 JSON into Postgres
│   ├── load_historical_to_postgres.py
│   └── compute_water_stress.py      # Standalone Python version of the stress calc
│
├── dbt_project/
│   └── water_stress_dbt/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_water_readings.sql
│       │   │   └── schema.yml       # not_null, accepted_values tests
│       │   └── marts/
│       │       ├── mart_water_stress.sql
│       │       └── schema.yml       # unique, accepted_range tests
│       └── packages.yml             # dbt_utils dependency
│
├── airflow/
│   ├── Dockerfile                   # Custom image: boto3, psycopg2, dbt-postgres
│   ├── docker-compose.yml
│   └── dags/
│       └── water_pipeline_dag.py    # 4-task daily DAG
│
├── docker/
│   └── docker-compose.yml           # Postgres container
│
├── data/
│   └── data_centers.csv             # Curated AI data center locations (VA, AZ, TX)
│
├── dashboard.py                      # Streamlit dashboard
└── README.md
```

---

## Data Flow

### Ingestion

`fetch_water_data.py` pulls live readings from the USGS Water Services API (public, no API key required) for Virginia, Arizona, and Texas, and uploads raw JSON to S3:

```
s3://<bucket>/raw/{state}/{timestamp}.json
s3://<bucket>/historical/{state}/{timestamp}.json
```

`fetch_historical_water.py` separately pulls 30 days of daily flow values per state — needed to compute a meaningful average, since a single snapshot has no variation to compare against.

### Storage

`load_to_postgres.py` / `load_historical_to_postgres.py` parse the raw S3 JSON and insert structured rows into two Postgres tables: `raw_water_readings` and `historical_water_readings`.

### Transformation (dbt)

Two dbt models handle the transformation logic in version-controlled SQL:

- **`stg_water_readings`** — staging layer, filters nulls
- **`mart_water_stress`** — joins readings against `data_centers.csv` by nearest site (geodesic distance), and computes:

```
stress_score = 1 - (latest_flow / avg_flow)
```

clamped between 0 and 1, with explicit `NULL` handling for sites with zero or missing flow rather than silently reporting a false "low stress."

### Data Quality

10 dbt tests run across both models — `not_null`, `unique`, `accepted_values`, and a `dbt_utils.accepted_range` check on the stress score. These caught two real issues during development: a duplicate-row bug from an unaggregated join, and stress scores exceeding the expected 0–1 range — both fixed at the model level before reaching the mart table.

### Orchestration (Airflow)

A daily DAG runs all four stages in sequence:

```
fetch_water_data >> load_to_postgres >> run_dbt_models >> test_dbt_models
```

Airflow runs in a custom Docker image (built on top of `apache/airflow`, with `boto3`, `psycopg2`, and `dbt-postgres` installed), on a Docker network shared with the Postgres container.

---

## Dashboard

A Streamlit dashboard reads directly from the `mart_water_stress` table and shows:

- Summary metrics — data centers tracked, high-stress count, no-data count
- A map of all tracked data centers, colored by stress level
- A full results table — nearest water site, distance, flow values, and stress score per data center

---

## Prerequisites

- Docker Desktop (with WSL2 on Windows)
- Python 3.10+
- An AWS account with an IAM user (S3 access) and AWS CLI configured
- `dbt-postgres`

---

## Setup

```bash
git clone https://github.com/<your-username>/ai-data-center-water-risk-pipeline.git
cd ai-data-center-water-risk-pipeline

python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt

aws configure                # enter your IAM access key/secret
aws s3 mb s3://<your-bucket-name>
```

Update the `BUCKET` variable in `lambdas/fetch_water_data.py` and `scripts/load_to_postgres.py` to match your bucket name.

Start Postgres:
```bash
cd docker
docker compose up -d
```

Initialize dbt:
```bash
cd dbt_project
dbt init water_stress_dbt
dbt deps
```

---

## Running the Pipeline

### Manual (local testing)
```bash
python lambdas/fetch_water_data.py
python scripts/fetch_historical_water.py
python scripts/load_to_postgres.py
python scripts/load_historical_to_postgres.py
cd dbt_project/water_stress_dbt && dbt run && dbt test
streamlit run dashboard.py
```

### Automated (Airflow)
```bash
cd airflow
docker compose build
docker compose up -d
```

Visit `http://localhost:8080`, log in with the auto-generated admin password, unpause `water_stress_pipeline`, and trigger it.

---

## Results

All four pipeline stages run successfully end-to-end, scheduled daily:

![Airflow DAG Success](airflow_success.png)

Sample output from `mart_water_stress`:

![Dashboard](dashboard_screenshot.png)

---

## Data Sources

- [USGS Water Services API](https://waterservices.usgs.gov/) — public, free, no API key required
