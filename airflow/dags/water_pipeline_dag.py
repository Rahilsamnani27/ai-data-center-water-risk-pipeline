from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "rahil",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="water_stress_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 6, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    fetch_data = BashOperator(
        task_id="fetch_water_data",
        bash_command="python /opt/airflow/lambdas/fetch_water_data.py",
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command="python /opt/airflow/scripts/load_to_postgres.py",
    )

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /opt/airflow/dbt_project/water_stress_dbt && dbt run",
    )

    test_dbt = BashOperator(
        task_id="test_dbt_models",
        bash_command="cd /opt/airflow/dbt_project/water_stress_dbt && dbt test",
    )

    fetch_data >> load_to_postgres >> run_dbt >> test_dbt