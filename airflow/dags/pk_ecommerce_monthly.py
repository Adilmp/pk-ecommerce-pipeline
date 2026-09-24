"""The pipeline as an Airflow DAG: one DAG run per order month.

With `catchup=True`, Airflow itself performs the backfill: it creates one run for every month
from July 2016 to August 2018 and runs them in order (`max_active_runs=1`). Each run is the same
three idempotent steps as `make run MONTH=...`, so any failed run can simply be retried.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Rendered by Airflow for each run, e.g. "2017-03" for the March 2017 run.
MONTH = "{{ logical_date.strftime('%Y-%m') }}"

with DAG(
    dag_id="pk_ecommerce_monthly",
    description="Ingest one month, clean it with PySpark, load it into the star schema",
    schedule="@monthly",
    start_date=datetime(2016, 7, 1),
    end_date=datetime(2018, 8, 1),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["pk-ecommerce"],
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        bash_command=f"cd /app && python -m pipeline.ingest --month {MONTH}",
    )
    silver = BashOperator(
        task_id="silver",
        bash_command=f"cd /app && python -m pipeline.run --month {MONTH} --steps silver",
    )
    gold = BashOperator(
        task_id="gold",
        bash_command=f"cd /app && python -m pipeline.run --month {MONTH} --steps gold",
    )

    ingest >> silver >> gold
