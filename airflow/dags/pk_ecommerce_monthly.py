"""The pipeline as an Airflow DAG: one DAG run per order month.

Each run covers one **data interval**, a whole calendar month, and starts only once that month
is over: the March 2017 run covers [1 March, 1 April) and runs on 1 April. That is what a live
schedule needs, since a month can't be processed before it has happened. (Airflow 3's plain
"@monthly" would instead run at the *start* of the month, with an empty interval.)

With `catchup=True`, Airflow itself performs the backfill: it creates one run for every month
from July 2016 to August 2018 and runs them in order (`max_active_runs=1`). Each run is the same
three idempotent steps as `make run MONTH=...`, so any failed run can simply be retried.
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

# Rendered by Airflow for each run: the month the run's data interval covers, e.g. "2017-03".
MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"

with DAG(
    dag_id="pk_ecommerce_monthly",
    description="Ingest one month, clean it with PySpark, load it into the star schema",
    # Midnight UTC on the 1st of every month, processing the month that just ended.
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    start_date=datetime(2016, 7, 1),  # first interval: July 2016
    end_date=datetime(2018, 8, 1),    # last interval starts here: August 2018
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
