# Airflow on top of the pipeline image: tasks run with exactly the same Python, Java, Spark,
# jars and code as `make backfill`. Only the scheduler is added.
FROM pk-ecommerce-pipeline:latest

ARG AIRFLOW_VERSION=3.3.2
USER root
RUN pip install --no-cache-dir "apache-airflow[postgres]==${AIRFLOW_VERSION}" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"
USER app

ENV AIRFLOW_HOME=/home/app/airflow
CMD ["sh", "/app/airflow/entrypoint.sh"]
