#!/bin/sh
# Start Airflow (scheduler + web UI) for local use.
set -e
# Airflow keeps its own metadata in a separate "airflow" database in the same Postgres.
PGDATABASE=airflow python -c "from pipeline.config import get_settings; from pipeline.db import ensure_database; ensure_database(get_settings())"
exec airflow standalone
