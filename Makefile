# One-command entry points. Everything runs in Docker; nothing is installed on your machine.
#   make all            build, start, ingest and backfill everything, then print the report
#   make run MONTH=2017-03

COMPOSE := docker compose
JOB     := $(COMPOSE) run --rm pipeline
MONTH   ?= 2016-07

.PHONY: help env build up down init ingest run backfill report charts test test-e2e lint psql all airflow airflow-down clean

help:  ## show this help
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-10s %s\n", $$1, $$2}'

env:  ## create .env from .env.example (if missing)
	@test -f .env || (cp .env.example .env && echo "created .env")

build: env  ## build the pipeline image
	$(COMPOSE) build pipeline

up: env  ## start the data lake (RustFS) and the warehouse (Postgres)
	$(COMPOSE) up -d --wait objectstore postgres

down:  ## stop everything (data is kept)
	$(COMPOSE) --profile airflow down

init: up  ## create the bucket, database, tables and views (safe to rerun)
	$(JOB) python -m pipeline.init

ingest: up  ## land the raw CSV in the lake, one file per month
	$(JOB) python -m pipeline.ingest

run: up  ## process one month: make run MONTH=2017-03
	$(JOB) python -m pipeline.run --month $(MONTH)

backfill: up  ## process every month, oldest first
	$(JOB) python -m pipeline.run --all

report: up  ## print the headline numbers
	$(JOB) python -m pipeline.report

charts: up  ## regenerate the charts in docs/images
	$(JOB) python -m pipeline.charts

test: build  ## unit tests (Spark, no services needed)
	$(COMPOSE) run --rm --no-deps pipeline pytest tests/unit

test-e2e: up  ## end-to-end test on a small fixture, in a separate bucket and database
	$(JOB) pytest tests/e2e

lint: build  ## static checks
	$(COMPOSE) run --rm --no-deps pipeline ruff check .

psql: up  ## open a SQL shell in the warehouse
	$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

all: build init ingest backfill report  ## the whole pipeline, from scratch

airflow: build init  ## optional: start Airflow on http://localhost:8080 (the DAG backfills every month)
	$(COMPOSE) --profile airflow up -d --build airflow

airflow-down:  ## stop Airflow
	$(COMPOSE) --profile airflow stop airflow

clean:  ## stop everything AND delete the lake and warehouse data
	$(COMPOSE) --profile airflow down -v
