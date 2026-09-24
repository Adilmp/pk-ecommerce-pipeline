"""Step 3: silver -> gold. Load one month into the Postgres star schema.

1. Spark writes the month's clean rows and quarantined rows into staging tables (JDBC).
2. In ONE database transaction:
     - upsert the dimensions (customer, product, payment method)
     - delete that month's facts, then insert them from staging
     - same for that month's quarantine rows
   If anything fails, the transaction rolls back and the warehouse is unchanged.

Delete-then-insert per month makes the load idempotent (decision D10). Loads never overlap:
they share the staging tables, so a Postgres advisory lock lets only one run at a time.
"""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.config import QUARANTINE_PREFIX, SILVER_PREFIX, Settings
from pipeline.db import SQL_DIR, connect, run_sql_file
from pipeline.schema import QUARANTINE_SCHEMA, SILVER_SCHEMA

log = logging.getLogger("pipeline.gold")


# Every load shares the staging tables, so loads must not overlap: a second load waits on
# this Postgres advisory lock until the first has committed (decision D26). A session-level
# advisory lock is released when its connection closes, even if the load fails.
LOAD_LOCK_ID = 20160701


class LoadCheckError(Exception):
    """The warehouse does not match what was staged."""


def write_staging(df, settings: Settings, table: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", table)
        .option("user", settings.pg_user)
        .option("password", settings.pg_password)
        .option("driver", "org.postgresql.Driver")
        # Overwrite = TRUNCATE + INSERT, keeping the table definition from sql/schema.
        .option("truncate", "true")
        .option("batchsize", 10000)
        .mode("overwrite")
        .save()
    )


def load_month(spark: SparkSession, settings: Settings, month: str) -> dict:
    # Reading with an explicit schema also works for a month with zero clean rows.
    silver = (
        spark.read.schema(SILVER_SCHEMA)
        .parquet(settings.lake_path(SILVER_PREFIX, f"order_month={month}"))
        .withColumn("order_month", F.lit(month))
    )
    quarantine = (
        spark.read.schema(QUARANTINE_SCHEMA)
        .parquet(settings.lake_path(QUARANTINE_PREFIX, f"order_month={month}"))
        .withColumn("order_month", F.lit(month))
    )
    with connect(settings, autocommit=True) as lock:
        lock.execute("SELECT pg_advisory_lock(%s)", (LOAD_LOCK_ID,))
        facts, quarantined = _stage_and_load(settings, month, silver, quarantine)

    log.info("gold   %s | facts %s | quarantine %s", month, f"{facts:,}", f"{quarantined:,}")
    return {"fact_rows": facts, "quarantine_rows": quarantined}


def _stage_and_load(settings: Settings, month: str, silver, quarantine) -> tuple[int, int]:
    write_staging(silver, settings, "staging.stg_order_items")
    write_staging(quarantine, settings, "staging.stg_quarantine")

    params = {"month": month}
    with connect(settings) as conn:  # one transaction: commits on success, rolls back on error
        staged = conn.execute("SELECT count(*) FROM staging.stg_order_items").fetchone()[0]
        staged_q = conn.execute("SELECT count(*) FROM staging.stg_quarantine").fetchone()[0]

        run_sql_file(conn, SQL_DIR / "load" / "01_dimensions.sql", params)
        run_sql_file(conn, SQL_DIR / "load" / "02_facts.sql", params)
        run_sql_file(conn, SQL_DIR / "load" / "03_quarantine.sql", params)

        facts = conn.execute(
            "SELECT count(*) FROM dw.fact_order_items WHERE order_month = %s", (month,)
        ).fetchone()[0]
        quarantined = conn.execute(
            "SELECT count(*) FROM dq.quarantine WHERE order_month = %s", (month,)
        ).fetchone()[0]
        # Every staged row must land; a missing dimension match would silently drop facts.
        if facts != staged or quarantined != staged_q:
            raise LoadCheckError(
                f"{month}: staged {staged} facts / {staged_q} quarantine rows, "
                f"loaded {facts} / {quarantined}"
            )
    return facts, quarantined
