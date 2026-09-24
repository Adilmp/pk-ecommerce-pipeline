"""Step 3: silver -> gold. Load one month into the Postgres star schema.

1. Spark writes the month's clean rows and quarantined rows into staging tables (JDBC).
2. In ONE database transaction:
     - upsert the dimensions (customer, product, payment method)
     - delete that month's facts, then insert them from staging
     - same for that month's quarantine rows
   If anything fails, the transaction rolls back and the warehouse is unchanged.

Delete-then-insert per month makes the load idempotent (decision D10).
"""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.config import QUARANTINE_PREFIX, SILVER_PREFIX, Settings
from pipeline.db import SQL_DIR, connect, run_sql_file
from pipeline.schema import QUARANTINE_SCHEMA, SILVER_SCHEMA

log = logging.getLogger("pipeline.gold")


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

    log.info("gold   %s | facts %s | quarantine %s", month, f"{facts:,}", f"{quarantined:,}")
    return {"fact_rows": facts, "quarantine_rows": quarantined}
