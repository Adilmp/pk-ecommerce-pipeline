"""Step 2: raw -> silver. Clean, type, de-duplicate and validate one monthly batch.

Clean rows  -> s3://<bucket>/silver/order_items/order_month=YYYY-MM/      (Parquet)
Failed rows -> s3://<bucket>/quarantine/order_items/order_month=YYYY-MM/  (Parquet + reasons)

Each run replaces exactly that month's folders, so running a month twice gives the same
result as running it once (idempotency, decision D10).
"""
from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline import transform
from pipeline.config import MANIFEST_PREFIX, QUARANTINE_PREFIX, SILVER_PREFIX, Settings, raw_key
from pipeline.schema import RAW_SCHEMA
from pipeline.storage import get_json, s3_client

log = logging.getLogger("pipeline.silver")


class ReconciliationError(Exception):
    """Row counts don't add up: rows were lost or invented somewhere."""


def read_raw_batch(spark: SparkSession, settings: Settings, month: str):
    return (
        spark.read.schema(RAW_SCHEMA)  # explicit schema, never inferSchema (D8)
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        # 11 rows have line breaks inside quoted SKUs (finding F8). Without multiLine,
        # Spark would split each of those rows into two broken records.
        .option("multiLine", True)
        .csv(settings.lake_path(raw_key(month)))
        .withColumn("source_file", F.input_file_name())
        # Original order of the rows in the file; used so "the last copy wins" on duplicates.
        .withColumn("_row_order", F.monotonically_increasing_id())
    )


def count_codes(df, column: str) -> dict[str, int]:
    rows = df.select(F.explode(column).alias("code")).groupBy("code").count().collect()
    return {row["code"]: row["count"] for row in sorted(rows, key=lambda r: r["code"])}


def process_month(spark: SparkSession, settings: Settings, month: str) -> dict:
    raw = read_raw_batch(spark, settings, month)
    typed = transform.add_typed_columns(transform.drop_blank_rows(transform.standardize(raw)))
    # The batch is used by several actions (counts, two writes). Caching it means the CSV is
    # read and transformed once, not once per action.
    typed.cache()
    rows_read = typed.count()

    # Check 1: Spark read exactly the rows that ingest wrote (catches CSV parsing problems).
    # No manifest means nothing to check against, so that fails loudly too.
    manifest = get_json(s3_client(settings), settings, f"{MANIFEST_PREFIX}/orders_{month}.json")
    if manifest is None:
        raise ReconciliationError(f"{month}: no ingest manifest, so the row count can't be checked. "
                                  "Run `python -m pipeline.ingest` first.")
    if manifest["rows"] != rows_read:
        raise ReconciliationError(
            f"{month}: ingest wrote {manifest['rows']} rows but Spark read {rows_read}. "
            "Check the CSV parsing options (quotes, line breaks)."
        )

    batch = transform.apply_quality_rules(transform.deduplicate(typed), month).cache()
    valid = transform.valid_rows(batch)
    quarantined = transform.quarantined_rows(batch)

    metrics = {
        "manifest_rows": manifest["rows"],
        "rows_read": rows_read,
        "duplicates_removed": rows_read - batch.count(),
        "silver_rows": valid.count(),
        "quarantined_rows": quarantined.count(),
        "error_counts": count_codes(quarantined, "dq_errors"),
        "warning_counts": count_codes(valid, "dq_warnings"),
    }

    # Check 2: nothing lost silently. Every row read is clean, quarantined or a duplicate.
    accounted = metrics["silver_rows"] + metrics["quarantined_rows"] + metrics["duplicates_removed"]
    if accounted != rows_read:
        raise ReconciliationError(f"{month}: read {rows_read} rows but accounted for {accounted}")

    # One file per month: at this size, more files would only add small-file overhead (D6).
    (transform.to_silver(valid).coalesce(1).write.mode("overwrite")
        .parquet(settings.lake_path(SILVER_PREFIX, f"order_month={month}")))
    (transform.to_quarantine(quarantined).coalesce(1).write.mode("overwrite")
        .parquet(settings.lake_path(QUARANTINE_PREFIX, f"order_month={month}")))

    batch.unpersist()
    typed.unpersist()
    log.info(
        "silver %s | read %s | clean %s | quarantined %s | duplicates %s",
        month, f"{rows_read:,}", f"{metrics['silver_rows']:,}",
        f"{metrics['quarantined_rows']:,}", f"{metrics['duplicates_removed']:,}",
    )
    return metrics
