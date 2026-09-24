"""Pure transformation functions: DataFrame in, DataFrame out.

No I/O happens here, which is what makes these functions easy to unit-test with a few
hand-made rows (tests/unit). The jobs in silver.py and gold.py do the reading and writing.

Flow for one monthly batch:
    standardize -> drop_blank_rows -> add_typed_columns -> deduplicate
    -> apply_quality_rules -> split into silver / quarantine
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from pipeline.quality import (
    ERROR_RULES,
    NULL_TOKENS,
    PAYMENT_TYPES,
    STATUS_GROUPS,
    WARN_RULES,
    lookup,
)
from pipeline.schema import AMOUNT, MONEY, QUARANTINE_COLUMNS, RAW_COLUMNS, SILVER_SCHEMA

STD = [f"std_{name}" for name in RAW_COLUMNS]
INTEGER_PATTERN = r"^-?[0-9]+$"
DECIMAL_PATTERN = r"^-?[0-9]+(\.[0-9]+)?$"


def standardize(df: DataFrame) -> DataFrame:
    """Add std_<name> columns: trimmed, line breaks removed, and empty strings, \\N and Excel
    errors turned into NULL.

    The raw_<name> columns are left untouched, so quarantine can show values exactly as received.
    One select() for all 26 columns: a loop of withColumn() calls would nest 26 projections
    and make Spark's query plan much bigger than it needs to be.
    """
    std = []
    for name in RAW_COLUMNS:
        # Line breaks inside a value (finding F8) become a single space, then trim.
        trimmed = F.trim(F.regexp_replace(F.col(f"raw_{name}"), r"\s*[\r\n]+\s*", " "))
        std.append(F.when(trimmed.isin(*NULL_TOKENS), None).otherwise(trimmed).alias(f"std_{name}"))
    return df.select("*", *std)


def drop_blank_rows(df: DataFrame) -> DataFrame:
    """Remove rows where every column is empty (Excel's padding rows, finding F1)."""
    return df.filter(F.coalesce(*[F.col(name) for name in STD]).isNotNull())


def _integer(name: str, spark_type: str) -> Column:
    # Explicit pattern check: Spark would silently turn "1.5" into 1 if cast directly.
    col = F.col(name)
    return F.when(col.rlike(INTEGER_PATTERN), col.cast(spark_type))


def _decimal(name: str, decimal_type) -> Column:
    col = F.regexp_replace(F.col(name), ",", "")
    return F.when(col.rlike(DECIMAL_PATTERN), col.cast(decimal_type))


def add_typed_columns(df: DataFrame) -> DataFrame:
    """Add cleaned, typed columns next to the raw strings (raw values are kept for quarantine)."""
    status = F.lower(F.col("std_status"))
    payment_method = F.col("std_payment_method")
    order_date = F.to_date(F.col("std_created_at"), "M/d/yyyy")
    typed = df.select(
        "*",
        _integer("std_item_id", "long").alias("item_id"),
        F.col("std_increment_id").alias("order_id"),
        _integer("std_customer_id", "long").alias("customer_id"),
        F.col("std_sku").alias("sku"),
        F.col("std_category_name_1").alias("category_raw"),
        F.coalesce(F.col("std_category_name_1"), F.lit("Unknown")).alias("category"),
        status.alias("status"),
        lookup(STATUS_GROUPS, status).alias("status_group"),
        payment_method.alias("payment_method_raw"),
        F.coalesce(payment_method, F.lit("unknown")).alias("payment_method"),
        F.coalesce(lookup(PAYMENT_TYPES, F.lower(payment_method)), F.lit("Other")).alias("payment_type"),
        order_date.alias("order_date"),
        F.date_format(order_date, "yyyy-MM").alias("order_month"),
        _integer("std_qty_ordered", "int").alias("qty"),
        _decimal("std_price", MONEY).alias("unit_price"),
        _decimal("std_discount_amount", MONEY).alias("discount_amount"),
        _decimal("std_grand_total", AMOUNT).alias("order_grand_total"),
    )
    # Revenue is computed per item, never from grand_total, which is order-level (D18).
    gross = (F.col("unit_price") * F.col("qty")).cast(AMOUNT)
    return typed.select(
        "*",
        gross.alias("gross_amount"),
        (gross - F.col("discount_amount")).cast(AMOUNT).alias("net_amount"),
        F.coalesce(F.col("unit_price") == 0, F.lit(False)).alias("is_zero_price"),
    )


def deduplicate(df: DataFrame, order_col: str = "_row_order") -> DataFrame:
    """Keep one row per item_id: the last occurrence wins.

    Rows without an item_id are never treated as duplicates of each other; they are kept
    so the MISSING_ITEM_ID rule can quarantine them.
    """
    latest_first = Window.partitionBy("item_id").orderBy(F.col(order_col).desc())
    ranked = df.withColumn(
        "_rn", F.when(F.col("item_id").isNull(), F.lit(1)).otherwise(F.row_number().over(latest_first))
    )
    return ranked.filter(F.col("_rn") == 1).drop("_rn")


def _codes(rules, batch_month: str) -> Column:
    """Array of the codes of every rule the row fails (empty array = passes all)."""
    return F.array_compact(
        F.array(*[F.when(rule.fails(batch_month), F.lit(rule.code)) for rule in rules])
    )


def apply_quality_rules(df: DataFrame, batch_month: str) -> DataFrame:
    return df.withColumn("dq_errors", _codes(ERROR_RULES, batch_month)).withColumn(
        "dq_warnings", _codes(WARN_RULES, batch_month)
    )


def prepare_batch(raw_df: DataFrame, batch_month: str) -> DataFrame:
    """Everything up to (but not including) the silver/quarantine split."""
    typed = add_typed_columns(drop_blank_rows(standardize(raw_df)))
    return apply_quality_rules(deduplicate(typed), batch_month)


def valid_rows(df: DataFrame) -> DataFrame:
    return df.filter(F.size("dq_errors") == 0)


def quarantined_rows(df: DataFrame) -> DataFrame:
    return df.filter(F.size("dq_errors") > 0)


def to_silver(df: DataFrame) -> DataFrame:
    """Select the silver columns, in the silver schema's order and types."""
    columns = []
    for field in SILVER_SCHEMA.fields:
        if field.name == "dq_warnings":
            warnings = F.when(F.size("dq_warnings") > 0, F.concat_ws(",", "dq_warnings"))
            columns.append(warnings.alias("dq_warnings"))
        else:
            columns.append(F.col(field.name).cast(field.dataType).alias(field.name))
    return df.select(*columns)


def to_quarantine(df: DataFrame) -> DataFrame:
    """Raw values exactly as received, plus every failed rule."""
    return df.select(
        *[F.col(f"raw_{name}").alias(name) for name in QUARANTINE_COLUMNS],
        F.concat_ws(",", "dq_errors").alias("error_codes"),
        F.col("source_file"),
    )
