"""Schemas: the contract between the source file and the pipeline (decision D8).

Raw is read with an explicit schema where every column is a string. Types are then applied
column by column in `transform.py`, so a value that can't be cast is *caught* by a
data-quality rule instead of silently becoming NULL at read time.
"""
from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# The exact header of the source file. Ingest refuses a file whose header differs, so a
# change in the source fails loudly at the door instead of corrupting data downstream.
SOURCE_HEADER = [
    "item_id", "status", "created_at", "sku", "price", "qty_ordered", "grand_total",
    "increment_id", "category_name_1", "sales_commission_code", "discount_amount",
    "payment_method", "Working Date", "BI Status", " MV ", "Year", "Month", "Customer Since",
    "M-Y", "FY", "Customer ID", "", "", "", "", "",
]

# Clean, positional names for the same 26 columns.
RAW_COLUMNS = [
    "item_id", "status", "created_at", "sku", "price", "qty_ordered", "grand_total",
    "increment_id", "category_name_1", "sales_commission_code", "discount_amount",
    "payment_method", "working_date", "bi_status", "mv", "year", "month", "customer_since",
    "m_y", "fy", "customer_id", "unnamed_21", "unnamed_22", "unnamed_23", "unnamed_24",
    "unnamed_25",
]

# Raw columns are read as raw_<name>, so the cleaned, typed columns can use the plain names.
RAW_SCHEMA = StructType([StructField(f"raw_{name}", StringType(), True) for name in RAW_COLUMNS])

# Silver: cleaned and typed, one row per order item (the grain, decision D11).
MONEY = DecimalType(12, 2)
AMOUNT = DecimalType(14, 2)

SILVER_SCHEMA = StructType([
    StructField("item_id", LongType(), False),
    StructField("order_id", StringType(), False),
    StructField("customer_id", LongType(), False),
    StructField("sku", StringType(), False),
    StructField("category", StringType(), False),
    StructField("status", StringType(), False),
    StructField("status_group", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("payment_type", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("qty", IntegerType(), False),
    StructField("unit_price", MONEY, False),
    StructField("discount_amount", MONEY, False),
    StructField("gross_amount", AMOUNT, False),
    StructField("net_amount", AMOUNT, False),
    StructField("order_grand_total", AMOUNT, True),
    StructField("is_zero_price", BooleanType(), False),
    StructField("dq_warnings", StringType(), True),
    StructField("source_file", StringType(), False),
])

# Quarantine keeps the raw (string) values exactly as received, plus the reasons.
QUARANTINE_COLUMNS = [
    "item_id", "increment_id", "customer_id", "sku", "status", "created_at", "price",
    "qty_ordered", "discount_amount", "grand_total", "category_name_1", "payment_method",
]
QUARANTINE_SCHEMA = StructType(
    [StructField(name, StringType(), True) for name in QUARANTINE_COLUMNS]
    + [
        StructField("error_codes", StringType(), False),
        StructField("source_file", StringType(), False),
    ]
)
