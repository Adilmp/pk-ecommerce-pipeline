"""All configuration comes from environment variables (decision D14).

The same code runs against local RustFS or real AWS S3: only the environment changes.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Where each layer lives inside the bucket (decision D3).
RAW_PREFIX = "raw/orders"
MANIFEST_PREFIX = "raw/_manifests"
SILVER_PREFIX = "silver/order_items"
QUARANTINE_PREFIX = "quarantine/order_items"
RAW_FILE_NAME = "orders.csv"


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable {name} (see .env.example)")
    return value


@dataclass(frozen=True)
class Settings:
    s3_endpoint: str | None
    s3_bucket: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_database: str
    raw_file: Path
    spark_driver_memory: str

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"

    def lake_path(self, *parts: str) -> str:
        """s3a:// path that Spark uses to read and write the lake."""
        return "/".join([f"s3a://{self.s3_bucket}", *parts])


def raw_key(month: str) -> str:
    """Object key of one month's raw batch, e.g. raw/orders/ingest_month=2017-03/orders.csv."""
    return f"{RAW_PREFIX}/ingest_month={month}/{RAW_FILE_NAME}"


def get_settings() -> Settings:
    return Settings(
        s3_endpoint=os.environ.get("S3_ENDPOINT") or None,  # empty -> real AWS S3
        s3_bucket=_require("S3_BUCKET"),
        aws_access_key_id=_require("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_require("AWS_SECRET_ACCESS_KEY"),
        aws_region=os.environ.get("AWS_REGION", "us-east-1"),
        pg_host=_require("PGHOST"),
        pg_port=int(os.environ.get("PGPORT", "5432")),
        pg_user=_require("PGUSER"),
        pg_password=_require("PGPASSWORD"),
        pg_database=_require("PGDATABASE"),
        raw_file=Path(os.environ.get("RAW_FILE", "data/raw/pakistan_ecommerce.csv")),
        spark_driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "2g"),
    )
