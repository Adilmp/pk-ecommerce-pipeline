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


def _optional(name: str) -> str | None:
    return os.environ.get(name) or None


@dataclass(frozen=True)
class Settings:
    s3_endpoint: str | None
    s3_bucket: str
    aws_access_key_id: str | None  # None -> AWS's default credential chain, e.g. an IAM role (D29)
    aws_secret_access_key: str | None
    aws_region: str
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_database: str
    pg_sslmode: str | None  # e.g. "require" for a cloud database; None -> the driver's default
    raw_file: Path
    spark_driver_memory: str

    @property
    def jdbc_url(self) -> str:
        url = f"jdbc:postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"
        return f"{url}?sslmode={self.pg_sslmode}" if self.pg_sslmode else url

    def lake_path(self, *parts: str) -> str:
        """s3a:// path that Spark uses to read and write the lake."""
        return "/".join([f"s3a://{self.s3_bucket}", *parts])


def raw_key(month: str) -> str:
    """Object key of one month's raw batch, e.g. raw/orders/ingest_month=2017-03/orders.csv."""
    return f"{RAW_PREFIX}/ingest_month={month}/{RAW_FILE_NAME}"


def get_settings() -> Settings:
    access_key, secret_key = _optional("AWS_ACCESS_KEY_ID"), _optional("AWS_SECRET_ACCESS_KEY")
    if bool(access_key) != bool(secret_key):
        raise RuntimeError(
            "Set both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or neither to use an IAM role"
        )
    return Settings(
        s3_endpoint=_optional("S3_ENDPOINT"),  # empty -> real AWS S3
        s3_bucket=_require("S3_BUCKET"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_region=os.environ.get("AWS_REGION", "us-east-1"),
        pg_host=_require("PGHOST"),
        pg_port=int(os.environ.get("PGPORT", "5432")),
        pg_user=_require("PGUSER"),
        pg_password=_require("PGPASSWORD"),
        pg_database=_require("PGDATABASE"),
        pg_sslmode=_optional("PGSSLMODE"),
        raw_file=Path(os.environ.get("RAW_FILE", "data/raw/pakistan_ecommerce.csv")),
        spark_driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "2g"),
    )
