"""Builds the SparkSession, including how Spark reaches the S3 data lake."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from pipeline.config import Settings


def get_spark(settings: Settings, app_name: str = "pk-ecommerce-pipeline") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", settings.spark_driver_memory)
        # A month is at most ~84k rows. The default of 200 shuffle partitions would create
        # 200 tiny tasks for every join/groupBy, so a small number is faster here.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        # Unparseable dates become NULL (and are caught by a data-quality rule) instead of
        # raising Spark 2-vs-3 parser upgrade errors.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.showConsoleProgress", "false")
        # The cleaning step derives ~50 columns and then filters. Spark's constraint
        # propagation (inferring extra filters from every derived column) grows exponentially
        # with that shape and ran out of memory while *planning* a one-row batch. It adds
        # nothing for this workload, so it's off. See docs/DECISIONS.md (D24).
        .config("spark.sql.constraintPropagation.enabled", "false")
        # --- S3 access through the s3a:// filesystem (hadoop-aws) ---
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
    )
    if settings.s3_endpoint:
        # Local S3-compatible server (RustFS): custom endpoint + path-style URLs.
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", settings.s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(settings.s3_endpoint.startswith("https")).lower(),
            )
        )
    else:
        # Real AWS S3: regional endpoint.
        builder = builder.config(
            "spark.hadoop.fs.s3a.endpoint", f"s3.{settings.aws_region}.amazonaws.com"
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
