"""Thin helpers around the S3 API (boto3). Works with RustFS locally and AWS S3 unchanged."""
from __future__ import annotations

import json
import re

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from pipeline.config import RAW_PREFIX, Settings

MONTH_PREFIX = re.compile(rf"^{RAW_PREFIX}/ingest_month=(\d{{4}}-\d{{2}})/$")


def s3_client(settings: Settings):
    # A custom endpoint (RustFS) needs path-style URLs: http://host:9000/bucket/key
    # Keys of None make boto3 use its default credential chain, e.g. an IAM role (D29).
    config = Config(s3={"addressing_style": "path"}) if settings.s3_endpoint else None
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        config=config,
    )


def ensure_bucket(client, settings: Settings) -> bool:
    """Create the bucket if it doesn't exist. Returns True if it was created."""
    try:
        client.head_bucket(Bucket=settings.s3_bucket)
        return False
    except ClientError as error:
        if error.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            raise
    kwargs = {"Bucket": settings.s3_bucket}
    if not settings.s3_endpoint and settings.aws_region != "us-east-1":
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": settings.aws_region}
    client.create_bucket(**kwargs)
    return True


def list_raw_months(client, settings: Settings) -> list[str]:
    """Months that have a raw batch in the lake, oldest first."""
    months = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.s3_bucket, Prefix=f"{RAW_PREFIX}/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            match = MONTH_PREFIX.match(prefix["Prefix"])
            if match:
                months.append(match.group(1))
    return sorted(months)


def put_json(client, settings: Settings, key: str, payload: dict) -> None:
    body = json.dumps(payload, indent=2, sort_keys=True).encode()
    client.put_object(Bucket=settings.s3_bucket, Key=key, Body=body, ContentType="application/json")


def get_json(client, settings: Settings, key: str) -> dict | None:
    """Read a JSON object from the lake, or None if it doesn't exist."""
    try:
        body = client.get_object(Bucket=settings.s3_bucket, Key=key)["Body"].read()
    except ClientError as error:
        if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(body)
