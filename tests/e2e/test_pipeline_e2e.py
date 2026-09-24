"""End-to-end: fixture CSV -> lake -> silver -> warehouse, against the real RustFS and Postgres.

Uses its own bucket and database, so it never touches the real data. Run with `make test-e2e`.

The fixture (tests/fixtures/sample_orders.csv) contains one example of each problem found
while profiling the real data, so every expected number below is known exactly:

  July 2016 (9 rows): 4 clean, 4 quarantined (blank sku, discount > line value, #N/A customer,
                      \\N status), 1 exact duplicate; plus 1 blank Excel line
  Aug 2016 (3 rows):  2 clean, 1 quarantined (negative discount); plus 2 blank Excel lines
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal

import pytest

pytestmark = pytest.mark.skipif(not os.environ.get("PGHOST"), reason="needs the Docker Compose services")

E2E_ENV = {
    "S3_BUCKET": "pk-ecommerce-e2e",
    "PGDATABASE": "warehouse_e2e",
    "RAW_FILE": "tests/fixtures/sample_orders.csv",
    "SPARK_DRIVER_MEMORY": "1g",
}


@pytest.fixture(scope="module")
def pipeline_env():
    from pipeline.config import get_settings
    from pipeline.db import connect
    from pipeline.storage import s3_client

    previous = {key: os.environ.get(key) for key in E2E_ENV}
    os.environ.update(E2E_ENV)
    settings = get_settings()
    yield settings

    # Clean up: drop the test database and empty + delete the test bucket.
    with connect(settings, dbname="postgres", autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {E2E_ENV['PGDATABASE']} WITH (FORCE)")
    client = s3_client(settings)
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=settings.s3_bucket):
        for obj in page.get("Contents", []):
            client.delete_object(Bucket=settings.s3_bucket, Key=obj["Key"])
    client.delete_bucket(Bucket=settings.s3_bucket)
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def scalar(settings, query, params=None):
    from pipeline.db import connect

    with connect(settings) as conn:
        return conn.execute(query, params).fetchone()[0]


def warehouse_counts(settings):
    return {
        "facts": scalar(settings, "SELECT count(*) FROM dw.fact_order_items"),
        "quarantine": scalar(settings, "SELECT count(*) FROM dq.quarantine"),
        "customers": scalar(settings, "SELECT count(*) FROM dw.dim_customer"),
        "products": scalar(settings, "SELECT count(*) FROM dw.dim_product"),
        "payment_methods": scalar(settings, "SELECT count(*) FROM dw.dim_payment_method"),
    }


def test_full_pipeline(pipeline_env, monkeypatch):
    from pipeline import ingest, init, run

    settings = pipeline_env
    init.main()
    monkeypatch.setattr(sys, "argv", ["ingest"])
    ingest.main()
    assert run.main(["--all"]) == 0

    assert warehouse_counts(settings) == {
        "facts": 6, "quarantine": 5, "customers": 4, "products": 4, "payment_methods": 5,
    }

    runs = {
        row[0]: row[1]
        for row in _rows(settings, "SELECT order_month, metrics FROM dq.pipeline_runs WHERE step = 'silver'")
    }
    assert runs["2016-07"]["rows_read"] == 9
    assert runs["2016-07"]["silver_rows"] == 4
    assert runs["2016-07"]["quarantined_rows"] == 4
    assert runs["2016-07"]["duplicates_removed"] == 1
    assert runs["2016-08"]["quarantined_rows"] == 1

    errors = dict(_rows(settings, "SELECT error_code, rows_affected FROM analytics.dq_error_summary"))
    assert errors == {"MISSING_SKU": 1, "DISCOUNT_EXCEEDS_LINE": 1, "INVALID_CUSTOMER_ID": 1,
                      "MISSING_STATUS": 1, "NEGATIVE_DISCOUNT": 1}
    warnings = dict(_rows(settings, "SELECT warning_code, rows_affected FROM analytics.dq_warning_summary"))
    assert warnings == {"ZERO_PRICE": 1, "UNKNOWN_CATEGORY": 1}

    # The quarantine keeps the raw value exactly as received.
    assert scalar(settings, "SELECT customer_id FROM dq.quarantine WHERE item_id = '7'") == "#N/A"

    # Revenue is per item: order 100000001 has grand_total 1500 on BOTH rows, but its
    # revenue is 1000 + 500 = 1500, not 3000.
    assert scalar(settings, "SELECT sum(net_amount) FROM dw.fact_order_items WHERE order_id = '100000001'") \
        == Decimal("1500.00")
    kpis = {row[0]: row[1:] for row in _rows(
        settings, "SELECT order_month, orders, net_revenue, cancellation_rate_pct FROM analytics.monthly_kpis")}
    assert kpis["2016-07"] == (3, Decimal("1500.00"), Decimal("33.3"))
    assert kpis["2016-08"] == (2, Decimal("900.00"), Decimal("0.0"))

    # Dimensions
    assert scalar(settings, "SELECT category FROM dw.dim_product WHERE sku = 'SKU-C'") == "Unknown"
    assert scalar(settings, "SELECT last_order_date::text FROM dw.dim_customer WHERE customer_id = 1") \
        == "2016-08-01"


def test_rerunning_a_month_changes_nothing(pipeline_env):
    from pipeline import run

    settings = pipeline_env
    before = warehouse_counts(settings)
    revenue_before = scalar(settings, "SELECT sum(net_amount) FROM dw.fact_order_items")
    assert run.main(["--month", "2016-07"]) == 0
    assert warehouse_counts(settings) == before
    assert scalar(settings, "SELECT sum(net_amount) FROM dw.fact_order_items") == revenue_before
    # ...but the rerun itself is recorded.
    assert scalar(settings, "SELECT count(*) FROM dq.pipeline_runs WHERE order_month = '2016-07'") == 4


def _rows(settings, query):
    from pipeline.db import connect

    with connect(settings) as conn:
        return conn.execute(query).fetchall()
