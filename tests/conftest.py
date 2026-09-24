import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.schema import RAW_COLUMNS, RAW_SCHEMA


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.constraintPropagation.enabled", "false")  # same as production (spark.py)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# A row that passes every rule. Tests override only the fields they care about.
VALID_ROW = {
    "item_id": "1", "status": "complete", "created_at": "7/15/2016", "sku": "SKU-1",
    "price": "100", "qty_ordered": "2", "grand_total": "200", "increment_id": "100000001",
    "category_name_1": "Books", "sales_commission_code": "\\N", "discount_amount": "0",
    "payment_method": "cod", "customer_id": "10",
}


@pytest.fixture
def make_raw(spark):
    """Build a raw batch DataFrame (all strings, raw_ prefixed) from a list of overrides."""

    def build(*overrides: dict):
        rows = []
        for override in overrides:
            values = {**VALID_ROW, **override}
            rows.append(tuple(values.get(name, "") for name in RAW_COLUMNS))
        return (
            spark.createDataFrame(rows, RAW_SCHEMA)
            .withColumn("source_file", F.lit("test.csv"))
            .withColumn("_row_order", F.monotonically_increasing_id())
        )

    return build
