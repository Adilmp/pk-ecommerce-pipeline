from datetime import date
from decimal import Decimal

import pytest

from pipeline import transform
from pipeline.quality import RULES
from pipeline.schema import SILVER_SCHEMA

BATCH = "2016-07"


def prepared(make_raw, *overrides):
    return transform.prepare_batch(make_raw(*overrides), BATCH)


# ---------- standardize / blank rows ----------

def test_standardize_turns_empty_values_and_excel_errors_into_null(make_raw):
    df = transform.standardize(make_raw(
        {"sku": "  "}, {"sku": "\\N"}, {"sku": "#N/A"}, {"sku": "#REF!"}, {"sku": "  SKU-9 "},
    ))
    rows = df.orderBy("_row_order").collect()
    assert [r["std_sku"] for r in rows] == [None, None, None, None, "SKU-9"]
    # The raw value is never modified: quarantine must show exactly what arrived.
    assert rows[2]["raw_sku"] == "#N/A"


def test_blank_rows_are_dropped(make_raw):
    blank = {name: "" for name in ["item_id", "status", "created_at", "sku", "price", "qty_ordered",
                                    "grand_total", "increment_id", "category_name_1",
                                    "sales_commission_code", "discount_amount", "payment_method",
                                    "customer_id"]}
    df = transform.drop_blank_rows(transform.standardize(make_raw({}, blank, blank)))
    assert df.count() == 1


# ---------- typing ----------

def test_typed_columns(make_raw):
    row = transform.add_typed_columns(transform.standardize(make_raw(
        {"price": "82.5", "qty_ordered": "3", "created_at": "12/31/2017", "customer_id": "42"}
    ))).first()
    assert row["unit_price"] == Decimal("82.50")
    assert row["qty"] == 3
    assert row["order_date"] == date(2017, 12, 31)
    assert row["order_month"] == "2017-12"
    assert row["customer_id"] == 42


def test_non_integer_quantity_is_not_silently_truncated(make_raw):
    # A plain cast would turn "1.5" into 1. The pipeline must treat it as invalid instead.
    row = transform.add_typed_columns(transform.standardize(make_raw({"qty_ordered": "1.5"}))).first()
    assert row["qty"] is None


def test_revenue_is_computed_per_item_not_from_grand_total(make_raw):
    # grand_total is the ORDER total repeated on every item (finding F3).
    row = transform.add_typed_columns(transform.standardize(make_raw(
        {"price": "420", "qty_ordered": "1", "discount_amount": "20", "grand_total": "1270"}
    ))).first()
    assert row["gross_amount"] == Decimal("420.00")
    assert row["net_amount"] == Decimal("400.00")


@pytest.mark.parametrize("status, group", [
    ("complete", "completed"), ("canceled", "cancelled"), ("fraud", "cancelled"),
    ("closed", "refunded"), ("order_refunded", "refunded"), ("received", "in_progress"),
    ("COMPLETE", "completed"),
])
def test_status_groups(make_raw, status, group):
    row = transform.add_typed_columns(transform.standardize(make_raw({"status": status}))).first()
    assert row["status_group"] == group


@pytest.mark.parametrize("method, payment_type", [
    ("cod", "Cash on delivery"), ("Easypay", "Prepaid"), ("jazzvoucher", "Prepaid"),
    ("customercredit", "Store credit / internal"), ("brand_new_wallet", "Other"),
])
def test_payment_types(make_raw, method, payment_type):
    row = transform.add_typed_columns(transform.standardize(make_raw({"payment_method": method}))).first()
    assert row["payment_type"] == payment_type
    assert row["payment_method"] == method


# ---------- de-duplication ----------

def test_duplicates_keep_the_last_occurrence(make_raw):
    df = prepared(make_raw, {"item_id": "5", "price": "100"}, {"item_id": "5", "price": "120"})
    rows = df.collect()
    assert len(rows) == 1
    assert rows[0]["unit_price"] == Decimal("120.00")


def test_rows_without_item_id_are_not_deduplicated_away(make_raw):
    df = prepared(make_raw, {"item_id": ""}, {"item_id": ""})
    assert df.count() == 2
    assert all(r["dq_errors"] == ["MISSING_ITEM_ID"] for r in df.collect())


# ---------- data-quality rules ----------

def test_valid_row_passes_every_rule(make_raw):
    row = prepared(make_raw, {}).first()
    assert row["dq_errors"] == []
    assert row["dq_warnings"] == []


@pytest.mark.parametrize("override, code", [
    ({"item_id": ""}, "MISSING_ITEM_ID"),
    ({"increment_id": ""}, "MISSING_ORDER_ID"),
    ({"customer_id": "#N/A"}, "INVALID_CUSTOMER_ID"),
    ({"sku": ""}, "MISSING_SKU"),
    ({"created_at": "2016-07-15"}, "INVALID_DATE"),
    ({"created_at": "8/1/2016"}, "DATE_OUTSIDE_BATCH"),
    ({"status": "\\N"}, "MISSING_STATUS"),
    ({"status": "teleported"}, "UNKNOWN_STATUS"),
    ({"price": "abc"}, "INVALID_NUMBER"),
    ({"qty_ordered": "0"}, "NON_POSITIVE_QTY"),
    ({"price": "-1"}, "NEGATIVE_PRICE"),
    ({"discount_amount": "-5"}, "NEGATIVE_DISCOUNT"),
    ({"price": "100", "qty_ordered": "1", "discount_amount": "150"}, "DISCOUNT_EXCEEDS_LINE"),
])
def test_error_rules(make_raw, override, code):
    row = prepared(make_raw, override).first()
    assert code in row["dq_errors"]


def test_every_error_rule_has_a_test():
    tested = {"MISSING_ITEM_ID", "MISSING_ORDER_ID", "INVALID_CUSTOMER_ID", "MISSING_SKU",
              "INVALID_DATE", "DATE_OUTSIDE_BATCH", "MISSING_STATUS", "UNKNOWN_STATUS",
              "INVALID_NUMBER", "NON_POSITIVE_QTY", "NEGATIVE_PRICE", "NEGATIVE_DISCOUNT",
              "DISCOUNT_EXCEEDS_LINE"}
    assert {r.code for r in RULES if r.severity == "ERROR"} == tested


def test_discount_equal_to_line_value_is_allowed(make_raw):
    row = prepared(make_raw, {"price": "100", "qty_ordered": "1", "discount_amount": "100"}).first()
    assert row["dq_errors"] == []


def test_warnings_keep_the_row(make_raw):
    row = prepared(make_raw, {"price": "0", "category_name_1": "\\N", "payment_method": ""}).first()
    assert row["dq_errors"] == []
    assert set(row["dq_warnings"]) == {"ZERO_PRICE", "UNKNOWN_CATEGORY", "UNKNOWN_PAYMENT_METHOD"}
    assert row["category"] == "Unknown"
    assert row["payment_method"] == "unknown"


def test_a_row_can_fail_several_rules(make_raw):
    row = prepared(make_raw, {"sku": "", "customer_id": "#N/A"}).first()
    assert set(row["dq_errors"]) == {"MISSING_SKU", "INVALID_CUSTOMER_ID"}


# ---------- outputs ----------

def test_split_accounts_for_every_row(make_raw):
    raw = make_raw({"item_id": "1"}, {"item_id": "2", "sku": ""}, {"item_id": "3", "price": "0"},
                   {"item_id": "3", "price": "0"})
    batch = transform.prepare_batch(raw, BATCH)
    valid, quarantined = transform.valid_rows(batch), transform.quarantined_rows(batch)
    duplicates = raw.count() - batch.count()
    assert (valid.count(), quarantined.count(), duplicates) == (2, 1, 1)
    assert valid.count() + quarantined.count() + duplicates == raw.count()


def test_silver_output_matches_the_silver_schema(make_raw):
    silver = transform.to_silver(transform.valid_rows(prepared(make_raw, {}, {"item_id": "2", "price": "0"})))
    assert [f.name for f in silver.schema.fields] == [f.name for f in SILVER_SCHEMA.fields]
    warnings = sorted(r["dq_warnings"] or "" for r in silver.collect())
    assert warnings == ["", "ZERO_PRICE"]


def test_quarantine_keeps_raw_values_and_reasons(make_raw):
    quarantined = transform.to_quarantine(
        transform.quarantined_rows(prepared(make_raw, {"customer_id": "#N/A", "sku": ""}))
    ).first()
    assert quarantined["customer_id"] == "#N/A"
    assert quarantined["error_codes"] == "INVALID_CUSTOMER_ID,MISSING_SKU"
