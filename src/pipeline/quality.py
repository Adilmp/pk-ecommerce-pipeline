"""Data-quality rules and business mappings.

Every rule is data: a code, a severity and a condition that is TRUE when a row FAILS.
ERROR rows go to quarantine; WARN rows stay in silver with a flag (decision D16).
Rules marked "profile" were found in the real data (docs/DATA_PROFILE.md); rules marked
"defensive" found nothing in this dataset but protect against future batches.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column
from pyspark.sql import functions as F

ERROR = "ERROR"
WARN = "WARN"

# Values that mean "no value": empty strings, MySQL's \N, and Excel error codes that leaked
# into the export (#REF! in BI Status, #N/A in Customer ID).
EXCEL_ERRORS = ("#N/A", "#REF!", "#VALUE!", "#DIV/0!", "#NAME?", "#NULL!", "#NUM!")
NULL_TOKENS = ("", "\\N", *EXCEL_ERRORS)


@dataclass(frozen=True)
class Rule:
    code: str
    severity: str
    origin: str  # "profile" or "defensive"
    description: str
    fails: Callable[[str], Column]  # batch_month -> Column that is True when the row fails


def c(name: str) -> Column:
    return F.col(name)


RULES: list[Rule] = [
    # ---------- ERROR: the row cannot be trusted, so it is quarantined ----------
    Rule("MISSING_ITEM_ID", ERROR, "defensive", "item_id is missing or not an integer",
         lambda m: c("item_id").isNull()),
    Rule("MISSING_ORDER_ID", ERROR, "defensive", "increment_id (order number) is missing",
         lambda m: c("order_id").isNull()),
    Rule("INVALID_CUSTOMER_ID", ERROR, "profile", "Customer ID is missing or not an integer (e.g. #N/A)",
         lambda m: c("customer_id").isNull()),
    Rule("MISSING_SKU", ERROR, "profile", "sku is blank",
         lambda m: c("sku").isNull()),
    Rule("INVALID_DATE", ERROR, "defensive", "created_at is missing or not a M/D/YYYY date",
         lambda m: c("order_date").isNull()),
    Rule("DATE_OUTSIDE_BATCH", ERROR, "defensive", "order date is not in the month of the batch",
         lambda m: c("order_date").isNotNull() & (c("order_month") != F.lit(m))),
    Rule("MISSING_STATUS", ERROR, "profile", "status is blank or \\N",
         lambda m: c("status").isNull()),
    Rule("UNKNOWN_STATUS", ERROR, "defensive", "status is not one of the known statuses",
         lambda m: c("status").isNotNull() & c("status_group").isNull()),
    Rule("INVALID_NUMBER", ERROR, "defensive", "price, qty_ordered or discount_amount is missing or not a number",
         lambda m: c("unit_price").isNull() | c("qty").isNull() | c("discount_amount").isNull()),
    Rule("NON_POSITIVE_QTY", ERROR, "defensive", "qty_ordered is zero or negative",
         lambda m: c("qty") <= 0),
    Rule("NEGATIVE_PRICE", ERROR, "defensive", "price is negative",
         lambda m: c("unit_price") < 0),
    Rule("NEGATIVE_DISCOUNT", ERROR, "profile", "discount_amount is negative",
         lambda m: c("discount_amount") < 0),
    Rule("DISCOUNT_EXCEEDS_LINE", ERROR, "profile", "discount is larger than price x qty (negative revenue)",
         lambda m: c("discount_amount") > c("unit_price") * c("qty")),
    # ---------- WARN: the row is real, so it is kept with a flag ----------
    Rule("ZERO_PRICE", WARN, "profile", "price is 0 (possibly a free gift)",
         lambda m: c("unit_price") == 0),
    Rule("UNKNOWN_CATEGORY", WARN, "profile", "category is blank or \\N (loaded as 'Unknown')",
         lambda m: c("category_raw").isNull()),
    Rule("UNKNOWN_PAYMENT_METHOD", WARN, "defensive", "payment_method is blank (loaded as 'unknown')",
         lambda m: c("payment_method_raw").isNull()),
]

ERROR_RULES = [r for r in RULES if r.severity == ERROR]
WARN_RULES = [r for r in RULES if r.severity == WARN]

# ---------- Business mappings (documented assumptions, decision D19) ----------

STATUS_GROUPS: dict[str, str] = {
    "complete": "completed",
    "received": "in_progress",
    "cod": "in_progress",
    "paid": "in_progress",
    "pending": "in_progress",
    "pending_paypal": "in_progress",
    "processing": "in_progress",
    "holded": "in_progress",
    "payment_review": "in_progress",
    "order_refunded": "refunded",
    "refund": "refunded",
    "closed": "refunded",  # Magento meaning: refunded after invoicing
    "exchange": "refunded",
    "canceled": "cancelled",
    "fraud": "cancelled",
}

# Keys are lower-cased payment_method values.
PAYMENT_TYPES: dict[str, str] = {
    "cod": "Cash on delivery",
    "cashatdoorstep": "Cash on delivery",
    "payaxis": "Prepaid",
    "easypay": "Prepaid",
    "easypay_ma": "Prepaid",
    "easypay_voucher": "Prepaid",
    "jazzwallet": "Prepaid",
    "jazzvoucher": "Prepaid",
    "bankalfalah": "Prepaid",
    "apg": "Prepaid",
    "ublcreditcard": "Prepaid",
    "mcblite": "Prepaid",
    "mygateway": "Prepaid",
    "internetbanking": "Prepaid",
    "customercredit": "Store credit / internal",
    "productcredit": "Store credit / internal",
    "marketingexpense": "Store credit / internal",
    "financesettlement": "Store credit / internal",
}


def lookup(mapping: dict[str, str], key: Column) -> Column:
    """Map a column through a Python dict inside Spark (NULL when the key is unknown)."""
    pairs = [F.lit(x) for item in mapping.items() for x in item]
    return F.create_map(*pairs)[key]
