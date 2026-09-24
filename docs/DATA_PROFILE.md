# Data Profile

What the raw data actually looks like. F1–F7 were found **before** designing anything (Step 1);
F8–F12 were found **while building**, by the pipeline's own reconciliation checks and by
cross-checking its output against an independent plain-Python implementation.
Every data-quality rule and modelling choice traces back to a finding here.

**File:** `data/raw/pakistan_ecommerce.csv` (106 MB, from Kaggle)

## Headline numbers

| | |
|---|---|
| Lines in the file | 1,048,575 |
| Completely blank lines | 464,051 (44%) |
| **Real rows (order items)** | **584,524** |
| Orders (`increment_id`) | 408,782 |
| Customers | 115,327 |
| Products (SKUs) | 84,889 |
| Date range | 2016-07-01 → 2018-08-28 (**26 months**) |
| Rows per month | 8,591 → 83,928 |

## Findings

### F1 — The file was exported from Excel, and Excel padded it
1,048,575 lines is exactly Excel's maximum row count. Every line after the real data is blank.
**Action:** remove fully blank lines and *count* them. They are not records, so they are not
quarantined (see D16).

### F2 — Some columns are spreadsheet formulas, not source data
- `Unnamed: 21`–`Unnamed: 25`: 100% empty.
- `Working Date`: identical to `created_at` in 100% of rows.
- `BI Status`: an analyst's grouping of `status`; contains an Excel error, `#REF!`.
- ` MV `: price × qty formatted as text (`" 1,950 "`), with `" -   "` for zero.
- `Year`, `Month`, `M-Y`, `FY`, `Customer Since`: all derived from dates.

**Action:** keep them in raw (raw is never modified) but **don't use them**. Recompute anything
needed from `created_at`, `price`, `qty_ordered` (see D17).

### F3 — ⚠️ `grand_total` is the ORDER total, repeated on every item
In all 81,854 multi-item orders, `grand_total` is identical on every item row. Example:

| increment_id | item_id | price | grand_total |
|---|---|---|---|
| 100147458 | 211149 | 420 | 1270 |
| 100147458 | 211150 | 360 | 1270 |
| 100147458 | 211151 | 490 | 1270 |

Summing `grand_total` over item rows would count this order's revenue **three times**.
**Action:** item revenue = `price × qty_ordered − discount_amount`. `grand_total` is not a
measure at item grain (see D18).

### F4 — Discounts are mostly per item, but not always
In 73.5% of multi-item orders with a discount, the discount differs between items, so it is
an item-level value. In the other 26.5% it is the same on every item, which suggests an order
discount copied onto each row. This is why some discounts exceed the item's value (F5).

### F5 — Values that can't be right

| Problem | Rows | Severity |
|---|---:|---|
| Discount larger than price × qty (negative revenue) | 9,713 | ERROR |
| Blank `sku` | 20 | ERROR |
| `status` blank or `\N` | 19 | ERROR |
| `Customer ID` is `#N/A` (see F9) | 11 | ERROR |
| Negative discount | 3 | ERROR |
| **Rows with at least one ERROR** | **9,766 (1.67%)** | → quarantine |
| Price = 0 (possibly free gifts, e.g. "Buy 2 get 1 free") | 2,146 kept rows | WARN |
| Category `\N` or blank | 7,988 kept rows | WARN |

WARN counts are for rows that were *kept*; some zero-price or no-category rows also failed an
ERROR rule and went to quarantine instead. `grand_total` also has 76 negative values, but it
isn't used at item grain (F3).

### F6 — Status has 17 values with unclear meanings
`complete` 233,685 · `canceled` 201,249 · `received` 77,290 · `order_refunded` 59,529 ·
`refund` 8,050 · `cod` 2,859 · `paid` 1,159 · `closed` 494 · `payment_review` 57 ·
`pending` 48 · `processing` 33 · `holded` 31 · blank 15 · `fraud` 10 · `pending_paypal` 7 ·
`exchange` 4 · `\N` 4

The original `BI Status` grouping is inconsistent: it puts refunds in the same group as paid
orders. **Action:** our own documented grouping (D19).

### F7 — Messy but usable
- `sales_commission_code`: 81% missing or `\N`. Not used.
- `payment_method`: 18 values, with inconsistent casing (`Easypay`, `Easypay_MA`, `easypay_voucher`).
  Kept as-is, trimmed.
- `created_at` is `M/D/YYYY` text. Parsed to a date.
- No duplicate `item_id`s and no fully duplicate rows. We de-duplicate anyway, because a
  real pipeline can receive the same batch twice.

### F8 — Line breaks inside values (found by a reconciliation check)
11 rows have a line break inside a quoted SKU, e.g. `"peekaboo_Smurfs-Grey⏎Blue-2-4 years"`.
Python's `csv` module reads these correctly, but Spark's default CSV reader treats each line
as a record, so it split one July 2016 row into two broken rows. The pipeline noticed because
Spark read 8,838 rows where ingest's manifest said 8,837.
**Action:** read with `multiLine`, and turn line breaks inside values into a space during
standardisation (quarantine still keeps the raw value). See D25.

### F9 — A second Excel error: `#N/A` customer IDs
11 rows have `#N/A` (a failed Excel lookup) instead of a customer ID. A sale can't be
attributed to a customer, so these are quarantined (`INVALID_CUSTOMER_ID`). All Excel error
codes (`#N/A`, `#REF!`, `#VALUE!`, …) are treated as missing values.

### F10 — Discounts with three decimals
11,529 discounts have three decimals (e.g. `110.625`), probably percentage discounts. Money is
stored with 2 decimals, rounded half-up (`110.63`). An independent Python check matched the
pipeline's total net revenue to the paisa only once it used the same rounding. See D23.

### F11 — The last months aren't finished
From May 2018, almost no items are `complete` (0.1–5%, against 25–46% in the months before) while `received`
jumps to ~41%. Orders from those months hadn't reached "complete" when the data was exported
(or the status workflow changed). Net revenue (completed items only) therefore drops to
nearly zero for June–August 2018. That is not a sales collapse, so the monthly KPIs also report
`open_order_value` and the chart shows it.

### F12 — The data-quality problem starts in January 2018
Until December 2017, 0–60 rows a month are quarantined. From January 2018 it is 250–2,900 a
month, almost all `DISCOUNT_EXCEEDS_LINE`. Something changed in the source system then, most
likely an order-level discount being copied onto every item (see F4). In a real job this is
the moment to raise it with the team that owns the source.

## Data-quality rules

Implemented in [`src/pipeline/quality.py`](../src/pipeline/quality.py). **Profile** rules were
found in this data; **defensive** rules found nothing here but protect future batches.

| Code | Rule | Severity | Origin | Rows (this data) |
|---|---|---|---|---:|
| `MISSING_ITEM_ID` | item_id missing or not an integer | ERROR | defensive | 0 |
| `MISSING_ORDER_ID` | increment_id missing | ERROR | defensive | 0 |
| `INVALID_CUSTOMER_ID` | Customer ID missing or not an integer (`#N/A`) | ERROR | profile | 11 |
| `MISSING_SKU` | sku blank | ERROR | profile | 20 |
| `INVALID_DATE` | created_at missing or not M/D/YYYY | ERROR | defensive | 0 |
| `DATE_OUTSIDE_BATCH` | order date not in the batch's month (late data) | ERROR | defensive | 0 |
| `MISSING_STATUS` | status blank or `\N` | ERROR | profile | 19 |
| `UNKNOWN_STATUS` | status not in the known list | ERROR | defensive | 0 |
| `INVALID_NUMBER` | price, qty or discount missing / not a number | ERROR | defensive | 0 |
| `NON_POSITIVE_QTY` | qty ≤ 0 | ERROR | defensive | 0 |
| `NEGATIVE_PRICE` | price < 0 | ERROR | defensive | 0 |
| `NEGATIVE_DISCOUNT` | discount < 0 | ERROR | profile | 3 |
| `DISCOUNT_EXCEEDS_LINE` | discount > price × qty | ERROR | profile | 9,713 |
| `ZERO_PRICE` | price = 0 | WARN | profile | 2,146 |
| `UNKNOWN_CATEGORY` | category blank or `\N` → `Unknown` | WARN | profile | 7,988 |
| `UNKNOWN_PAYMENT_METHOD` | payment method blank → `unknown` | WARN | defensive | 0 |

A row can fail several rules; quarantine records every reason.

## Final numbers

| | Rows |
|---|---:|
| Real rows read | 584,524 |
| Clean → `dw.fact_order_items` | 574,758 |
| Quarantined → `dq.quarantine` | 9,766 |
| Duplicates removed | 0 |

Every one of these was confirmed month by month against an independent plain-Python
re-implementation of the same rules.
