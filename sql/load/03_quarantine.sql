-- Replace the month's quarantined rows (idempotent, same pattern as the facts).

DELETE FROM dq.quarantine
WHERE order_month = %(month)s;

INSERT INTO dq.quarantine (
    item_id, increment_id, customer_id, sku, status, created_at, price, qty_ordered,
    discount_amount, grand_total, category_name_1, payment_method, error_codes,
    order_month, source_file
)
SELECT
    item_id, increment_id, customer_id, sku, status, created_at, price, qty_ordered,
    discount_amount, grand_total, category_name_1, payment_method, error_codes,
    order_month, source_file
FROM staging.stg_quarantine
WHERE order_month = %(month)s;
