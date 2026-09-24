-- Replace the month's facts: delete, then insert from staging (idempotent, decision D10).
-- Both statements run in the same transaction, so readers never see a half-loaded month.

DELETE FROM dw.fact_order_items
WHERE order_month = %(month)s;

INSERT INTO dw.fact_order_items (
    item_id, order_id, date_key, customer_key, product_key, payment_method_key,
    order_month, status, status_group, qty, unit_price, discount_amount,
    gross_amount, net_amount, is_zero_price, dq_warnings
)
SELECT
    s.item_id,
    s.order_id,
    to_char(s.order_date, 'YYYYMMDD')::integer,
    c.customer_key,
    p.product_key,
    pm.payment_method_key,
    s.order_month,
    s.status,
    s.status_group,
    s.qty,
    s.unit_price,
    s.discount_amount,
    s.gross_amount,
    s.net_amount,
    s.is_zero_price,
    s.dq_warnings
FROM staging.stg_order_items AS s
JOIN dw.dim_customer       AS c  ON c.customer_id     = s.customer_id
JOIN dw.dim_product        AS p  ON p.sku             = s.sku
JOIN dw.dim_payment_method AS pm ON pm.payment_method = s.payment_method
WHERE s.order_month = %(month)s;
