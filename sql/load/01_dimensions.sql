-- Upsert the dimensions from the staged month (runs inside the load transaction).
-- Every statement is idempotent: loading the same month again changes nothing.

-- Customers: keep the earliest first order and the latest last order seen so far.
INSERT INTO dw.dim_customer (customer_id, first_order_date, last_order_date)
SELECT customer_id, MIN(order_date), MAX(order_date)
FROM staging.stg_order_items
GROUP BY customer_id
ON CONFLICT (customer_id) DO UPDATE SET
    first_order_date = LEAST(dw.dim_customer.first_order_date, EXCLUDED.first_order_date),
    last_order_date  = GREATEST(dw.dim_customer.last_order_date, EXCLUDED.last_order_date);

-- Products: one row per SKU. The category is the most recent known one (SCD Type 1):
-- within the month, prefer a real category over 'Unknown', then the latest sale.
INSERT INTO dw.dim_product (sku, category, first_seen_date, last_seen_date)
SELECT DISTINCT ON (sku)
    sku,
    category,
    MIN(order_date) OVER (PARTITION BY sku),
    MAX(order_date) OVER (PARTITION BY sku)
FROM staging.stg_order_items
ORDER BY sku, (category = 'Unknown'), order_date DESC, item_id DESC
ON CONFLICT (sku) DO UPDATE SET
    category = CASE
        WHEN EXCLUDED.category <> 'Unknown'
         AND (dw.dim_product.category = 'Unknown'
              OR EXCLUDED.last_seen_date >= dw.dim_product.last_seen_date)
        THEN EXCLUDED.category
        ELSE dw.dim_product.category
    END,
    first_seen_date = LEAST(dw.dim_product.first_seen_date, EXCLUDED.first_seen_date),
    last_seen_date  = GREATEST(dw.dim_product.last_seen_date, EXCLUDED.last_seen_date);

-- Payment methods: small and stable; the payment_type mapping may be updated.
INSERT INTO dw.dim_payment_method (payment_method, payment_type)
SELECT DISTINCT payment_method, payment_type
FROM staging.stg_order_items
ON CONFLICT (payment_method) DO UPDATE SET
    payment_type = EXCLUDED.payment_type;
