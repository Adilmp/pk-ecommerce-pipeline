-- How orders end, by payment method. Status is the same for every item of an order (verified
-- during profiling), so items are first rolled up to one row per order.
CREATE OR REPLACE VIEW analytics.order_outcomes AS
SELECT
    f.order_id,
    f.payment_method_key,
    MIN(f.status_group) AS status_group,
    SUM(f.net_amount)   AS order_value
FROM dw.fact_order_items AS f
GROUP BY f.order_id, f.payment_method_key;

CREATE OR REPLACE VIEW analytics.payment_method_outcomes AS
SELECT
    pm.payment_type,
    pm.payment_method,
    COUNT(*)                                                                         AS orders,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.status_group = 'completed')   / COUNT(*), 1) AS completed_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.status_group = 'cancelled')   / COUNT(*), 1) AS cancelled_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.status_group = 'refunded')    / COUNT(*), 1) AS refunded_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.status_group = 'in_progress') / COUNT(*), 1) AS in_progress_pct,
    ROUND(AVG(o.order_value), 2)                                                     AS avg_order_value
FROM analytics.order_outcomes AS o
JOIN dw.dim_payment_method AS pm USING (payment_method_key)
GROUP BY pm.payment_type, pm.payment_method;

-- The headline comparison for Pakistani e-commerce: cash on delivery vs prepaid.
CREATE OR REPLACE VIEW analytics.payment_type_outcomes AS
SELECT
    pm.payment_type,
    COUNT(*)                                                                         AS orders,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)                               AS share_of_orders_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.status_group = 'completed')   / COUNT(*), 1) AS completed_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.status_group = 'cancelled')   / COUNT(*), 1) AS cancelled_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.status_group = 'refunded')    / COUNT(*), 1) AS refunded_pct,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.status_group = 'in_progress') / COUNT(*), 1) AS in_progress_pct,
    ROUND(AVG(o.order_value), 2)                                                     AS avg_order_value
FROM analytics.order_outcomes AS o
JOIN dw.dim_payment_method AS pm USING (payment_method_key)
GROUP BY pm.payment_type;
