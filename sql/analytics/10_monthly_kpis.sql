-- Monthly KPIs with month-over-month growth and a running total.
-- Definitions:
--   ordered_value    = net amount of every item ordered, whatever happened to it afterwards
--   net_revenue      = net amount of COMPLETED items only
--   open_order_value = net amount of items still IN PROGRESS when the data was exported.
--                      From May 2018 almost nothing is 'complete' yet, so net_revenue alone
--                      would look like a collapse in sales (see docs/DATA_PROFILE.md, F11).
-- Order counts use COUNT(DISTINCT order_id): the fact table's grain is the item, not the order.
CREATE OR REPLACE VIEW analytics.monthly_kpis AS
WITH monthly AS (
    SELECT
        order_month,
        COUNT(DISTINCT order_id)                                                AS orders,
        COUNT(*)                                                                AS items,
        COUNT(DISTINCT customer_key)                                            AS customers,
        SUM(net_amount)                                                         AS ordered_value,
        COALESCE(SUM(net_amount) FILTER (WHERE status_group = 'completed'), 0)  AS net_revenue,
        COALESCE(SUM(net_amount) FILTER (WHERE status_group = 'in_progress'), 0) AS open_order_value,
        COUNT(DISTINCT order_id) FILTER (WHERE status_group = 'completed')      AS completed_orders,
        COUNT(DISTINCT order_id) FILTER (WHERE status_group = 'cancelled')      AS cancelled_orders
    FROM dw.fact_order_items
    GROUP BY order_month
)
SELECT
    order_month,
    orders,
    items,
    customers,
    ordered_value,
    net_revenue,
    open_order_value,
    ROUND(net_revenue / NULLIF(completed_orders, 0), 2)                        AS avg_completed_order_value,
    ROUND(100.0 * cancelled_orders / NULLIF(orders, 0), 1)                     AS cancellation_rate_pct,
    ROUND(
        100.0 * (net_revenue - LAG(net_revenue) OVER w) / NULLIF(LAG(net_revenue) OVER w, 0), 1
    )                                                                          AS revenue_growth_pct,
    SUM(net_revenue) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_net_revenue
FROM monthly
WINDOW w AS (ORDER BY order_month);
