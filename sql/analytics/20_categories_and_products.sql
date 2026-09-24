-- Revenue share, rank and cancellation rate per category.
CREATE OR REPLACE VIEW analytics.category_performance AS
WITH by_category AS (
    SELECT
        p.category,
        COUNT(*)                                                                  AS items,
        COALESCE(SUM(f.net_amount) FILTER (WHERE f.status_group = 'completed'), 0) AS net_revenue,
        COUNT(*) FILTER (WHERE f.status_group = 'cancelled')                      AS cancelled_items
    FROM dw.fact_order_items AS f
    JOIN dw.dim_product AS p USING (product_key)
    GROUP BY p.category
)
SELECT
    category,
    items,
    net_revenue,
    ROUND(100.0 * net_revenue / NULLIF(SUM(net_revenue) OVER (), 0), 1) AS revenue_share_pct,
    RANK() OVER (ORDER BY net_revenue DESC)                             AS revenue_rank,
    ROUND(100.0 * cancelled_items / items, 1)                           AS item_cancellation_rate_pct
FROM by_category;

-- Top 5 products in every category: the classic "top N per group" with ROW_NUMBER.
CREATE OR REPLACE VIEW analytics.top_products_by_category AS
WITH product_revenue AS (
    SELECT p.category, p.sku, SUM(f.qty) AS units, SUM(f.net_amount) AS net_revenue
    FROM dw.fact_order_items AS f
    JOIN dw.dim_product AS p USING (product_key)
    WHERE f.status_group = 'completed'
    GROUP BY p.category, p.sku
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY net_revenue DESC, sku) AS rank_in_category
    FROM product_revenue
)
SELECT category, rank_in_category, sku, units, net_revenue
FROM ranked
WHERE rank_in_category <= 5;
