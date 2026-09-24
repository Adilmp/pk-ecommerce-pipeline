-- Customer behaviour. A customer is "active" in a month if they placed a non-cancelled order.

-- How many customers come back at all?
CREATE OR REPLACE VIEW analytics.customer_summary AS
WITH per_customer AS (
    SELECT customer_key, COUNT(DISTINCT order_id) AS orders
    FROM dw.fact_order_items
    WHERE status_group <> 'cancelled'
    GROUP BY customer_key
)
SELECT
    COUNT(*)                                                     AS customers,
    COUNT(*) FILTER (WHERE orders >= 2)                          AS repeat_customers,
    ROUND(100.0 * COUNT(*) FILTER (WHERE orders >= 2) / COUNT(*), 1) AS repeat_rate_pct,
    ROUND(AVG(orders), 2)                                        AS avg_orders_per_customer
FROM per_customer;

-- Monthly cohort retention: of the customers whose first order was in month X,
-- what share were active N months later?
-- Caveat: the data starts in July 2016, so customers who first ordered before then are
-- counted in the July 2016 cohort.
CREATE OR REPLACE VIEW analytics.customer_cohorts AS
WITH active_months AS (
    SELECT DISTINCT customer_key, to_date(order_month, 'YYYY-MM') AS month_start
    FROM dw.fact_order_items
    WHERE status_group <> 'cancelled'
),
with_cohort AS (
    SELECT
        customer_key,
        month_start,
        MIN(month_start) OVER (PARTITION BY customer_key) AS cohort_start
    FROM active_months
),
indexed AS (
    SELECT
        customer_key,
        cohort_start,
        (EXTRACT(YEAR FROM age(month_start, cohort_start)) * 12
         + EXTRACT(MONTH FROM age(month_start, cohort_start)))::integer AS months_since_first
    FROM with_cohort
)
SELECT
    to_char(cohort_start, 'YYYY-MM') AS cohort_month,
    months_since_first,
    COUNT(*)                         AS active_customers,
    ROUND(
        100.0 * COUNT(*)
        / FIRST_VALUE(COUNT(*)) OVER (PARTITION BY cohort_start ORDER BY months_since_first),
        1
    )                                AS retention_pct
FROM indexed
GROUP BY cohort_start, months_since_first;
