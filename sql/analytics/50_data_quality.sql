-- Data quality, visible to everyone: how much was quarantined each month, and why.

-- The latest successful silver run for each month.
CREATE OR REPLACE VIEW analytics.dq_monthly AS
SELECT DISTINCT ON (order_month)
    order_month,
    (metrics ->> 'rows_read')::integer          AS rows_read,
    (metrics ->> 'silver_rows')::integer        AS clean_rows,
    (metrics ->> 'quarantined_rows')::integer   AS quarantined_rows,
    (metrics ->> 'duplicates_removed')::integer AS duplicates_removed,
    ROUND(
        100.0 * (metrics ->> 'quarantined_rows')::integer
        / NULLIF((metrics ->> 'rows_read')::integer, 0), 2
    )                                           AS quarantined_pct,
    finished_at
FROM dq.pipeline_runs
WHERE step = 'silver' AND status = 'success'
ORDER BY order_month, finished_at DESC;

-- Why rows were quarantined. A row can fail several rules, so these can add up to more
-- than the number of quarantined rows.
CREATE OR REPLACE VIEW analytics.dq_error_summary AS
SELECT code AS error_code, COUNT(*) AS rows_affected
FROM dq.quarantine
CROSS JOIN LATERAL unnest(string_to_array(error_codes, ',')) AS code
GROUP BY code;

-- Warnings on rows that were kept (e.g. zero prices, unknown categories).
CREATE OR REPLACE VIEW analytics.dq_warning_summary AS
SELECT code AS warning_code, COUNT(*) AS rows_affected
FROM dw.fact_order_items
CROSS JOIN LATERAL unnest(string_to_array(dq_warnings, ',')) AS code
WHERE dq_warnings IS NOT NULL
GROUP BY code;
