"""Print the headline numbers from the analytics views.

    python -m pipeline.report
"""
from __future__ import annotations

from decimal import Decimal

from pipeline.config import get_settings
from pipeline.db import connect


def fmt(value) -> str:
    if isinstance(value, Decimal):
        return f"{value:,.2f}" if value != value.to_integral() else f"{int(value):,}"
    if isinstance(value, int):
        return f"{value:,}"
    return "" if value is None else str(value)


def table(conn, title: str, query: str) -> None:
    cursor = conn.execute(query)
    headers = [d.name for d in cursor.description]
    rows = [[fmt(v) for v in row] for row in cursor.fetchall()]
    widths = [max(len(h), *(len(r[i]) for r in rows)) if rows else len(h) for i, h in enumerate(headers)]
    print(f"\n== {title} ==")
    print("  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)))
    for row in rows:
        print("  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)))


def main() -> None:
    with connect(get_settings()) as conn:
        table(conn, "Totals", """
            SELECT (SELECT SUM(rows_read) FROM analytics.dq_monthly)        AS rows_read,
                   (SELECT COUNT(*) FROM dw.fact_order_items)               AS fact_rows,
                   (SELECT COUNT(*) FROM dq.quarantine)                     AS quarantined_rows,
                   (SELECT COUNT(DISTINCT order_id) FROM dw.fact_order_items) AS orders,
                   (SELECT COUNT(*) FROM dw.dim_customer)                   AS customers,
                   (SELECT COUNT(*) FROM dw.dim_product)                    AS products,
                   (SELECT SUM(net_revenue) FROM analytics.monthly_kpis)    AS net_revenue_pkr""")
        table(conn, "Quarantine reasons", "SELECT * FROM analytics.dq_error_summary ORDER BY rows_affected DESC")
        table(conn, "Warnings on kept rows", "SELECT * FROM analytics.dq_warning_summary ORDER BY rows_affected DESC")
        table(conn, "Monthly KPIs", """
            SELECT order_month, orders, net_revenue, avg_completed_order_value AS aov,
                   cancellation_rate_pct AS cancel_pct, revenue_growth_pct AS growth_pct
            FROM analytics.monthly_kpis ORDER BY order_month""")
        table(conn, "Categories", "SELECT * FROM analytics.category_performance ORDER BY revenue_rank")
        table(conn, "Cash on delivery vs prepaid", "SELECT * FROM analytics.payment_type_outcomes ORDER BY orders DESC")
        table(conn, "Customers", "SELECT * FROM analytics.customer_summary")


if __name__ == "__main__":
    main()
