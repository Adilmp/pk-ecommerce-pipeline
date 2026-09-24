"""Render the README charts from the analytics views, in a light and a dark version.

    python -m pipeline.charts        # -> docs/images/<name>-light.png and <name>-dark.png

Colours are the first four slots of a validated categorical palette (colour-blind safe in
both modes). Every stacked segment carries a visible label, and the README repeats the
numbers as tables, so no chart relies on colour alone.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

from pipeline.config import get_settings  # noqa: E402
from pipeline.db import connect  # noqa: E402

OUT = Path(__file__).resolve().parents[2] / "docs" / "images"

THEMES = {
    "light": {
        "surface": "#fcfcfb", "ink": "#0b0b0b", "secondary": "#52514e", "muted": "#898781",
        "grid": "#e1e0d9", "axis": "#c3c2b7",
        "series": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"],
    },
    "dark": {
        "surface": "#1a1a19", "ink": "#ffffff", "secondary": "#c3c2b7", "muted": "#898781",
        "grid": "#2c2c2a", "axis": "#383835",
        "series": ["#3987e5", "#d95926", "#199e70", "#c98500"],
    },
}
LABEL_ON_FILL = "#0b0b0b"  # dark ink reads on every fill in both palettes (>= 4.5:1)


def style_axes(ax, t, value_axis="y"):
    fig = ax.figure
    fig.patch.set_facecolor(t["surface"])
    ax.set_facecolor(t["surface"])
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(t["axis"])
    ax.tick_params(colors=t["muted"], labelsize=9, length=0, pad=6)
    ax.grid(axis=value_axis, color=t["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def titles(fig, t, title, subtitle):
    fig.text(0.012, 0.965, title, color=t["ink"], fontsize=13, fontweight="bold", va="top")
    fig.text(0.012, 0.905, subtitle, color=t["secondary"], fontsize=9.5, va="top")


def month_ticks(ax, months):
    positions, labels = [], []
    for i, m in enumerate(months):
        year, month = int(m[:4]), int(m[5:])
        if month in (1, 4, 7, 10):
            positions.append(i)
            labels.append(date(year, month, 1).strftime("%b\n%Y") if month in (1, 7) or i == 0
                          else date(year, month, 1).strftime("%b"))
    ax.set_xticks(positions, labels)


def monthly_revenue(conn, t, path):
    rows = conn.execute(
        "SELECT order_month, net_revenue, open_order_value FROM analytics.monthly_kpis ORDER BY 1"
    ).fetchall()
    months = [r[0] for r in rows]
    completed = [float(r[1]) / 1e6 for r in rows]
    open_value = [float(r[2]) / 1e6 for r in rows]
    x = range(len(months))

    fig, ax = plt.subplots(figsize=(10, 4.8), dpi=160)
    fig.subplots_adjust(left=0.06, right=0.985, top=0.8, bottom=0.14)
    style_axes(ax, t)
    ax.bar(x, completed, width=0.72, color=t["series"][0], edgecolor=t["surface"], linewidth=1.2,
           label="Completed items")
    ax.bar(x, open_value, width=0.72, bottom=completed, color=t["series"][1], edgecolor=t["surface"],
           linewidth=1.2, label="Items still in progress at export")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}M"))
    month_ticks(ax, months)
    ax.set_xlim(-0.6, len(months) - 0.4)

    totals = [c + o for c, o in zip(completed, open_value, strict=True)]
    for label_month, text in (("2016-11", "White Friday\n2016"), ("2017-11", "White Friday 2017")):
        i = months.index(label_month)
        ax.annotate(text, (i, totals[i]), xytext=(0, 6), textcoords="offset points", ha="center",
                    va="bottom", fontsize=8.5, color=t["secondary"])
    # A bracket over the last four months, where orders hadn't reached "complete" yet.
    start = months.index("2018-05")
    bracket_y = max(totals[start - 2:]) + 12  # clear the tall March 2018 bar too
    ax.plot([start - 0.36, len(months) - 0.64], [bracket_y, bracket_y], color=t["muted"], linewidth=0.8)
    ax.text(len(months) - 0.64, bracket_y + 5, "May–Aug 2018: most orders were not\nyet marked complete at export",
            ha="right", va="bottom", fontsize=8.5, color=t["secondary"])
    ax.set_ylim(0, max(totals) * 1.12)
    ax.legend(loc="upper left", bbox_to_anchor=(0, 1.02), ncol=2, frameon=False, fontsize=9,
              labelcolor=t["secondary"], handlelength=1.1, handleheight=1.1)
    titles(fig, t, "Monthly revenue (PKR)",
           "Net value of items per order month. Cancelled and refunded items are excluded.")
    fig.savefig(path, facecolor=t["surface"])
    plt.close(fig)


def quarantine_by_month(conn, t, path):
    rows = conn.execute("SELECT order_month, quarantined_rows FROM analytics.dq_monthly ORDER BY 1").fetchall()
    months = [r[0] for r in rows]
    values = [r[1] for r in rows]
    fig, ax = plt.subplots(figsize=(10, 4.2), dpi=160)
    fig.subplots_adjust(left=0.06, right=0.985, top=0.76, bottom=0.16)
    style_axes(ax, t)
    ax.bar(range(len(months)), values, width=0.72, color=t["series"][0], edgecolor=t["surface"], linewidth=1.2)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    month_ticks(ax, months)
    ax.set_xlim(-0.6, len(months) - 0.4)
    ax.set_ylim(0, max(values) * 1.2)
    i = months.index("2018-02")
    ax.annotate(f"{values[i]:,}", (i, values[i]), xytext=(0, 4), textcoords="offset points",
                ha="center", va="bottom", fontsize=8.5, color=t["secondary"])
    j = months.index("2018-01")
    ax.annotate("Jan 2018: discounts larger than the\nitem's own value start to appear",
                (j, values[j]), xytext=(-150, 40), textcoords="offset points", ha="center", va="bottom",
                fontsize=8.5, color=t["secondary"],
                arrowprops={"arrowstyle": "-", "color": t["muted"], "linewidth": 0.8})
    total = sum(values)
    titles(fig, t, "Rows quarantined per month",
           f"{total:,} of 584,524 rows (1.67%) failed a data-quality rule and were set aside with a reason.")
    fig.savefig(path, facecolor=t["surface"])
    plt.close(fig)


def payment_outcomes(conn, t, path):
    rows = conn.execute("""
        SELECT payment_type, orders, completed_pct, in_progress_pct, refunded_pct, cancelled_pct
        FROM analytics.payment_type_outcomes ORDER BY orders
    """).fetchall()
    segments = ["Completed", "In progress", "Refunded", "Cancelled"]
    fig, ax = plt.subplots(figsize=(10, 3.6), dpi=160)
    fig.subplots_adjust(left=0.2, right=0.985, top=0.66, bottom=0.12)
    style_axes(ax, t, value_axis="x")
    ax.grid(False)
    for y, row in enumerate(rows):
        left = 0.0
        for k, name in enumerate(segments):
            share = float(row[2 + k])
            ax.barh(y, share, left=left, height=0.62, color=t["series"][k], edgecolor=t["surface"],
                    linewidth=1.5, label=name if y == 0 else None)
            if share >= 5:
                ax.text(left + share / 2, y, f"{share:.0f}%", ha="center", va="center", fontsize=9,
                        color=LABEL_ON_FILL, fontweight="bold")
            left += share
    ax.set_yticks(range(len(rows)), [f"{r[0]}\n{r[1]:,} orders" for r in rows])
    ax.tick_params(axis="y", colors=t["secondary"], labelsize=9.5)
    ax.set_xlim(0, 100)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.legend(loc="lower left", bbox_to_anchor=(-0.005, 1.0), ncol=4, frameon=False, fontsize=9,
              labelcolor=t["secondary"], handlelength=1.1, handleheight=1.1)
    titles(fig, t, "How orders end, by payment type",
           "Prepaid orders are cancelled far more often, mostly at the card and wallet gateways.")
    fig.savefig(path, facecolor=t["surface"])
    plt.close(fig)


CHARTS = {
    "monthly-revenue": monthly_revenue,
    "quarantine-by-month": quarantine_by_month,
    "payment-outcomes": payment_outcomes,
}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    plt.rcParams["font.family"] = "DejaVu Sans"
    with connect(get_settings()) as conn:
        for name, draw in CHARTS.items():
            for mode, theme in THEMES.items():
                path = OUT / f"{name}-{mode}.png"
                draw(conn, theme, path)
                print(f"wrote {path.relative_to(OUT.parents[1])}")


if __name__ == "__main__":
    main()
