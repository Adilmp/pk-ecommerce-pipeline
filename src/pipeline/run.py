"""Run the pipeline for one month, or backfill a range of months.

    python -m pipeline.run --month 2017-03                 # silver + gold for one month
    python -m pipeline.run --all                           # every month in the lake, oldest first
    python -m pipeline.run --all --from 2017-01 --to 2017-06
    python -m pipeline.run --month 2017-03 --steps silver  # just one step

Each (month, step) is an independent, idempotent task: rerunning it is always safe, and it
maps one-to-one onto a task in an orchestrator such as Airflow (decision D15).
Every run is recorded in dq.pipeline_runs, including failures.
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import traceback
from collections.abc import Callable
from datetime import UTC, datetime
from functools import partial

from pipeline import gold, silver
from pipeline.config import Settings, get_settings
from pipeline.db import record_run
from pipeline.logs import setup_logging
from pipeline.spark import get_spark
from pipeline.storage import list_raw_months, s3_client

log = logging.getLogger("pipeline.run")
STEPS = ("silver", "gold")
MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def month_arg(value: str) -> str:
    if not MONTH.match(value):
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {value!r}")
    return value


def run_step(settings: Settings, month: str, step: str, task: Callable[[], dict]) -> dict:
    started = datetime.now(UTC)
    try:
        metrics = task()
    except Exception as error:
        record_run(settings, month, step, "failed", started, datetime.now(UTC),
                   error="".join(traceback.format_exception_only(type(error), error)).strip())
        raise
    record_run(settings, month, step, "success", started, datetime.now(UTC), metrics)
    return metrics


def print_summary(rows: list[dict]) -> None:
    if not rows:
        return
    columns = [("month", "month"), ("rows_read", "read"), ("silver_rows", "clean"),
               ("quarantined_rows", "quarantined"), ("duplicates_removed", "duplicates"),
               ("fact_rows", "facts loaded")]
    columns = [(key, title) for key, title in columns if any(key in row for row in rows)]
    widths = [max(len(title), 9) for _, title in columns]
    line = "  ".join(title.rjust(w) for (_, title), w in zip(columns, widths, strict=True))
    print("\n" + line + "\n" + "-" * len(line))
    totals = {}
    for row in rows:
        cells = []
        for (key, _), width in zip(columns, widths, strict=True):
            value = row.get(key, "")
            if isinstance(value, int):
                totals[key] = totals.get(key, 0) + value
                value = f"{value:,}"
            cells.append(str(value).rjust(width))
        print("  ".join(cells))
    if len(rows) > 1:
        print("-" * len(line))
        print("  ".join(
            ("TOTAL" if key == "month" else f"{totals.get(key, 0):,}").rjust(w)
            for (key, _), w in zip(columns, widths, strict=True)
        ))
    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--month", type=month_arg, help="one month, YYYY-MM")
    target.add_argument("--all", action="store_true", help="every month found in the raw layer")
    parser.add_argument("--from", dest="start", type=month_arg, help="with --all: first month")
    parser.add_argument("--to", dest="end", type=month_arg, help="with --all: last month")
    parser.add_argument("--steps", default="silver,gold",
                        help="comma-separated, from: silver,gold (default: both)")
    args = parser.parse_args(argv)

    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    unknown = set(steps) - set(STEPS)
    if unknown:
        parser.error(f"unknown step(s): {', '.join(sorted(unknown))}")

    setup_logging()
    settings = get_settings()
    available = list_raw_months(s3_client(settings), settings)
    if args.all:
        months = [m for m in available
                  if (not args.start or m >= args.start) and (not args.end or m <= args.end)]
    else:
        months = [args.month]
    missing = [m for m in months if m not in available]
    if missing or not months:
        log.error("No raw batch for %s. Run `python -m pipeline.ingest` first.",
                  ", ".join(missing) or "the requested range")
        return 1

    log.info("Running %s for %d month(s): %s .. %s", "+".join(steps), len(months), months[0], months[-1])
    spark = get_spark(settings)
    results: list[dict] = []
    try:
        for month in months:
            row = {"month": month}
            if "silver" in steps:
                row.update(run_step(settings, month, "silver",
                                    partial(silver.process_month, spark, settings, month)))
            if "gold" in steps:
                row.update(run_step(settings, month, "gold",
                                    partial(gold.load_month, spark, settings, month)))
            results.append(row)
    except Exception:
        log.exception("Pipeline failed on %s. Fix the cause and rerun: every step is idempotent.", month)
        print_summary(results)
        return 1
    finally:
        spark.stop()

    print_summary(results)
    log.info("Done: %d month(s) processed", len(results))
    return 0


if __name__ == "__main__":
    sys.exit(main())
