"""Step 1 of the pipeline: land the source data in the lake's raw layer, one file per month.

The source is one big CSV. To behave like a real system, where new data arrives in batches,
this job splits it by order month and uploads each month to
    s3://<bucket>/raw/orders/ingest_month=YYYY-MM/orders.csv
together with a manifest (row count + checksum). Raw files are never modified afterwards (D3).

    python -m pipeline.ingest                 # every month
    python -m pipeline.ingest --month 2017-03 # one month (simulates that month's delivery)
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from pipeline.config import MANIFEST_PREFIX, RAW_PREFIX, get_settings, raw_key
from pipeline.logs import setup_logging
from pipeline.schema import RAW_COLUMNS, SOURCE_HEADER
from pipeline.storage import ensure_bucket, put_json, s3_client

log = logging.getLogger("pipeline.ingest")
CREATED_AT = RAW_COLUMNS.index("created_at")
UNROUTABLE = "_unroutable"


class SchemaContractError(Exception):
    """The source file does not have the columns the pipeline was built for."""


def month_of(created_at: str) -> str | None:
    try:
        return datetime.strptime(created_at.strip(), "%m/%d/%Y").strftime("%Y-%m")
    except ValueError:
        return None


def split_by_month(source: Path, out_dir: Path) -> dict:
    """Stream the source CSV into one file per month. Never holds the whole file in memory."""
    stats = {"lines": 0, "blank_lines": 0, "rows": 0, "rows_per_month": {}}
    writers: dict[str, tuple] = {}
    with source.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        if header != SOURCE_HEADER:
            raise SchemaContractError(
                f"Unexpected header in {source}.\nExpected: {SOURCE_HEADER}\nGot:      {header}"
            )
        try:
            for row in reader:
                stats["lines"] += 1
                if all(not value.strip() for value in row):
                    stats["blank_lines"] += 1  # Excel padding (finding F1): not a record
                    continue
                stats["rows"] += 1
                month = month_of(row[CREATED_AT]) if len(row) > CREATED_AT else None
                month = month or UNROUTABLE
                if month not in writers:
                    handle = (out_dir / f"{month}.csv").open("w", newline="", encoding="utf-8")
                    writer = csv.writer(handle, lineterminator="\n")
                    writer.writerow(SOURCE_HEADER)
                    writers[month] = (handle, writer)
                writers[month][1].writerow(row)
                stats["rows_per_month"][month] = stats["rows_per_month"].get(month, 0) + 1
        finally:
            for handle, _ in writers.values():
                handle.close()
    return stats


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", help="only upload this month (YYYY-MM)")
    args = parser.parse_args()

    setup_logging()
    settings = get_settings()
    client = s3_client(settings)
    ensure_bucket(client, settings)
    ingested_at = datetime.now(UTC).isoformat(timespec="seconds")

    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp)
        stats = split_by_month(settings.raw_file, out_dir)
        months = sorted(m for m in stats["rows_per_month"] if m != UNROUTABLE)
        log.info(
            "Read %s lines: %s rows, %s blank lines skipped, %s months",
            f"{stats['lines']:,}", f"{stats['rows']:,}", f"{stats['blank_lines']:,}", len(months),
        )

        if args.month:
            if args.month not in months:
                raise SystemExit(f"No rows for month {args.month}. Available: {months[0]} .. {months[-1]}")
            months = [args.month]

        for month in months:
            path = out_dir / f"{month}.csv"
            client.upload_file(str(path), settings.s3_bucket, raw_key(month))
            put_json(client, settings, f"{MANIFEST_PREFIX}/orders_{month}.json", {
                "month": month,
                "rows": stats["rows_per_month"][month],
                "sha256": sha256(path),
                "source_file": settings.raw_file.name,
                "ingested_at": ingested_at,
            })
            log.info("Uploaded %-7s %7s rows -> s3://%s/%s",
                     month, f"{stats['rows_per_month'][month]:,}", settings.s3_bucket, raw_key(month))

        unroutable = stats["rows_per_month"].get(UNROUTABLE, 0)
        if unroutable:
            # Rows without a valid date can't be assigned to a batch: keep them, and say so.
            client.upload_file(str(out_dir / f"{UNROUTABLE}.csv"), settings.s3_bucket,
                               f"{RAW_PREFIX}/{UNROUTABLE}/orders.csv")
            log.warning("%s rows had no valid created_at and were stored under %s/%s/",
                        unroutable, RAW_PREFIX, UNROUTABLE)

        if not args.month:
            put_json(client, settings, f"{MANIFEST_PREFIX}/_ingest_summary.json", {
                **stats, "ingested_at": ingested_at, "source_file": settings.raw_file.name,
            })
    log.info("Ingest complete")


if __name__ == "__main__":
    main()
