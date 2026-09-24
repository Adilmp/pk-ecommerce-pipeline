"""One-time setup, safe to run again: create the bucket, the database and every table/view.

    python -m pipeline.init
"""
from __future__ import annotations

import logging

from pipeline.config import get_settings
from pipeline.db import SQL_DIR, connect, ensure_database, run_sql_file
from pipeline.logs import setup_logging
from pipeline.storage import ensure_bucket, s3_client

log = logging.getLogger("pipeline.init")


def main() -> None:
    setup_logging()
    settings = get_settings()

    created = ensure_bucket(s3_client(settings), settings)
    log.info("Bucket %s %s", settings.s3_bucket, "created" if created else "already exists")

    created = ensure_database(settings)
    log.info("Database %s %s", settings.pg_database, "created" if created else "already exists")

    with connect(settings) as conn:
        # Tables: every file is idempotent (IF NOT EXISTS / ON CONFLICT DO NOTHING), so data is kept.
        for path in sorted((SQL_DIR / "schema").glob("*.sql")):
            run_sql_file(conn, path)
            log.info("Applied %s", path.relative_to(SQL_DIR.parent))
        # Views hold no data, so the analytics schema is simply rebuilt. That way a changed
        # view definition (even a new column in the middle) always applies cleanly.
        conn.execute("DROP SCHEMA IF EXISTS analytics CASCADE")
        conn.execute("CREATE SCHEMA analytics")
        for path in sorted((SQL_DIR / "analytics").glob("*.sql")):
            run_sql_file(conn, path)
            log.info("Applied %s", path.relative_to(SQL_DIR.parent))
    log.info("Init complete")


if __name__ == "__main__":
    main()
