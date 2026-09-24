"""Postgres helpers: connections, running .sql files, and the pipeline run log."""
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg import sql

from pipeline.config import Settings

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def connect(settings: Settings, dbname: str | None = None, autocommit: bool = False):
    return psycopg.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        user=settings.pg_user,
        password=settings.pg_password,
        dbname=dbname or settings.pg_database,
        # Always explicit: "prefer" is libpq's own default, and an explicit value stops an empty
        # PGSSLMODE variable in the environment from breaking the connection.
        sslmode=settings.pg_sslmode or "prefer",
        autocommit=autocommit,
    )


def ensure_database(settings: Settings) -> bool:
    """Create the warehouse database if it doesn't exist. Returns True if it was created."""
    with connect(settings, dbname="postgres", autocommit=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s", (settings.pg_database,)
        ).fetchone()
        if exists:
            return False
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(settings.pg_database)))
        return True


def split_statements(text: str) -> list[str]:
    """Split a .sql file into statements (our SQL files never put ';' inside strings)."""
    statements = []
    for chunk in re.split(r";\s*(?:\n|$)", text):
        code = "\n".join(
            line for line in chunk.splitlines() if not line.strip().startswith("--")
        ).strip()
        if code:
            statements.append(chunk.strip())
    return statements


def run_sql_file(conn, path: Path, params: dict | None = None) -> None:
    for statement in split_statements(path.read_text()):
        conn.execute(statement, params)


def record_run(
    settings: Settings,
    month: str,
    step: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    metrics: dict | None = None,
    error: str | None = None,
) -> None:
    """Append one row to dq.pipeline_runs. Uses its own connection, so a failed load
    (whose transaction was rolled back) is still recorded."""
    with connect(settings, autocommit=True) as conn:
        conn.execute(
            """
            INSERT INTO dq.pipeline_runs
                (order_month, step, status, started_at, finished_at, duration_s, metrics, error)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                month,
                step,
                status,
                started_at,
                finished_at,
                round((finished_at - started_at).total_seconds(), 2),
                json.dumps(metrics) if metrics is not None else None,
                error,
            ),
        )
