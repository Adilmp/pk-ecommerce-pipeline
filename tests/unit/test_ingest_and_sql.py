import csv
from pathlib import Path

import pytest

from pipeline.db import SQL_DIR, split_statements
from pipeline.ingest import SchemaContractError, month_of, split_by_month
from pipeline.schema import SOURCE_HEADER

FIXTURE = "tests/fixtures/sample_orders.csv"


def test_month_of():
    assert month_of("7/1/2016") == "2016-07"
    assert month_of(" 12/31/2017 ") == "2017-12"
    assert month_of("2016-07-01") is None
    assert month_of("") is None


def test_split_by_month_routes_rows_and_skips_blank_lines(tmp_path):
    stats = split_by_month(Path(FIXTURE), tmp_path)
    assert stats["blank_lines"] == 3
    assert stats["rows"] == 12
    assert stats["rows_per_month"] == {"2016-07": 9, "2016-08": 3}
    with (tmp_path / "2016-07.csv").open(newline="") as f:
        rows = list(csv.reader(f))
    assert rows[0] == SOURCE_HEADER
    assert len(rows) == 1 + 9


def test_a_changed_source_header_fails_loudly(tmp_path):
    source = tmp_path / "changed.csv"
    source.write_text("item_id,status,created_at\n1,complete,7/1/2016\n")
    with pytest.raises(SchemaContractError):
        split_by_month(source, tmp_path)


def test_split_statements_ignores_comment_only_chunks():
    text = "-- a comment\nSELECT 1;\n\n-- another\nSELECT 2;\n-- trailing comment\n"
    assert [s.splitlines()[-1] for s in split_statements(text)] == ["SELECT 1", "SELECT 2"]


def test_every_sql_file_splits_into_statements():
    for path in SQL_DIR.rglob("*.sql"):
        assert split_statements(path.read_text()), f"{path} produced no statements"
