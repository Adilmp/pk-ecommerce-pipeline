# How it was built, step by step

Each step says what was built, where the code is, how to run it, and how we know it works.
Read it in order: it's the story you tell when someone asks *"walk me through how you built it."*
After each step, the matching section of [QUIZ.md](QUIZ.md) checks you understood it.

| Step | What | Key files |
|---|---|---|
| 0 | Setup and decisions | [DECISIONS.md](DECISIONS.md) |
| 1 | Profile the raw data | [DATA_PROFILE.md](DATA_PROFILE.md) |
| 2 | Infrastructure | `docker-compose.yml`, `Dockerfile`, `Makefile`, `.env.example` |
| 3 | Ingest monthly batches | `src/pipeline/ingest.py` |
| 4 | Clean + data quality (raw → silver) | `transform.py`, `quality.py`, `silver.py` |
| 5 | Warehouse schema | `sql/schema/*.sql` |
| 6 | Load gold (idempotent) | `gold.py`, `sql/load/*.sql` |
| 7 | Orchestration + backfill | `run.py` |
| 8 | Analytics SQL + charts | `sql/analytics/*.sql`, `charts.py` |
| 9 | Tests + CI | `tests/`, `.github/workflows/ci.yml` |
| 10 | AWS and shipping | [AWS.md](AWS.md), `README.md` |

---

## Step 0: Setup and decisions
Chose the dataset (Pakistan's largest e-commerce dataset) and the architecture before writing
code: batch, three layers, S3 lake, Spark, star schema, idempotent loads. **Done:** DECISIONS.md.

## Step 1: Profile the raw data
Looked at every column before designing anything: null rates, odd values, duplicates, and how
values relate to each other. Found the Excel padding (44% blank lines), the formula columns,
and the big one: `grand_total` is an order-level value repeated on every item.
**Done:** DATA_PROFILE.md, with a rule for every finding.

## Step 2: Infrastructure
- `docker-compose.yml`: **RustFS** (S3 API, port 9000, console on 9001), **Postgres** (5432), and
  the **pipeline** container, a job that runs a command and exits.
- `Dockerfile`: Python 3.11 + Java 17 + PySpark 3.5.9 + the jars for S3 (`hadoop-aws`) and
  Postgres (JDBC). The build checks the Hadoop version matches.
- `.env.example`: all configuration. `Makefile`: one-word commands.

**Run:** `make up` → both services report *healthy*.

## Step 3: Ingest (simulate data arriving)
`ingest.py` streams the 106 MB CSV (never loading it all into memory), checks the header
against the schema contract, skips blank lines, and writes one file per month to
`s3://pk-ecommerce/raw/orders/ingest_month=YYYY-MM/orders.csv`, plus a manifest with the row
count and SHA-256 checksum.
**Run:** `make ingest` → 26 months, 584,524 rows, 464,051 blank lines skipped, ~20 seconds.

## Step 4: Clean + data quality (raw → silver)
For one month, `silver.py`:
1. reads the raw CSV with an explicit all-string schema (`multiLine` on),
2. `standardize`: trims values, removes line breaks, and turns `\N`, `#N/A` and `#REF!` into NULL,
3. `add_typed_columns`: explicit, pattern-checked casts, status and payment groupings, and
   **revenue per item**,
4. `deduplicate`: one row per `item_id`, last copy wins,
5. `apply_quality_rules`: 13 ERROR and 3 WARN rules, each producing a reason code,
6. writes clean rows to silver and failed rows to quarantine (Parquet, one file per month),
7. runs reconciliation checks 1 and 2 (D21) and stops if they don't add up.

**Run:** `make run MONTH=2016-07` → *read 8,837 · clean 8,837 · quarantined 0*.

## Step 5: Warehouse schema
`sql/schema/`: schemas (`staging`, `dw`, `dq`, `analytics`), a generated `dim_date` (with
Pakistan's July–June fiscal year), three dimensions with surrogate keys, the fact table
(grain: order item), the quarantine table, the run log, and unlogged staging tables.
**Run:** `make init` (safe to rerun: everything is `IF NOT EXISTS`).

## Step 6: Load gold (silver → warehouse)
`gold.py`: Spark writes the month to staging over JDBC, then **one transaction** upserts the
dimensions (`INSERT … ON CONFLICT`), deletes the month's facts, and inserts them with
surrogate-key lookups. Check 3 (D21) confirms every staged row landed.
**Done when:** loading a month twice leaves the warehouse unchanged. The end-to-end test asserts this.

## Step 7: Orchestration + backfill
`run.py`: `--month` or `--all` (optionally `--from/--to`), one Spark session for the whole
backfill, every (month, step) logged in `dq.pipeline_runs`, a summary table at the end, and a
non-zero exit code on failure.
**Run:** `make backfill` → 26 months in ~4 minutes: **584,524 read → 574,758 clean + 9,766 quarantined**.

## Step 8: Analytics SQL + charts
`sql/analytics/`: monthly KPIs with `LAG` growth and a running total, category share and rank,
top 5 products per category (`ROW_NUMBER`), COD vs prepaid outcomes, repeat customers, monthly
cohort retention, and data-quality summaries. `charts.py` renders the README charts (light and
dark versions) from those views.
**Run:** `make report`, `make charts`.

## Step 9: Tests + CI
- **Unit tests** (45, about 30 s): every transformation and every rule, on tiny hand-made
  DataFrames. No services needed.
- **End-to-end test**: a 12-row fixture CSV with one example of each real problem, run through
  the real RustFS and Postgres in a *separate* bucket and database. It asserts exact counts,
  the `grand_total` trap, and that rerunning a month changes nothing.
- **CI** (GitHub Actions): lint + unit tests, then the end-to-end test in Docker Compose, on every push.

**Run:** `make test`, `make test-e2e`, `make lint`.

## Step 10: AWS and shipping
Moving to AWS S3 is configuration only (see [AWS.md](AWS.md)). The README gives the
architecture, results and how to run it, and the repo is public on GitHub.
