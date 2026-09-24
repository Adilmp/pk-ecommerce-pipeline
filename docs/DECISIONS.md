# Architecture Decisions

Every non-obvious choice in this project, and why. Each one ends with **Say it**: a one-line
answer you can give in an interview. Learn the *Say it* lines; understand the rest.

| # | Decision | # | Decision |
|---|---|---|---|
| D1 | Batch, not streaming | D14 | Configuration from environment variables |
| D2 | ETL for cleaning, ELT for analytics | D15 | CLI first, Airflow on top |
| D3 | Raw → silver → gold layers | D16 | ERROR quarantines, WARN flags |
| D4 | S3-compatible data lake | D17 | Recompute spreadsheet columns |
| D5 | Parquet after the raw layer | D18 | Revenue per item, not `grand_total` |
| D6 | Partition by month, one file each | D19 | Documented status grouping |
| D7 | PySpark, honestly | D20 | Surrogate keys in dimensions |
| D8 | Explicit schema, explicit casts | D21 | Three reconciliation checks |
| D9 | Quarantine, never drop | D22 | Guard against late-arriving rows |
| D10 | Idempotent loads | D23 | Money as decimals, rounded half-up |
| D11 | Star schema, item grain | D24 | Spark tuned for small batches |
| D12 | Postgres as the warehouse | D25 | CSV read with `multiLine` |
| D13 | Everything in Docker Compose | D26 | Staging tables and rebuildable views |

---

## D1: Batch, not streaming
**Decision:** Process data in scheduled batches, one month at a time.
**Why:** Orders are analysed daily and monthly, not second by second. Batch is simpler to
build, test, rerun and debug.
**Alternative:** Streaming (Kafka + Spark Structured Streaming).
**Trade-off:** Data is only as fresh as the last batch.
**Say it:** *"The business questions are daily and monthly, so batch gives the same value as
streaming at a fraction of the complexity."*

## D2: ETL for cleaning, ELT for analytics
**Decision:** Clean and validate in Spark *before* loading (ETL). Do business analysis in SQL
views *inside* the warehouse (ELT style).
**Why:** The raw data is messy and must be fixed before anyone trusts it. Business questions
change often, and SQL views are the fastest place to change them.
**Alternative:** Pure ELT: load raw into the warehouse and clean with SQL (e.g. dbt).
**Say it:** *"Heavy cleaning happens in Spark before the warehouse; business logic lives in SQL
views, so analysts can change it without touching the pipeline."*

## D3: Three layers: raw → silver → gold (medallion)
- **Raw (bronze):** files exactly as received, plus a manifest (row count, checksum). Never modified.
- **Silver:** cleaned, typed, validated Parquet, plus a quarantine for rejected rows.
- **Gold:** star schema in the warehouse, ready for analysis.

**Why:** If a cleaning rule is wrong, fix it and **replay from raw**; nothing is lost. Each layer
can be inspected when debugging. This project used exactly that: the `multiLine` fix (D25) was
applied by simply re-running from raw.
**Say it:** *"Raw is immutable, so any bug in cleaning can be fixed and replayed without
re-collecting data."*

## D4: S3-compatible object storage as the data lake
**Decision:** Store raw and silver data in an S3 bucket. Locally, **RustFS** provides the S3 API.
**Why:** Object storage is cheap, effectively unlimited, and **separates storage from compute**:
Spark can be scaled or replaced without moving the data.
**Real-world proof:** the project was designed on MinIO. MinIO deleted its images from Docker
Hub on 11 September 2026, two weeks before this was built. Because the code only speaks the S3 API,
swapping to RustFS was a one-line change in `docker-compose.yml`. Moving to AWS is the same:
change the endpoint in `.env` (see [AWS.md](AWS.md)).
**Say it:** *"Storage and compute are decoupled and the code only speaks the S3 API. When MinIO
pulled its images, switching to RustFS was a one-line change."*

## D5: Parquet, not CSV, after the raw layer
**Why:**
- **Columnar:** a query reading 3 of 20 columns only reads those 3.
- **Compressed:** much smaller than CSV.
- **Typed:** the schema travels with the file, so a price is always a number.

**Say it:** *"Parquet is columnar, compressed and carries its schema, so reads are faster and
types can't drift."*

## D6: Partition by month, one file per month
**Decision:** Silver is laid out as `order_month=2017-03/`, with **one** Parquet file per month
(`coalesce(1)`).
**Why:** We load and query by month, so a reader can skip every other month (**partition
pruning**). Partitioning by day would create ~800 folders of tiny files (the **small-files
problem**), and even one month split into Spark's default number of files would be many
small files. The biggest month is ~84k rows, a few MB: one file is right.
**Say it:** *"Partition on how the data is loaded and queried, and size the files so you don't
create the small-files problem."*

## D7: PySpark, and being honest about it
**Decision:** Transform with PySpark 3.5.
**Why:** It is the job's core tool, and the same code scales from half a million rows to billions.
**Honest trade-off:** 0.5M rows fits in pandas or DuckDB on a laptop and would run faster.
**Say it:** *"At this size pandas would be faster; I used Spark because the design needs to scale
100×, and in production I'd pick the tool based on data volume."*

## D8: Explicit schema, explicit casts
**Decision:** Read raw with an explicit schema where **every column is a string**, then cast each
column explicitly, checking the pattern first.
**Why:** `inferSchema` reads the data twice and guesses. Reading as strings means nothing is
silently turned into NULL at read time. The pattern check matters: Spark would quietly cast the
quantity `"1.5"` to `1`. Here it becomes invalid and is caught by a rule (there's a unit test
for exactly this).
**Also:** ingest refuses a file whose header differs from the expected one (a **schema
contract**), so a changed source fails loudly at the door.
**Say it:** *"Raw is read as strings and cast explicitly, so a bad value is caught by a rule
instead of silently becoming NULL, and a changed header fails at the door."*

## D9: Quarantine bad rows, never silently drop them
**Decision:** Rows that fail an ERROR rule go to `dq.quarantine` with **every** reason code and
the raw values exactly as received (e.g. `#N/A` stays visible).
**Why:** Dropping hides problems. Quarantine is auditable, measurable (`analytics.dq_error_summary`)
and reprocessable once a rule is fixed.
**Say it:** *"Bad data is quarantined with a reason, not deleted. You can't fix what you can't see."*

## D10: Idempotent loads: safe to rerun
**Decision:** Running a month twice gives the same result as running it once.
**How:**
- **Silver:** each run overwrites exactly that month's folder (`.../order_month=2017-03/`).
  Spark's "dynamic partition overwrite" was deliberately *not* used: if a month ever produced
  zero clean rows, it would write no partition and leave the old data in place.
- **Warehouse:** Spark writes the month to staging; then **one transaction** upserts the
  dimensions, deletes that month's facts and inserts them from staging.

**Why:** Pipelines fail halfway. If a rerun creates duplicates, every failure becomes a manual
clean-up. This was tested for real: a bad July 2016 load (see D25) was replaced cleanly by
rerunning it, and the end-to-end test reruns a month and asserts nothing changes.
**Say it:** *"Every load is idempotent, so a failed run is fixed by running it again."*

## D11: Star schema, grain = one order item
**Decision:** `dw.fact_order_items` (one row per item in an order) with customer, product, date
and payment-method dimensions. `order_id` stays on the fact as a degenerate dimension.
**Why:** The **grain** defines what one row means; every measure and query depends on it. Star
schemas make analytical SQL simple (one join per dimension) and fast.
**Say it:** *"I declared the grain first, one row per order item, and built the dimensions around it."*

## D12: Postgres as the local warehouse
**Why:** Free, runs in Docker, standard SQL with window functions and transactions.
**In the cloud:** Redshift, BigQuery or Snowflake play this role.
**Say it:** *"Postgres stands in for Redshift or BigQuery locally; the modelling and SQL transfer
directly."*

## D13: Everything in Docker Compose, versions pinned
**Decision:** RustFS, Postgres and the Spark pipeline each run in a container; `make all` runs the
whole thing. The image pins Python 3.11, Java 17, PySpark 3.5.9 and the exact `hadoop-aws` jar
that matches PySpark's bundled Hadoop (3.3.4). The build *checks* that match and fails if it's
wrong. The container runs as a normal user, not root.
**Why:** "Works on my machine" disappears; anyone can clone and run it.
**Say it:** *"The whole stack is reproducible with one command because every dependency is pinned
in a container, and the build verifies the Spark and Hadoop versions match."*

## D14: All configuration from environment variables
**Decision:** Endpoints, bucket, database and passwords come from `.env`, never from code.
**Why:** RustFS → AWS S3 is a config change, not a code change, and secrets never get committed.
The end-to-end test uses this too: it points the same code at a separate bucket and database.
**Say it:** *"Config lives in the environment, so the same code runs locally, in tests and on AWS,
and no secret is ever in git."*

## D15: Simple orchestration first, then Airflow on top
**Decision:** The core is a CLI (`python -m pipeline.run --month 2017-03` / `--all`) driven by a
Makefile. On top of it, an optional **Airflow DAG** runs the same three steps (ingest → silver →
gold) as one DAG run per month. Every run of every step, from either route, is logged in
`dq.pipeline_runs`.
**Why:** A working, idempotent pipeline comes first; a scheduler only wires it together. Because
each (month, step) is already an independent, idempotent task, the DAG is ~30 lines of wiring:
- `schedule="@monthly"` with `catchup=True`: Airflow itself creates one run per month from July
  2016 to August 2018. **Airflow does the backfill.**
- `max_active_runs=1`: months run in order, one at a time (they share the staging tables).
- `retries=2`: safe, because every task is idempotent.
- The Airflow image is built *on top of* the pipeline image, so tasks run exactly the same code.

**Verified:** a full Airflow backfill of all 26 months left the warehouse identical to the
Makefile run (same row counts, same revenue to the paisa).
**Say it:** *"The same idempotent tasks run from a Makefile or from Airflow. In Airflow it's one
DAG run per month with catch-up, so Airflow performs the backfill, and retries are safe."*

---

*D16–D19 came from profiling the data ([DATA_PROFILE.md](DATA_PROFILE.md)).*

## D16: Two severity levels: ERROR quarantines, WARN flags
**Decision:** ERROR rows (e.g. discount larger than the item's value) are quarantined. WARN rows
(price 0, missing category) stay in the data with a flag in `dq_warnings`. Excel's blank padding
lines are neither: they aren't records, so they're removed and counted.
**Why:** Not every oddity is wrong. A zero price may be a free gift, and a missing category still
has a real sale behind it.
**Say it:** *"Rules have severities: errors are quarantined, warnings are kept and flagged, so we
don't throw away real sales."*

## D17: Don't trust spreadsheet-derived columns; recompute them
**Decision:** Ignore `BI Status`, ` MV `, `Year`, `Month`, `FY` and similar columns.
**Why:** They came from spreadsheet formulas: one contains `#REF!`, another stores numbers as
text. Derived data should come from the pipeline, where it's tested. The fiscal year, for
example, is recomputed in `dim_date` (Pakistan's July–June year).
**Say it:** *"I only trust source fields; anything derived is recomputed in the pipeline, where it's tested."*

## D18: `grand_total` is order-level, so revenue is computed per item
**Decision:** Revenue = `price × qty − discount`, per item. `grand_total` is not in the fact table.
**Why:** It's the order total copied onto every item row; summing it counts a three-item order
three times. This is the classic **grain mismatch**.
**Say it:** *"Profiling showed grand_total was an order-level value repeated on every item, so
summing it would double-count; I computed revenue at the item grain instead."*

## D19: Our own status grouping (a documented assumption)

| `status_group` | Raw statuses |
|---|---|
| `completed` | complete |
| `in_progress` | received, cod, paid, pending, pending_paypal, processing, holded, payment_review |
| `refunded` | order_refunded, refund, closed, exchange |
| `cancelled` | canceled, fraud |

**Why:** 17 raw statuses are too many, and the dataset's own grouping is inconsistent. Net
revenue counts only `completed` items.
**Assumption:** `closed` is treated as refunded, as in Magento. With a real client this table
would be confirmed with the business, and the data shows why that matters: from May 2018
almost nothing is `complete` while `received` rises to ~41% (finding F11), so the monthly KPIs
also show `open_order_value`.
**Say it:** *"The status meanings were ambiguous, so I wrote the mapping down as an explicit
assumption and surfaced where it matters, instead of silently reporting a revenue collapse."*

---

*D20–D26 were made while building.*

## D20: Surrogate keys in dimensions
**Decision:** Each dimension has a small integer key (`customer_key`, `product_key`, …) used by
the fact table; the source's natural key (`customer_id`, `sku`) is kept as a unique column.
`dim_date` uses a readable key (`20170315`).
**Why:** Facts join on small integers (faster, smaller), and the warehouse isn't tied to the
source's key format. SKUs are long strings, for example.
**Trade-off:** Loading facts needs a lookup join to find each key. The load check (D21) makes
sure no fact is lost if a lookup fails.
**Say it:** *"Facts carry integer surrogate keys; natural keys live in the dimensions."*

## D21: Three reconciliation checks, and the pipeline stops if one fails
1. **Ingest → Spark:** the rows Spark reads must equal the row count in ingest's manifest.
2. **Silver:** rows read = clean + quarantined + duplicates removed.
3. **Gold:** rows loaded into the warehouse = rows staged.

**Why:** Silent row loss is the worst data bug, because nobody notices. Check 1 caught a real bug:
Spark read 8,838 rows for July 2016 where ingest wrote 8,837 (finding F8, fixed in D25).
The final numbers were also verified against an independent plain-Python implementation of
the same rules, month by month.
**Say it:** *"Every stage proves its row counts add up, and one of those checks caught a real CSV
parsing bug before it reached the warehouse."*

## D22: Guard against late-arriving rows
**Decision:** A row whose order date is not in its batch's month is quarantined
(`DATE_OUTSIDE_BATCH`).
**Why:** Each run overwrites one month's folder. A late row from another month would otherwise
land in the wrong folder, or cause a run to overwrite a month it doesn't own.
**In production:** late data would be routed to its correct month and that month re-processed.
Zero rows here, but the guard makes the overwrite logic safe.
**Say it:** *"A batch may only write its own month, so late-arriving rows are caught instead of
corrupting another month."*

## D23: Money is stored as decimals, rounded half-up
**Decision:** Prices and amounts are `DECIMAL(12,2)`/`DECIMAL(14,2)` in Spark and `numeric` in
Postgres. Never floating point. Values with 3 decimals (11,529 discounts) round half-up.
**Why:** Floats can't represent money exactly (`0.1 + 0.2 != 0.3`). The rounding rule was
confirmed the hard way: an independent check disagreed by PKR 0.35 until it used the same
half-up rounding, after which the total matched exactly.
**Say it:** *"Money is always decimal, never float, with an explicit rounding rule, verified
against an independent calculation to the paisa."*

## D24: Spark tuned for small batches
- **`spark.sql.shuffle.partitions = 8`** (default 200): a month is tiny, and 200 partitions means
  200 near-empty tasks for every join or groupBy.
- **`cache()` the batch:** it's used for several counts and two writes, so it's read and parsed once.
- **One `select()` instead of a loop of `withColumn()`:** each `withColumn` adds a layer to the
  query plan.
- **Constraint propagation off:** with ~50 derived columns followed by filters, Spark's
  optimizer tried to infer constraints from every derived column. That grows exponentially: it
  **ran out of memory while planning a one-row test**. Turning it off fixed it (the test
  suite went from out-of-memory to 45 tests in 32 s), and this workload gains nothing from it.

**Say it:** *"I diagnosed an out-of-memory error that happened during query planning, not
execution: constraint propagation exploding over many derived columns. I turned it off and
flattened the plan."*

## D25: CSV read with `multiLine`, line breaks normalised
**Decision:** Spark reads raw CSV with `multiLine=true`; line breaks inside values become a space.
**Why:** 11 rows have a line break inside a quoted SKU. Without `multiLine`, Spark splits such a
row into two broken records. Found by reconciliation check 1 (D21).
**Say it:** *"A reconciliation check caught Spark splitting quoted values with line breaks; the
fix was multiLine parsing, and a replay from raw corrected the warehouse."*

## D26: Staging tables and rebuildable views
**Decision:** Staging tables are `UNLOGGED` and overwritten on every load. The `analytics` schema
(views only) is dropped and rebuilt on every `init`.
**Why:** Staging is temporary by design, so skipping Postgres's write-ahead log makes it faster.
Views hold no data, so rebuilding them means a changed view always applies cleanly.
**Say it:** *"Staging is disposable and unlogged; views are code, rebuilt on every deploy."*
