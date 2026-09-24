# Quiz

Answer each question **out loud or on paper first**, then open the answer. If you get one
wrong, reread the decision it points to (D1–D29 in [DECISIONS.md](DECISIONS.md)) and try the
question again tomorrow: spaced repetition is what makes it stick.

Sections follow the build steps in [STEPS.md](STEPS.md). 71 questions in total.

---

## Step 0: Architecture decisions

**1. Why batch instead of streaming for this project?**
<details><summary>Answer</summary>
The business questions are daily and monthly, so batch delivers the same value with far less
complexity: easier to build, test, rerun and debug. (D1)
</details>

**2. What's the difference between ETL and ELT, and which does this project use?**
<details><summary>Answer</summary>
ETL transforms before loading into the warehouse; ELT loads raw data and transforms inside the
warehouse with SQL. This project uses ETL for the heavy cleaning (Spark) and ELT-style SQL views
for business logic. (D2)
</details>

**3. Name the three layers and what each holds.**
<details><summary>Answer</summary>
Raw/bronze: files exactly as received, plus manifests; never modified. Silver: cleaned, typed,
validated Parquet (plus quarantine). Gold: the star schema in Postgres. (D3)
</details>

**4. A cleaning rule turns out to be wrong after a month of runs. What do you do, and which
decisions make that possible?**
<details><summary>Answer</summary>
Fix the rule and replay from raw. Possible because raw is immutable (D3) and loads are
idempotent (D10). This project did exactly that for the `multiLine` bug (D25).
</details>

**5. Give three reasons to use Parquet instead of CSV.**
<details><summary>Answer</summary>
Columnar (reads only the columns needed), compressed (much smaller), typed (schema travels
with the file). (D5)
</details>

**6. Why partition by month and not by day? And why only one file per month?**
<details><summary>Answer</summary>
Loads and queries are by month, so Spark can skip other months (partition pruning). Daily
partitions would create ~800 folders of tiny files: the small-files problem. The biggest month
is only a few MB, so one file per month (`coalesce(1)`) avoids small files too. (D6)
</details>

**7. "Half a million rows? Why not just use pandas?"**
<details><summary>Answer</summary>
"You're right, at this size pandas or DuckDB would be faster. I used Spark because the design
has to scale 100× and the role uses it; in production I'd choose based on data volume." (D7)
</details>

**8. Why is the raw data read with every column as a string?**
<details><summary>Answer</summary>
So nothing is silently turned into NULL when it's read. Each column is then cast explicitly with a
pattern check, and a value that fails becomes a rule violation that you can see. Spark would
otherwise quietly turn `"1.5"` into `1`. (D8)
</details>

**9. What happens to a row with a negative discount, and why not just delete it?**
<details><summary>Answer</summary>
It goes to `dq.quarantine` with the reason `NEGATIVE_DISCOUNT` and its raw values. Deleting hides
the problem; quarantine is auditable, measurable and reprocessable. (D9)
</details>

**10. What does "idempotent" mean, and how is the warehouse load idempotent?**
<details><summary>Answer</summary>
Running a load twice gives the same result as running it once. Spark writes the month to a
staging table; then one transaction deletes that month's facts and inserts them from staging.
(D10)
</details>

**11. What is the fact table's grain, and why decide it first?**
<details><summary>Answer</summary>
One row per order item. The grain defines what a row means; every measure, dimension and query
depends on it. (D11)
</details>

**12. What plays Postgres's role in the cloud?**
<details><summary>Answer</summary>
Amazon Redshift, Google BigQuery or Snowflake. (D12)
</details>

**13. How do you move the lake from RustFS to AWS S3?**
<details><summary>Answer</summary>
Change the endpoint, bucket and keys in `.env`. No code changes, because the code only speaks
the S3 API and all configuration comes from the environment. (D4, D14, AWS.md)
</details>

**14. Why was the CLI built first, and Airflow added on top?**
<details><summary>Answer</summary>
A working, idempotent pipeline beats a half-configured scheduler. Because each (month, step) was
already an independent, idempotent, logged task, the Airflow DAG turned out to be ~30 lines of
wiring around the same CLI. (D15)
</details>

---

## Step 1: Profiling

**15. Why profile the data before designing the pipeline?**
<details><summary>Answer</summary>
The design depends on what the data really contains. Profiling found that `grand_total` is
order-level, that 44% of the file is blank, and which values are impossible. None of that shows
in the column names.
</details>

**16. The file has exactly 1,048,576 rows, header included. Why is that suspicious?**
<details><summary>Answer</summary>
It's Excel's maximum row count. The file was exported from Excel, which padded it with 464,051
blank lines. Only 584,524 rows are real.
</details>

**17. What's wrong with summing `grand_total` to get revenue?**
<details><summary>Answer</summary>
It's the order total repeated on every item of the order, so summing it at item level counts
multi-item orders several times: a grain mismatch. Revenue is price × qty − discount per item. (D18)
</details>

**18. ERROR vs WARN: what's the difference? One example of each.**
<details><summary>Answer</summary>
ERROR rows are quarantined, e.g. a discount larger than the item's value. WARN rows are kept and
flagged, e.g. price = 0, which may be a free gift. (D16)
</details>

**19. Why aren't the blank Excel lines quarantined?**
<details><summary>Answer</summary>
They aren't records, so there's no order behind them. They're removed and counted (464,051) so the
numbers still add up. (D16)
</details>

**20. Why ignore columns like `BI Status` and `MV` when they already exist?**
<details><summary>Answer</summary>
They were spreadsheet formulas: `BI Status` contains `#REF!`, `MV` stores numbers as text.
Derived values are recomputed in the pipeline, where they're tested. (D17)
</details>

**21. The meaning of the `closed` status is unclear. What do you do?**
<details><summary>Answer</summary>
Make a reasonable assumption (refunded, as in Magento), write it down, and flag it to confirm with
the business. Never guess silently. (D19)
</details>

**22. How many rows end up in quarantine, and why mostly?**
<details><summary>Answer</summary>
9,766 rows (1.67%). 9,713 of them because the discount is larger than the item's value.
</details>

---

## Step 2: Infrastructure (Docker)

**23. What's the difference between a Docker image and a container?**
<details><summary>Answer</summary>
An image is the blueprint: a read-only package of the OS, Java, Python, Spark and code. A
container is a running instance of an image. `make build` builds the image; each
`docker compose run` starts a container from it.
</details>

**24. Why does the pipeline image need Java?**
<details><summary>Answer</summary>
Spark runs on the JVM. PySpark is a Python interface that talks to a Java process.
</details>

**25. What are `hadoop-aws` and the JDBC jar for?**
<details><summary>Answer</summary>
`hadoop-aws` gives Spark the `s3a://` filesystem so it can read and write S3. The PostgreSQL
JDBC jar lets Spark write DataFrames into Postgres tables.
</details>

**26. What does a named volume (`lake-data`, `warehouse-data`) do?**
<details><summary>Answer</summary>
It keeps the data when containers are removed. `make down` keeps it; `make clean` (`down -v`)
deletes it.
</details>

**27. The pipeline connects to `postgres:5432` and `objectstore:9000`, not `localhost`. Why?**
<details><summary>Answer</summary>
Inside Docker Compose, services reach each other by service name on a shared network.
`localhost` inside a container means that container itself.
</details>

**28. Why is the pipeline service in a `job` profile?**
<details><summary>Answer</summary>
It isn't a long-running server. It runs one command and exits, so `docker compose up` shouldn't
start it; `docker compose run pipeline …` does.
</details>

**29. MinIO's images disappeared from Docker Hub. What did it cost this project, and why so little?**
<details><summary>Answer</summary>
A one-line change to use RustFS, another S3-compatible server. The code only speaks the S3 API,
and all endpoints come from configuration. (D4)
</details>

---

## Step 3: Ingest

**30. Why split the single CSV into monthly files?**
<details><summary>Answer</summary>
To behave like a real system where data arrives in batches, so incremental, idempotent,
month-by-month processing (and backfills) can be shown.
</details>

**31. What's in a manifest, and what is it used for?**
<details><summary>Answer</summary>
The month's row count and SHA-256 checksum. The silver job checks that Spark read exactly that
many rows (reconciliation check 1). That's how the `multiLine` bug was caught. (D21)
</details>

**32. What happens if the source adds, removes or renames a column?**
<details><summary>Answer</summary>
Ingest compares the header with the expected schema contract and refuses the file with a
clear error. It fails loudly at the door instead of corrupting data downstream. (D8)
</details>

**33. Why does ingest stream the file instead of loading it with pandas?**
<details><summary>Answer</summary>
Streaming uses almost no memory whatever the file size; it writes each row straight into its
month's file.
</details>

---

## Step 4: Spark (raw → silver)

**34. What is lazy evaluation in Spark? Transformation vs action?**
<details><summary>Answer</summary>
Transformations (`select`, `filter`, `withColumn`, `join`) only build a plan. Nothing runs until an
action (`count`, `collect`, `write`) needs a result. Then Spark optimises the whole plan and runs it.
</details>

**35. Why does the silver job `cache()` the batch?**
<details><summary>Answer</summary>
The same batch is used by several actions: counts, then two writes. Without caching, Spark would
re-read and re-transform the CSV for every action.
</details>

**36. What's a shuffle, and which step in this pipeline causes one?**
<details><summary>Answer</summary>
Moving data between partitions so rows with the same key end up together. It's the most
expensive operation in Spark. De-duplication (a window partitioned by `item_id`) and the
`groupBy` for error counts cause shuffles.
</details>

**37. Why set `spark.sql.shuffle.partitions` to 8 instead of the default 200?**
<details><summary>Answer</summary>
A month is at most ~84k rows. 200 partitions would mean 200 tiny tasks for each shuffle, mostly
overhead. (D24)
</details>

**38. How are duplicates removed, and which copy is kept?**
<details><summary>Answer</summary>
`ROW_NUMBER()` over a window partitioned by `item_id`, ordered by position in the file,
descending; rows with number 1 are kept. The last copy wins. Rows without an `item_id` are never
treated as duplicates, so the `MISSING_ITEM_ID` rule can quarantine them.
</details>

**39. A unit test ran out of memory on ONE row. What was it, and how was it fixed?**
<details><summary>Answer</summary>
Spark's constraint propagation grew exponentially while *planning* a query with ~50 derived
columns followed by filters. Fixed by turning `spark.sql.constraintPropagation.enabled` off and
building columns in one `select()` instead of a chain of `withColumn()` calls. (D24)
</details>

**40. What was the `multiLine` bug, and how was it found?**
<details><summary>Answer</summary>
11 rows have a line break inside a quoted SKU. Spark's default CSV reader split one of them into two
broken rows, so it read 8,838 rows where ingest's manifest said 8,837. Reconciliation check 1
caught it; `multiLine=true` fixed it; a replay from raw corrected the warehouse. (D21, D25)
</details>

**41. Why store money as `DECIMAL` and not `float`?**
<details><summary>Answer</summary>
Floats can't represent most decimal values exactly (0.1 + 0.2 ≠ 0.3). Money needs exact
arithmetic and an explicit rounding rule: here, 2 decimals, half-up. (D23)
</details>

**42. What does the `DATE_OUTSIDE_BATCH` rule protect against?**
<details><summary>Answer</summary>
Late-arriving rows. Each run overwrites one month's folder, so a row from another month must
never land in (or overwrite) the wrong month. (D22)
</details>

---

## Step 5: Warehouse schema

**43. Fact vs dimension table?**
<details><summary>Answer</summary>
Facts hold measurable events (one row per order item, with quantity, prices and amounts).
Dimensions describe them: who (customer), what (product), when (date), how (payment method).
</details>

**44. Surrogate key vs natural key? Which does this warehouse use?**
<details><summary>Answer</summary>
A natural key comes from the source (`customer_id`, `sku`); a surrogate key is an integer the
warehouse generates (`customer_key`). Facts use surrogate keys; natural keys are unique columns
in the dimensions. `dim_date` uses a readable key like `20170315`. (D20)
</details>

**45. What's a degenerate dimension? Give the example here.**
<details><summary>Answer</summary>
A dimension key that sits on the fact table with no table of its own: `order_id`. There's nothing
more to say about an order number than the number itself.
</details>

**46. What's special about the date dimension here?**
<details><summary>Answer</summary>
It's generated, not loaded: one row per day for 2016–2019, with Pakistan's July–June fiscal year
(July 2016 is FY17).
</details>

**47. Why are the staging tables `UNLOGGED`?**
<details><summary>Answer</summary>
They're overwritten every load, so they don't need crash safety. Skipping Postgres's write-ahead
log makes writes faster. (D26)
</details>

**48. What is SCD Type 1, and where is it used?**
<details><summary>Answer</summary>
Slowly Changing Dimension Type 1: overwrite the attribute and keep no history. `dim_product`
keeps the latest known category of each SKU. Type 2 would add a new row per change, with
valid-from and valid-to dates.
</details>

---

## Step 6: Loading gold

**49. Why write to staging first instead of straight into the fact table?**
<details><summary>Answer</summary>
So the replace (delete the month's facts + insert new ones) and the dimension upserts can run in
ONE transaction. Readers never see a half-loaded month, and a failure rolls everything back.
</details>

**50. What does `INSERT … ON CONFLICT (customer_id) DO UPDATE` do?**
<details><summary>Answer</summary>
An upsert: insert the customer if new, otherwise update the existing row. Here it keeps the
earliest first-order date and the latest last-order date, so reloading a month changes nothing.
</details>

**51. The fact insert joins staging to the dimensions. What could silently go wrong, and what
prevents it?**
<details><summary>Answer</summary>
An inner join drops any row with no matching dimension, so facts could silently disappear.
Reconciliation check 3 compares facts loaded with rows staged and fails the load if they differ.
(D21)
</details>

---

## Step 7: Orchestration

**52. What's a backfill?**
<details><summary>Answer</summary>
Running the pipeline for past periods, here every month from July 2016 to August 2018, oldest
first (`make backfill`).
</details>

**53. What's recorded in `dq.pipeline_runs`, and why record failures too?**
<details><summary>Answer</summary>
One row per step per month: status, timings, metrics (row counts, error counts) and the error
message if it failed. Failures are the runs you most need to see later.
</details>

**Airflow**

**61. What does `catchup=True` do in the DAG?**
<details><summary>Answer</summary>
When the DAG starts, Airflow creates a run for every data interval between `start_date` and now
(or `end_date`) that hasn't run yet: here one run per month from July 2016 to August 2018. That's
how Airflow performs the backfill.
</details>

**62. What is a DAG run's data interval, and how does each task know which month to process?**
<details><summary>Answer</summary>
The data interval is the period the run is responsible for: the March 2017 run covers
[2017-03-01, 2017-04-01) and starts on 1 April, once March is complete. Tasks use the template
`{{ data_interval_start.strftime('%Y-%m') }}`, which Airflow fills in per run. (D15)
</details>

**63. Why `max_active_runs=1`?**
<details><summary>Answer</summary>
Months must run one at a time: they share the staging tables (loads are also serialised by a
Postgres advisory lock), and running in order keeps the dimensions' "latest known" values correct.
</details>

**64. Why are `retries=2` safe here?**
<details><summary>Answer</summary>
Every task is idempotent. Retrying a half-finished task replaces the month rather than
duplicating it.
</details>

**65. Why does the DAG call the same CLI (`python -m pipeline.run`) instead of containing the logic?**
<details><summary>Answer</summary>
The logic stays in one tested place; the DAG only schedules it. The pipeline also works without
Airflow (Makefile, CI), and the Airflow image is built on the pipeline image, so it's the same code.
</details>

---

## Step 8: Analytics SQL

**54. How is month-over-month revenue growth computed?**
<details><summary>Answer</summary>
`LAG(net_revenue) OVER (ORDER BY order_month)` gets the previous month's value; growth =
(this − previous) / previous × 100.
</details>

**55. ROW_NUMBER vs RANK vs DENSE_RANK?**
<details><summary>Answer</summary>
Four rows, with rows 2 and 3 tied: ROW_NUMBER → 1, 2, 3, 4 (ties broken arbitrarily);
RANK → 1, 2, 2, 4 (gaps after ties); DENSE_RANK → 1, 2, 2, 3 (no gaps). Top-N-per-category uses ROW_NUMBER so exactly N rows
come back.
</details>

**56. Why do order counts use `COUNT(DISTINCT order_id)`?**
<details><summary>Answer</summary>
The fact table's grain is the item. `COUNT(*)` would count items, and an order with 3 items would
be counted 3 times.
</details>

**57. What is cohort retention?**
<details><summary>Answer</summary>
Group customers by the month of their first order (their cohort), then measure what share of each
cohort is active 1, 2, 3… months later. The view uses `MIN() OVER (PARTITION BY customer)` to find
each customer's cohort.
</details>

**58. Net revenue for June–August 2018 is almost zero. Did sales collapse?**
<details><summary>Answer</summary>
No. From May 2018 almost no orders were marked `complete` yet (~41% `received`). The data was
exported before they finished. That's why the KPIs and chart also show `open_order_value`.
(F11, D19)
</details>

---

## Step 9: Tests + CI

**59. Unit tests vs the end-to-end test: what does each prove?**
<details><summary>Answer</summary>
Unit tests (50) prove each transformation and rule works, using tiny hand-made DataFrames and no
services. The end-to-end test proves the whole system works together (ingest → RustFS → Spark →
Postgres) with exact expected numbers, and that rerunning a month changes nothing.
</details>

**60. Why does the end-to-end test use its own bucket and database?**
<details><summary>Answer</summary>
So it can never overwrite the real data. It's the same code, pointed elsewhere by environment
variables (D14), and it deletes both when it finishes.
</details>

---

## Step 11: Security

**66. On which network interfaces does Docker publish a port by default, and why did it matter here?**
<details><summary>Answer</summary>
On **all** of them (`0.0.0.0`), and Docker's firewall rules bypass `ufw`. So Postgres, with its
default password and a superuser account, was reachable by anyone on the same Wi-Fi. Postgres
superusers can even run shell commands. The fix: publish every port on `127.0.0.1` only. (D27)
</details>

**67. How do you make sure no secret ever reaches git? Why does the CI secret scan need the full history?**
<details><summary>Answer</summary>
Secrets live only in `.env`, which is git-ignored and docker-ignored; the repo has only
`.env.example` with local placeholder values. gitleaks scans in CI with `fetch-depth: 0`, because a
secret that was committed and later deleted is still in the history, and still leaked. (D14, D29)
</details>

**68. How does the pipeline authenticate to S3 on AWS without any access keys?**
<details><summary>Answer</summary>
Leave the keys empty. boto3 and s3a then use AWS's default credential chain, which ends at the IAM
role attached to the machine or container. Its credentials are short-lived and rotated
automatically, and the role's policy only allows the one bucket. (D29)
</details>

**69. The version of each jar is already pinned. Why also check a SHA-256?**
<details><summary>Answer</summary>
The version says what you *asked for*; the checksum proves what you *got*. If the download is
tampered with (a compromised mirror, a man-in-the-middle, a re-published artifact), the hash
doesn't match and the build fails. (D28)
</details>

**70. The image scan still reports HIGH CVEs. How do you justify shipping it?**
<details><summary>Answer</summary>
Everything fixable from this project was fixed: JDBC driver, AWS SDK, pip tooling, curl removed.
What's left is inside PySpark 3.5's bundled jars, the end-of-life AWS SDK v1 that `hadoop-aws`
3.3.4 requires, or Debian packages with no fix released yet. Clearing those needs Spark 4, a major
upgrade. Meanwhile the job only parses trusted files and exposes nothing beyond localhost. It's
documented as an accepted risk with the upgrade path. "A CVE is present" is not the same as "it's
exploitable here". (D28)
</details>

**71. How is SQL injection prevented?**
<details><summary>Answer</summary>
Values are always passed as parameters (`%s`, `%(month)s`), never pasted into SQL strings;
identifiers such as database names go through psycopg's `sql.Identifier`; `--month` must match
`YYYY-MM`; and CSV values only ever become typed column values, never code. ruff's bandit rules flag
SQL built from strings on every lint.
</details>
