# Interview Prep

Answers are in first person. Say them out loud until they sound like you, not like this page.
Every number here comes from the actual run.

---

## The 2-minute walkthrough

> "I built a batch data pipeline for Pakistan's largest public e-commerce dataset: 584,000 order
> items from 2016 to 2018. To make it realistic, the data arrives one month at a time.
>
> Each month lands untouched in an S3 data lake with a manifest of its row count. A PySpark job
> reads it with an explicit schema, cleans and de-duplicates it, and runs 16 data-quality rules.
> Clean rows go to Parquet, partitioned by month. Bad rows go to a quarantine table with a
> reason code, so nothing is silently lost.
>
> Then the clean data is loaded into a star schema in Postgres, one fact table at order-item
> grain with customer, product, date and payment dimensions, and business questions are answered
> with SQL views: revenue growth, category share, cash on delivery vs prepaid, cohort retention.
>
> Every load is idempotent, so a failed run is fixed by running it again, and every stage proves
> its row counts add up. It all runs in Docker Compose with one command, it has 45 unit tests and
> an end-to-end test in CI, Airflow can schedule it month by month, and moving it to AWS S3
> is a config change.
>
> It connects to my background: I spent two years on data quality for AI training data at CNTXT
> and Turing, and this is the pipeline built around that skill."

---

## Your stories (the STAR answers)

**"Tell me about a data problem you found."** (the grain trap)
"While profiling, I noticed `grand_total` was identical on every item of an order. It was the
order total copied onto each row. Across 81,000 multi-item orders, summing it would have counted
revenue two, three, up to 72 times. So I computed revenue at the item grain instead: price times
quantity minus discount. The end-to-end test checks it explicitly: an order with a grand total of
1,500 on two rows must come out as 1,500, not 3,000."

**"Tell me about a bug you caught."** (reconciliation)
"Every stage checks its row counts. For July 2016, Spark read 8,838 rows but ingest's manifest
said 8,837. I traced it to 11 rows where the SKU had a line break inside a quoted value. Python's
CSV reader handled it, but Spark split the row in two. I switched on `multiLine`, reran the month
from the raw layer, and because the loads are idempotent, the warehouse corrected itself. Without
that check, a real sale would have silently disappeared and two broken halves would have sat in
quarantine with nobody knowing why."

**"Tell me about a performance problem."** (Spark planning OOM)
"My unit tests ran out of memory on a one-row DataFrame, so it couldn't be the data. The stack
trace was inside Catalyst, Spark's optimizer, while it was still planning the query. With about 50
derived columns followed by filters, constraint propagation was growing exponentially. I turned it
off and replaced a chain of `withColumn` calls with a single `select`. The tests went from crashing
to 45 tests in 32 seconds."

**"How do you know your numbers are right?"**
"Three reconciliation checks inside the pipeline, plus an independent check: I re-implemented the
rules in plain Python and compared every month. All 26 matched exactly. Total revenue was off by
35 paisa, which turned out to be rounding: the check used banker's rounding and Spark rounds
half-up. Once both used half-up, it matched to the paisa."

**"Tell me about something outside your control."** (MinIO)
"I designed the project on MinIO for local S3 storage. Two weeks before I built it, MinIO deleted
its images from Docker Hub. Because my code only speaks the S3 API and all endpoints are
configuration, I swapped in RustFS with a one-line change in the Compose file. That's exactly why
you don't couple code to a vendor."

**"What did you learn from the data?"**
- "White Friday, November 2017, brought in PKR 296 million of completed revenue, about 31% of the
  whole 26 months."
- "Mobiles and tablets are 48% of completed revenue."
- "Prepaid orders are cancelled 61% of the time against 8% for cash on delivery, concentrated at
  the card and wallet gateways, which fits failed or abandoned online payments. COD orders are
  refunded more (24%), which fits returns after delivery."
- "Data quality falls off a cliff in January 2018: quarantined rows go from almost none to hundreds or
  thousands a month. That looks like a change in the source system, and in a real job I'd raise it with the
  team that owns it."
- "Customers rarely come back quickly: on average only about 11% of a month's new customers order
  again the following month."
- "The last four months look like a revenue collapse, but they aren't: those orders simply hadn't
  been marked complete when the data was exported. So I report open order value alongside."

---

## Questions about the design

**Why Spark for half a million rows?**
"At this size pandas or DuckDB would be faster. I used Spark because the design needs to scale
100× and it's the tool this role uses. In production I'd pick based on volume."

**How do you handle bad data?**
"Quarantine, not delete. 16 rules with two severities: errors are quarantined with every reason
code and the raw values; warnings, like a zero price, are kept and flagged. 1.67% of rows were
quarantined, and the reasons are a SQL view anyone can query."

**What happens if the pipeline fails halfway?**
"I rerun it. The silver step overwrites exactly that month's folder, and the warehouse load
replaces the month inside one transaction, so a rerun never duplicates anything. Every run,
including failures, is logged in a runs table."

**How would you scale this 100×?**
"Spark on a cluster, EMR or Dataproc, instead of one container. The data is already partitioned by
month in object storage, so months can be processed in parallel. The warehouse would move to
Redshift or BigQuery, and I'd raise the shuffle partitions back up."

**How is it scheduled?**
"Two ways, running the same code. A Makefile and CLI for development and CI, and an Airflow DAG:
one run per month, ingest then silver then gold, with catch-up on, so Airflow itself backfills all
26 months. Retries are safe because every task is idempotent. I checked that the Airflow backfill
leaves the warehouse identical to the Makefile run."

**How would you move it to AWS?**
"S3 for the lake: change the endpoint and keys in the environment. An IAM user or role with access
to that one bucket only. EMR or Glue for Spark, Redshift for the warehouse."

**What would you do differently with more time?**
"dbt for the SQL layer with its tests, Airflow alerts on failure, SCD Type 2 for product categories,
and alerting when the quarantine rate jumps, like it did in January 2018."

---

## Fundamentals they may ask

**Spark**
- *Lazy evaluation:* transformations build a plan; actions (`count`, `write`) run it.
- *Narrow vs wide:* narrow (`filter`, `select`) works within a partition; wide (`join`,
  `groupBy`, window) needs a **shuffle**, which moves data between partitions and is expensive.
- *`repartition` vs `coalesce`:* `repartition` shuffles to any number of partitions; `coalesce`
  only reduces them, without a full shuffle (used here to write one file per month).
- *Broadcast join:* send a small table to every executor to avoid shuffling the big one.
- *`cache()`:* keep a DataFrame in memory when several actions reuse it.

**SQL**
- *Window functions:* `LAG`, `ROW_NUMBER`, running `SUM() OVER` (see `sql/analytics/`).
- *Anti-join:* `LEFT JOIN … WHERE right.key IS NULL` finds rows with no match.
- *`WHERE` vs `HAVING`:* `WHERE` filters rows before grouping; `HAVING` filters groups after.
- *De-duplicate:* `ROW_NUMBER() OVER (PARTITION BY key ORDER BY …) = 1`.
- *Index:* speeds up lookups and joins on a column, and slows down writes a little.

**Modelling**: grain, fact vs dimension, surrogate vs natural key, degenerate dimension,
SCD Type 1 vs 2, OLTP vs OLAP, data lake vs warehouse. See QUIZ.md, Step 5.

**Docker**: image vs container, Dockerfile layers, volumes, ports, Compose networking by
service name. See QUIZ.md, Step 2.

**Cloud**: S3 (object storage), IAM and least privilege, Glue / EMR / Redshift / Athena, or
BigQuery / Dataproc on GCP. See AWS.md.
