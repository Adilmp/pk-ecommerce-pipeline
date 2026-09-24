# Running on AWS S3

The pipeline only speaks the S3 API, so moving the data lake from local RustFS to AWS S3 is a
**configuration change**: no code changes (decisions D4 and D14). Postgres stays local; in a
full cloud setup it would be replaced by Amazon RDS or Redshift.

> The pipeline was built and tested end to end against RustFS. The AWS path uses the same code
> with the settings below.

## 1. Create a bucket
In the AWS console → S3 → *Create bucket*. Bucket names are global, so pick something unique,
e.g. `pk-ecommerce-<your-name>`. Keep *Block all public access* on.

## 2. Create an IAM user with least privilege
IAM → Users → *Create user* (no console access) → attach this inline policy, which allows
access to **that one bucket only**:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::pk-ecommerce-<your-name>"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"],
      "Resource": "arn:aws:s3:::pk-ecommerce-<your-name>/*"
    }
  ]
}
```

Then create an **access key** for the user (*Security credentials → Create access key*).

**Running on AWS compute (EC2, ECS)?** Skip the access key: attach the same policy to an **IAM
role** for the instance or task, and leave both key variables empty in step 3. boto3 and Spark
then use AWS's default credential chain, which picks up the role's short-lived credentials, so
there is no long-lived key to leak (decision D29).

## 3. Point the pipeline at S3
In `.env` (never committed):

```bash
S3_ENDPOINT=
S3_BUCKET=pk-ecommerce-<your-name>
AWS_ACCESS_KEY_ID=<access key id>
AWS_SECRET_ACCESS_KEY=<secret access key>
AWS_REGION=ap-south-1
```

An empty `S3_ENDPOINT` tells both boto3 and Spark to use AWS's regional endpoint instead of RustFS,
over HTTPS. With an IAM role, leave `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` empty too.
For a cloud database such as Amazon RDS, add `PGSSLMODE=require` so both database drivers use TLS.

## 4. Run it

```bash
make init ingest backfill report
```

`make up` still starts the local RustFS container (the Compose file always includes it), but
with the settings above the pipeline doesn't use it.

## Costs
The raw CSVs plus Parquet come to well under 1 GB, so storage costs cents per month. Delete the
bucket afterwards if you don't need it.

## What would change in production
- **Compute:** Spark on **EMR** or **AWS Glue** instead of one container.
- **Warehouse:** **Redshift** (or Athena querying the Parquet directly).
- **Credentials:** an **IAM role** attached to the compute instead of access keys (already
  supported: leave the keys empty), and the database password in AWS Secrets Manager.
  See [SECURITY.md](SECURITY.md) for the rest of the checklist.
- **Writes to S3:** Hadoop's S3A *magic committer*, which makes Spark's output commits fast and
  safe on S3 (renames are expensive there).
- **Scheduling:** Airflow / MWAA, one task per step.
