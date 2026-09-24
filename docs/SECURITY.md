# Security

A security review of the project against a standard secure-development checklist, done on
2026-09-24: what applies to a batch data pipeline, what was found, what was fixed, and what is
knowingly accepted. Decisions D27–D29 in [DECISIONS.md](DECISIONS.md) cover the changes.

**Scope.** This is a batch pipeline that runs on one machine in Docker. It has no end users, no
login, no public API and no web frontend. So the checklist's sections on authentication, frontend,
API and AI security don't apply here. What does apply: secrets, network exposure, containers, the
dependency supply chain, injection through data or parameters, and cloud permissions.

---

## Threat model

| What is protected | Threat | Control |
|---|---|---|
| Credentials (S3 keys, database password) | Leaked in git, images or logs | `.env` is git- and docker-ignored; gitleaks scans the whole git history in CI; the Spark UI redacts the keys; logs never print them |
| The lake and the warehouse | Someone on the same network (office or café Wi-Fi) reaching Postgres, RustFS or Airflow with the default passwords | Every port is published on `127.0.0.1` only (D27) |
| The build | A vulnerable or tampered dependency | Exact versions, SHA-256-verified jars, CVE scans in CI (D28) |
| The warehouse | SQL injection through the data or a parameter | Parameterised queries only; the CSV's values never become SQL |
| The AWS account | Leaked or over-powered keys | An IAM role instead of keys, and a policy limited to one bucket (D29, [AWS.md](AWS.md)) |

---

## Findings and fixes

| # | Finding | Severity | Fix |
|---|---|---|---|
| 1 | **Postgres, RustFS (S3 API + console), the Spark UI and Airflow listened on every network interface.** Connecting to the machine's LAN address (not `localhost`), Postgres accepted the default `pipeline`/`pipeline` login, and that user is a superuser, which in Postgres can run shell commands (`COPY … TO PROGRAM`). Airflow's UI has no login. Docker's own firewall rules bypass `ufw`, so a host firewall didn't help. | High (local) | All ports bound to `127.0.0.1`; tested: the same connection is now refused. Every container runs with `no-new-privileges`. (D27) |
| 2 | **Postgres JDBC driver 42.7.4** had 3 HIGH CVEs (e.g. CVE-2025-49146). | Medium | Upgraded to 42.7.13 (0 known CVEs). |
| 3 | **AWS SDK bundle 1.12.262** shipped old Jackson, Ion and Netty with 19 HIGH/CRITICAL CVEs. | Medium | Upgraded to 1.12.797, the latest SDK v1 release: 12 remain, all in shaded Netty (see accepted risks). |
| 4 | **Jars were downloaded without an integrity check.** | Medium | Each jar is verified against a pinned SHA-256 (checked first against Maven Central's published hash); a mismatch fails the build. |
| 5 | **pip 24.0, setuptools 79 and wheel 0.45** from the base image had known CVEs. | Low | Upgraded to pip 26.2.1, setuptools 84.0.0 and wheel 0.48.0. |
| 6 | **curl stayed in the runtime image** after downloading the jars (11 HIGH CVEs in curl, libcurl and libldap). | Low | Removed in the same build step. |
| 7 | **CI had no explicit token permissions and no security checks.** | Low | `permissions: contents: read`; a `security` job runs gitleaks, pip-audit and trivy on every push; ruff's bandit rules (`S`) run in lint. |
| 8 | **AWS access keys were mandatory**, so on AWS the pipeline couldn't use an IAM role. | Low | Keys are now optional: without them, boto3 and s3a use AWS's default credential chain, which ends at the IAM role (D29). |
| 9 | **No way to require TLS to the database** for the JDBC writes. | Low | `PGSSLMODE` (e.g. `require`) now applies to both psycopg and JDBC. |

**Checked and fine:**
- Every query is parameterised (`%s`, `%(month)s`), and identifiers go through psycopg's
  `sql.Identifier`. `--month` must match `YYYY-MM`. Ingest builds file names from parsed dates
  only, so no path comes from the data.
- No `eval`, `pickle`, `shell=True` or unsafe YAML. The only deserialisation is `json.loads` of
  the pipeline's own manifests.
- The Spark UI shows `fs.s3a.access.key` and `fs.s3a.secret.key` as `*********(redacted)` (tested).
- The pipeline and Airflow containers run as a normal user (UID 1000), not root.
- The Airflow DAG builds its shell commands only from Airflow's own `logical_date`, never from
  user-supplied run parameters.

---

## Scan results

| Scan | Tool | Result |
|---|---|---|
| Secrets in the git history | gitleaks 8.30.1 | **0 leaks** in every commit on every branch, including the history from before the rewrite |
| Python dependencies | pip-audit 2.10.1 | **0 known vulnerabilities** in `requirements-dev.txt` and in the image's installed packages |
| Static analysis (SAST) | ruff `S` rules (bandit) + bandit | **0 findings** (asserts in tests excluded) |
| Dockerfile misconfiguration | trivy 0.74.0 `config` | **0 HIGH/CRITICAL.** 1 MEDIUM accepted: the Airflow image is built `FROM` the local pipeline image, which has no registry tag |
| Image vulnerabilities | trivy 0.74.0 `image` | See below |

HIGH/CRITICAL CVEs in the pipeline image, before and after the review:

| Where | Before | After | Why the rest remain |
|---|---:|---:|---|
| Jars this project adds (hadoop-aws, AWS SDK, Postgres JDBC) | 22 | **12** | All in Netty shaded into the AWS SDK v1 bundle. The S3 client that s3a uses runs on Apache HttpClient, not Netty. |
| Jars bundled inside PySpark 3.5.9 | 67 | 67 | Only a Spark upgrade changes them. Some are in parts this job never uses (Mesos, Hive's Derby and Thrift, the Kubernetes client, ZooKeeper); the rest (Hadoop client, Netty, Jackson…) are used, on trusted local files. |
| Debian 12 packages | 85 | **74** | Debian hasn't released fixes yet; a rebuild picks them up when it does. |
| Python packages | 2 (+ old pip/setuptools) | 2 | msgpack and setuptools vendored *inside* the latest pip, used only when installing packages. |

---

## Accepted risks

1. **Local-only defaults.** The simple passwords in `.env.example`, the pipeline connecting as the
   Postgres superuser, and Airflow's no-login UI are acceptable **only because every port is on
   `127.0.0.1`** (D27). None of them should reach a shared or cloud environment (see below).
2. **Spark 3.5's bundled jars and AWS SDK v1.** `hadoop-aws` 3.3.4, the version that matches
   PySpark 3.5, needs AWS SDK v1, which reached end of support on 31 December 2025. The fix is
   Spark 4 (Hadoop 3.4, SDK v2), a major upgrade to test separately. Until then, the job only
   parses trusted batch files, and its only network service (the Spark UI) is on localhost. The
   SDK's once-per-run end-of-support log line is silenced in `conf/log4j2.properties` because it's
   documented here.
3. **Upstream images.** `postgres:16-alpine` is reported for Go standard-library CVEs in `gosu`,
   the tool it uses only at start-up to drop root. `rustfs/rustfs:1.0.0` has no HIGH/CRITICAL
   findings.
4. **Transitive Python packages** (numpy, pillow…) are resolved at build time, not hash-locked.
   Direct dependencies are pinned exactly and the whole tree is audited on every push.

---

## Before running this anywhere shared

- **Credentials:** run on AWS with an IAM role and no keys (supported now, D29); keep the database
  password in AWS Secrets Manager or SSM Parameter Store, not a `.env` file.
- **Database roles:** a *loader* role that writes `dw`, `dq` and `staging`, a read-only *analyst*
  role for the `analytics` views, and no superuser.
- **Encryption in transit:** `PGSSLMODE=verify-full` for the database; the S3 endpoint is already
  HTTPS on AWS.
- **The bucket:** Block Public Access on (the AWS default), default encryption (SSE-S3 or SSE-KMS),
  versioning, lifecycle rules for raw-data retention, and CloudTrail data events as the access log.
- **Airflow:** a real auth manager (FAB or SSO) instead of "everyone is admin", on a private
  network, with alerts when a task fails.
- **Updates:** Dependabot or Renovate for version bumps, a hash-locked lockfile, and the Spark 4
  upgrade.
- **Privacy:** this dataset is public, with pseudonymous customer IDs and no names, emails, phone
  numbers or addresses. Real customer data would first need a review of the applicable
  data-protection law, retention periods and who may access it.

---

## Run the scans

```bash
make security
```

That runs `make scan-secrets` (gitleaks), `make scan-deps` (pip-audit), `make scan-config` (trivy)
and `make scan-image` (trivy, report only). CI runs the first three on every push, and `make lint`
includes the static security rules.
