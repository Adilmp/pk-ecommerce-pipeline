# Pipeline image: Python + Java + Spark + the jars Spark needs to talk to S3 and Postgres.
# Everything is pinned, so the pipeline runs the same on any machine (decision D13).
FROM python:3.11-slim-bookworm

# Spark runs on the JVM, so the image needs Java. procps provides `ps`, used by Spark's scripts.
# curl downloads the jars below and is removed again after that.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# The base image ships older pip/setuptools/wheel with known CVEs, so upgrade them first (D28).
RUN pip install --no-cache-dir --upgrade pip==26.2.1 setuptools==84.0.0 wheel==0.48.0

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

# Extra jars, added straight into PySpark's jar folder:
#   hadoop-aws + aws-java-sdk-bundle  -> the s3a:// filesystem (S3 / RustFS)
#   postgresql                        -> JDBC writes into the warehouse
# hadoop-aws must match the Hadoop version bundled inside PySpark, so the build checks it.
# Supply chain (D28): every jar is checked against a pinned SHA-256, so a tampered or swapped
# download fails the build. Changing a version means updating its checksum too.
# curl is only needed here, so it is removed afterwards (less software = fewer CVEs).
ARG HADOOP_VERSION=3.3.4
ARG HADOOP_AWS_SHA256=53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a
ARG AWS_SDK_VERSION=1.12.797
ARG AWS_SDK_SHA256=251115656f2b66ad644b6fc69152db5275ed26d8747fecabe59ac59bc1a4b42b
ARG POSTGRES_JDBC_VERSION=42.7.13
ARG POSTGRES_JDBC_SHA256=6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93
RUN set -eux; \
    JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    test -f "$JARS/hadoop-client-api-${HADOOP_VERSION}.jar" \
      || { echo "PySpark does not bundle Hadoop ${HADOOP_VERSION}; update HADOOP_VERSION"; exit 1; }; \
    MAVEN=https://repo1.maven.org/maven2; \
    fetch() { \
      curl -fsSL -o "$JARS/$(basename "$1")" "$MAVEN/$1"; \
      echo "$2  $JARS/$(basename "$1")" | sha256sum -c -; \
    }; \
    fetch "org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" "$HADOOP_AWS_SHA256"; \
    fetch "com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" "$AWS_SDK_SHA256"; \
    fetch "org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" "$POSTGRES_JDBC_SHA256"; \
    apt-get purge -y --auto-remove curl

# Run as a normal user (not root). Hadoop needs the user to exist in /etc/passwd, and matching
# the host's UID keeps files written to the mounted repo (e.g. charts) owned by you.
ARG UID=1000
ARG GID=1000
RUN groupadd --gid "${GID}" app && useradd --uid "${UID}" --gid "${GID}" --create-home app
USER app

ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    SPARK_CONF_DIR=/app/conf

COPY --chown=app:app . /app

CMD ["python", "-m", "pipeline.run", "--help"]
