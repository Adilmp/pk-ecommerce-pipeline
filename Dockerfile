# Pipeline image: Python + Java + Spark + the jars Spark needs to talk to S3 and Postgres.
# Everything is pinned, so the pipeline runs the same on any machine (decision D13).
FROM python:3.11-slim-bookworm

# Spark runs on the JVM, so the image needs Java. procps provides `ps`, used by Spark's scripts.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt

# Extra jars, added straight into PySpark's jar folder:
#   hadoop-aws + aws-java-sdk-bundle  -> the s3a:// filesystem (S3 / RustFS)
#   postgresql                        -> JDBC writes into the warehouse
# hadoop-aws must match the Hadoop version bundled inside PySpark, so the build checks it.
ARG HADOOP_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN set -eux; \
    JARS="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    test -f "$JARS/hadoop-client-api-${HADOOP_VERSION}.jar" \
      || { echo "PySpark does not bundle Hadoop ${HADOOP_VERSION}; update HADOOP_VERSION"; exit 1; }; \
    MAVEN=https://repo1.maven.org/maven2; \
    curl -fsSL -o "$JARS/hadoop-aws-${HADOOP_VERSION}.jar" \
      "$MAVEN/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"; \
    curl -fsSL -o "$JARS/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
      "$MAVEN/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    curl -fsSL -o "$JARS/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      "$MAVEN/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

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
