# Plan 112 Gate A2: lakehouse-worker image. The first JVM dependency this
# repo needs outside the existing Airflow/Flyway images -- kept fully
# isolated here (own Dockerfile, own requirements.txt), never merged into
# the shared FastAPI-services images.
#
# Pinned to -bookworm (Debian 12), not the rolling python:3.13-slim tag:
# Debian trixie (13) dropped the openjdk-17-jre-headless package in favor of
# openjdk-21 only, and Spark 3.5.x's Java 21 support is less proven than 17.
# Revisit this pin if/when pyspark is upgraded to a version with solid Java
# 21 support.
FROM python:3.13-slim-bookworm

# Iceberg-Spark-runtime + Hadoop-AWS jar versions, pinned to match pyspark's
# bundled Hadoop version (3.5.x ships Hadoop 3.3.4). Keep these three in sync
# if pyspark is ever upgraded -- see docs/runbook_lakehouse.md.
ARG ICEBERG_SPARK_RUNTIME_VERSION=1.6.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY lakehouse/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN SPARK_JARS_DIR="$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && curl -fsSL -o "$SPARK_JARS_DIR/iceberg-spark-runtime-3.5_2.12-${ICEBERG_SPARK_RUNTIME_VERSION}.jar" \
       "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_SPARK_RUNTIME_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_SPARK_RUNTIME_VERSION}.jar" \
    && curl -fsSL -o "$SPARK_JARS_DIR/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
       "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o "$SPARK_JARS_DIR/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
       "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

COPY . .

# One-shot worker (profile-gated in docker-compose.lakehouse.yml) -- no
# default CMD that starts a long-running process; invoked directly via
# `docker compose run --rm lakehouse-worker python -m scripts....`.
CMD ["python", "-c", "print('lakehouse-worker: run a script via docker compose run --rm lakehouse-worker python -m scripts....')"]
