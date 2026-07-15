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

# Iceberg-Spark-runtime + iceberg-aws-bundle jar versions. iceberg-aws-bundle
# (AWS SDK v2, shaded) backs Iceberg's native S3FileIO -- Lakekeeper hands
# Spark `s3://...` table locations, and S3FileIO is what actually serves
# that scheme; Hadoop-AWS's S3AFileSystem only registers `s3a://` and was
# tried first here, which does not exist for this catalog. Keep this version
# in sync with ICEBERG_SPARK_RUNTIME_VERSION if pyspark is ever upgraded --
# see docs/runbook_lakehouse.md.
ARG ICEBERG_SPARK_RUNTIME_VERSION=1.6.1
ARG ICEBERG_AWS_BUNDLE_VERSION=1.6.1

# Debian's openjdk-17 package installs to an architecture-suffixed path
# (java-17-openjdk-amd64 on x86_64 CI runners, java-17-openjdk-arm64 on the
# OCI A1/Ampere ARM64 VM -- see docs/plan_105_vm_migration.md) -- resolve it
# dynamically via a stable symlink instead of hardcoding one architecture,
# so this image builds correctly on both.
# procps provides `ps`, which Spark's load-spark-env.sh shells out to on
# startup -- without it, every Spark session logs a harmless but noisy
# "ps: command not found" warning (observed during VM A2 verification).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /usr/lib/jvm/default-java

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY lakehouse/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN SPARK_JARS_DIR="$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')" \
    && curl -fsSL -o "$SPARK_JARS_DIR/iceberg-spark-runtime-3.5_2.12-${ICEBERG_SPARK_RUNTIME_VERSION}.jar" \
       "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_SPARK_RUNTIME_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_SPARK_RUNTIME_VERSION}.jar" \
    && curl -fsSL -o "$SPARK_JARS_DIR/iceberg-aws-bundle-${ICEBERG_AWS_BUNDLE_VERSION}.jar" \
       "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_AWS_BUNDLE_VERSION}/iceberg-aws-bundle-${ICEBERG_AWS_BUNDLE_VERSION}.jar"

COPY . .

# One-shot worker (profile-gated in docker-compose.lakehouse.yml) -- no
# default CMD that starts a long-running process; invoked directly via
# `docker compose run --rm lakehouse-worker python -m scripts....`.
CMD ["python", "-c", "print('lakehouse-worker: run a script via docker compose run --rm lakehouse-worker python -m scripts....')"]
