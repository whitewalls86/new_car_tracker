# Plan 112 lakehouse image targets.
#
# This Dockerfile intentionally owns every Plan 112 runtime target so the
# exploratory lakehouse work does not sprawl across one-off image definitions:
#
#   - lakehouse-worker: PySpark/Iceberg one-shot worker.
#   - mlflow: standalone MLflow provenance/tracking smoke server.
#
# They remain separate build targets because their base-runtime constraints are
# genuinely different: Spark currently runs on Python 3.13 + Java 17, while
# mlflow==2.17.2 is kept on Python 3.12 for reliable ARM64 wheels.
#
# Pinned to -bookworm (Debian 12), not the rolling python:3.13-slim tag:
# Debian trixie (13) dropped the openjdk-17-jre-headless package in favor of
# openjdk-21 only, and Spark 3.5.x's Java 21 support is less proven than 17.
# Revisit this pin if/when pyspark is upgraded to a version with solid Java
# 21 support.
FROM python:3.13-slim-bookworm AS lakehouse-worker

# Iceberg-Spark-runtime + iceberg-aws-bundle jar versions. iceberg-aws-bundle
# (AWS SDK v2, shaded) backs Iceberg's native S3FileIO -- Lakekeeper hands
# Spark `s3://...` table locations, and S3FileIO is what actually serves
# that scheme; Hadoop-AWS's S3AFileSystem only registers `s3a://` and was
# tried first here, which does not exist for this catalog. Keep this version
# in sync with ICEBERG_SPARK_RUNTIME_VERSION if pyspark is ever upgraded --
# see docs/plan_112_refresh_policy_backtesting.md.
ARG ICEBERG_SPARK_RUNTIME_VERSION=1.6.1
ARG ICEBERG_AWS_BUNDLE_VERSION=1.6.1

# Plan 125 Gate A: hadoop-aws + the AWS SDK v1 bundle it needs, serving ONLY
# `s3a://` reads of plain (non-Iceberg) Parquet.
#
# This does not contradict the S3FileIO note above -- the two cover genuinely
# different code paths, and both are required:
#
#   * Iceberg TABLE reads/writes (Lakekeeper's `s3://` locations) go through
#     Iceberg's own S3FileIO, from the shaded iceberg-aws-bundle. Unchanged.
#   * Plain `spark.read.parquet(...)` of the normalized silver/ops_normalized
#     Parquet does NOT involve Iceberg at all -- it resolves the scheme via
#     Hadoop's FileSystem API, which has no handler for `s3`/`s3a` unless
#     hadoop-aws is on the classpath. Verified empirically at Gate A: without
#     these jars, reading ops_normalized Parquet fails with
#     `UnsupportedFileSystemException: No FileSystem for scheme "s3"` and
#     `ClassNotFoundException: ...s3a.S3AFileSystem`. No previous script hit
#     this because A2 used synthetic in-memory rows and A3 read via DuckDB.
#
# The two coexist: hadoop-aws uses AWS SDK v1 (com.amazonaws), while
# iceberg-aws-bundle shades SDK v2 (software.amazon.awssdk) -- different
# packages, no classpath conflict. Verified: a single Spark session reads
# s3a:// Parquet AND resolves the Iceberg REST catalog.
#
# hadoop-aws MUST match the hadoop-client-api/runtime version pyspark 3.5.3
# bundles (3.3.4) -- a mismatch is a NoSuchMethodError at runtime. The SDK
# bundle version is the one hadoop 3.3.4 was built against.
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_JAVA_SDK_BUNDLE_VERSION=1.12.262

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
       "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_AWS_BUNDLE_VERSION}/iceberg-aws-bundle-${ICEBERG_AWS_BUNDLE_VERSION}.jar" \
    && curl -fsSL -o "$SPARK_JARS_DIR/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
       "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
    && curl -fsSL -o "$SPARK_JARS_DIR/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar" \
       "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar"

COPY . .

# Plan 125 Gate A: install the dbt project's packages (dbt/packages.yml) so
# `dbt build --target spark` can run -- dbt refuses to build when packages.yml
# lists packages that are not installed. Same step dbt/Dockerfile performs;
# resolved from dbt Hub over HTTPS, so no git in the image is required.
RUN cd /app/dbt && dbt deps

# One-shot worker (profile-gated in docker-compose.lakehouse.yml) -- no
# default CMD that starts a long-running process; invoked directly via
# `docker compose run --rm lakehouse-worker python -m scripts....`.
CMD ["python", "-c", "print('lakehouse-worker: run a script via docker compose run --rm lakehouse-worker python -m scripts....')"]


# Plan 112 Gate B: standalone MLflow tracking/provenance target.
#
# Keep this on Python 3.12 for now: mlflow==2.17.2 constrains pyarrow<18,
# and pyarrow 17 does not have a reliable Python 3.13 ARM64 wheel on the OCI
# A1 VM. Python 3.13 falls back to a source build there and fails before the
# server image can build.
FROM python:3.12-slim-bookworm AS mlflow

WORKDIR /app

COPY lakehouse/mlflow-requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Package the provenance client into the same image as the tracking server so
# VM smoke tests can run:
#   python -m scripts.log_lakehouse_experiment_provenance ...
# without installing repo dependencies on the host or hot-copying files into a
# running container. .dockerignore keeps local caches, secrets, snapshots, and
# virtualenvs out of this build context.
COPY shared /app/shared
COPY scripts /app/scripts
COPY __init__.py /app/__init__.py

# SQLite backend store lives on a mounted volume at /mlflow/store (see
# docker-compose.mlflow.yml). Create the mount point so a fresh volume is
# writable by the server on first start.
RUN mkdir -p /mlflow/store

EXPOSE 5000
