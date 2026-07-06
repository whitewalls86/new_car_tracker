FROM python:3.13-slim

# Pinned to match dbt_runner/Dockerfile — dbt 1.10 supports data_tests.arguments; avoids dbt 2.x prerelease.
RUN pip install "dbt-core==1.10.20" "dbt-postgres==1.10.2" "dbt-duckdb==1.10.1" --no-cache-dir

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app
ENTRYPOINT ["dbt"]

# Copy all dbt project files (models, seeds, profiles, packages, etc.)
COPY dbt/ /usr/app/

RUN dbt deps
