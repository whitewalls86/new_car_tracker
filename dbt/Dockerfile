FROM ghcr.io/dbt-labs/dbt-postgres:1.8.2

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app

# Copy all dbt project files (models, seeds, profiles, packages, etc.)
COPY dbt/ /usr/app/
