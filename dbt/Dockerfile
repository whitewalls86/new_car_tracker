FROM python:3.13-slim

RUN pip install dbt-postgres==1.8.2 --no-cache-dir

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app
ENTRYPOINT ["dbt"]

# Copy all dbt project files (models, seeds, profiles, packages, etc.)
COPY dbt/ /usr/app/

RUN dbt deps
