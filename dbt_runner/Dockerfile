FROM python:3.13-slim

# dbt 1.10 is the minimum stable release supporting data_tests.arguments syntax.
# Avoid dbt 2.x prerelease: it rejects source meta.external_location used by dbt-duckdb here.
# Keep prod/CI/local aligned on the same exact pins.
RUN pip install "dbt-core==1.10.20" "dbt-postgres==1.10.2" "dbt-duckdb==1.10.1" uvicorn --no-cache-dir

# Install additional dependencies
COPY dbt_runner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app
ENV PYTHONUNBUFFERED=1

# Copy dbt project files and the runner app from committed code
COPY dbt/ /usr/app/
COPY dbt_runner/app.py /usr/app/app.py
COPY shared/ /usr/app/shared/
COPY scripts/audit_adaptive_refresh_features.py /usr/app/scripts/audit_adaptive_refresh_features.py
COPY scripts/report_dbt_run_results.py /usr/app/scripts/report_dbt_run_results.py

# Install dbt packages (dbt_packages/ is gitignored so not in the COPY above)
RUN dbt deps

# Pre-install DuckDB extensions so they don't download at runtime
RUN python -c "import duckdb; con = duckdb.connect(); con.install_extension('httpfs'); con.install_extension('postgres_scanner')"

EXPOSE 8080
ENTRYPOINT []
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
