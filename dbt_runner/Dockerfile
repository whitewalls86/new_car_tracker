FROM python:3.13-slim

# Install dbt-postgres and uvicorn
RUN pip install dbt-postgres==1.8.2 dbt-duckdb==1.8.4 uvicorn --no-cache-dir

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

# Install dbt packages (dbt_packages/ is gitignored so not in the COPY above)
RUN dbt deps

# Pre-install DuckDB extensions so they don't download at runtime
RUN python -c "import duckdb; con = duckdb.connect(); con.install_extension('httpfs'); con.install_extension('postgres_scanner')"

EXPOSE 8080
ENTRYPOINT []
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
