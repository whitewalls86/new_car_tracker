FROM ghcr.io/dbt-labs/dbt-postgres:1.8.2

COPY dbt_runner/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app
ENV PYTHONUNBUFFERED=1

# Copy dbt project files and the runner app from committed code
COPY dbt/ /usr/app/
COPY dbt_runner/app.py /usr/app/app.py
COPY shared/ /usr/app/shared/

EXPOSE 8080
ENTRYPOINT []
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
