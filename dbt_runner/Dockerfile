FROM ghcr.io/dbt-labs/dbt-postgres:1.8.2

RUN pip install --no-cache-dir fastapi uvicorn

WORKDIR /usr/app
ENV DBT_PROFILES_DIR=/usr/app
ENV PYTHONUNBUFFERED=1

EXPOSE 8080
ENTRYPOINT []
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
