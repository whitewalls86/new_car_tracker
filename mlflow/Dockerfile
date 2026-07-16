# Plan 112 Gate B: MLflow tracking server image.
#
# Kept isolated (own Dockerfile + requirements.txt), never merged into the
# shared FastAPI-services images -- same convention as lakehouse/Dockerfile.
# The server uses a SQLite backend store (isolated, no production Postgres)
# and serves MinIO-backed artifacts via --serve-artifacts, so it needs boto3
# but not psycopg2 in this first Gate B chunk.
FROM python:3.13-slim-bookworm

WORKDIR /app

COPY mlflow/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# SQLite backend store lives on a mounted volume at /mlflow/store (see
# docker-compose.mlflow.yml). Create the mount point so a fresh volume is
# writable by the server on first start.
RUN mkdir -p /mlflow/store

EXPOSE 5000
