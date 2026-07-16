# Plan 112 Gate B: MLflow tracking server image.
#
# Kept isolated (own Dockerfile + requirements.txt), never merged into the
# shared FastAPI-services images -- same convention as lakehouse/Dockerfile.
# The server uses a SQLite backend store (isolated, no production Postgres)
# and serves MinIO-backed artifacts via --serve-artifacts, so it needs boto3
# but not psycopg2 in this first Gate B chunk.
#
# Keep this on Python 3.12 for now: mlflow==2.17.2 constrains pyarrow<18,
# and pyarrow 17 does not have a reliable Python 3.13 ARM64 wheel on the OCI
# A1 VM. Python 3.13 falls back to a source build there and fails before the
# server image can build.
FROM python:3.12-slim-bookworm

WORKDIR /app

COPY mlflow/requirements.txt /app/requirements.txt
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
